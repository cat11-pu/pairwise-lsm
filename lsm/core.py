"""In-memory LSM-tree storage engine built on the Python standard library.

The engine models the usual three parts of a log structured merge store:

* the write path, where every mutation goes into a memtable and into an
  append-only write-ahead log,
* the flush path, where a full memtable is turned into an immutable sorted
  segment,
* the read path, which looks at the memtable first and then walks the
  segments from the newest one to the oldest one.

Everything lives in process memory only; nothing is written to disk.
"""

DEFAULT_MEMTABLE_THRESHOLD = 4

OP_PUT = "put"
OP_DELETE = "delete"


class _Tombstone:
    """Marker stored in place of a value for a key that has been deleted."""

    __slots__ = ()

    def __repr__(self):
        return "<TOMBSTONE>"


TOMBSTONE = _Tombstone()


def _check_key(key):
    """Return key after checking that it can be used as a key."""
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a non-empty string")
    return key


def _check_value(value):
    """Return value after checking that it can be used as a value."""
    if not isinstance(value, str) or not value:
        raise ValueError("value must be a non-empty string")
    return value


class MemTable:
    """Mutable write buffer that maps keys to (value, seq) pairs."""

    def __init__(self, threshold=DEFAULT_MEMTABLE_THRESHOLD):
        self.threshold = threshold
        self._entries = {}
        self._size = 0

    def put(self, key, value, seq):
        """Insert or overwrite key."""
        if key not in self._entries:
            self._size += 1
        self._entries[key] = (value, seq)

    def delete(self, key, seq):
        """Record a deletion marker for key."""
        self.put(key, TOMBSTONE, seq)

    def get(self, key):
        """Return the entry stored for key as a (value, seq) tuple, or None."""
        return self._entries.get(key)

    def size(self):
        """Return the number of distinct keys currently held."""
        return self._size

    def is_full(self):
        """Return True when the memtable can no longer take new writes."""
        return self._size >= self.threshold

    def entries(self):
        """Return every entry as a list of (key, value, seq), sorted by key."""
        items = sorted(self._entries.items())
        return [(key, value, seq) for key, (value, seq) in items]

    def clear(self):
        """Drop every entry."""
        self._entries = {}
        self._size = 0


class SSTable:
    """Immutable sorted segment of (key, value, seq) entries."""

    def __init__(self, entries, seq=0, level=0):
        self.entries = sorted(entries, key=lambda item: item[0])
        self.keys = [item[0] for item in self.entries]
        self.seq = seq
        self.level = level

    def __len__(self):
        return len(self.entries)

    @property
    def min_key(self):
        """Smallest key held by this segment, or None when it is empty."""
        return self.keys[0] if self.keys else None

    @property
    def max_key(self):
        """Largest key held by this segment, or None when it is empty."""
        return self.keys[-1] if self.keys else None

    def get(self, key):
        """Return the (key, value, seq) entry for key, or None."""
        lo, hi = 0, len(self.keys)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.keys[mid] < key:
                lo = mid + 1
            else:
                hi = mid
        if lo < len(self.keys) and self.keys[lo] == key:
            return self.entries[lo]
        return None

    def may_contain(self, key):
        """Return True when key falls inside the range this segment covers."""
        if not self.keys:
            return False
        return self.min_key <= key <= self.max_key

    def overlaps(self, other):
        """Return True when the key ranges of both segments intersect."""
        if not self.keys or not other.keys:
            return False
        return self.max_key >= other.min_key and other.max_key >= self.min_key


class WAL:
    """Append-only log holding every mutation that reached the engine."""

    def __init__(self):
        self.records = []

    def __len__(self):
        return len(self.records)

    def append(self, op, key, value=None):
        """Record one mutation."""
        self.records.append((op, key, value))

    def replay_into(self, engine):
        """Apply the recorded mutations to engine in order."""
        for op, key, value in self.records:
            if op == OP_PUT:
                engine._write(key, value, log=False)
            elif op == OP_DELETE:
                engine._write(key, TOMBSTONE, log=False)
            else:
                raise ValueError("unknown wal record: %r" % (op,))


class LSMEngine:
    """In-memory LSM-tree engine over string keys and string values."""

    def __init__(self, memtable_threshold=DEFAULT_MEMTABLE_THRESHOLD):
        if memtable_threshold < 1:
            raise ValueError("memtable_threshold must be a positive integer")
        self.memtable_threshold = memtable_threshold
        self.mem = MemTable(memtable_threshold)
        self.wal = WAL()
        self.sstables = []
        self._seq = 0

    # ------------------------------------------------------------------
    # write path
    # ------------------------------------------------------------------
    def put(self, key, value):
        """Store value under key and return the sequence number used."""
        return self._write(key, value, log=True)

    def delete(self, key):
        """Mark key as deleted and return the sequence number used."""
        return self._write(key, TOMBSTONE, log=True)

    def _write(self, key, value, log=True):
        _check_key(key)
        if value is TOMBSTONE:
            op = OP_DELETE
            logged = None
        else:
            _check_value(value)
            op = OP_PUT
            logged = value
        self._seq += 1
        if value is TOMBSTONE:
            self.mem.delete(key, self._seq)
        else:
            self.mem.put(key, value, self._seq)
        if log:
            self.wal.append(op, key, logged)
        self._maybe_flush()
        return self._seq

    def _maybe_flush(self):
        if self.mem.is_full():
            self.flush()

    def flush(self):
        """Turn the current memtable into a new segment."""
        if self.mem.size() == 0:
            return False
        segment = SSTable(self.mem.entries(), seq=self._seq, level=0)
        self.sstables.insert(0, segment)
        self.mem.clear()
        return True

    # ------------------------------------------------------------------
    # read path
    # ------------------------------------------------------------------
    def get(self, key):
        """Return the visible value stored for key, or None when absent."""
        _check_key(key)
        entry = self.mem.get(key)
        if entry is not None:
            value, _seq = entry
            return None if value is TOMBSTONE else value
        return self._read_from_segments(key)

    def _read_from_segments(self, key):
        for segment in self.sstables:
            if not segment.may_contain(key):
                continue
            entry = segment.get(key)
            if entry is None:
                continue
            _found_key, value, _seq = entry
            if value is TOMBSTONE:
                return None
            return value
        return None

    def scan(self, start, end):
        """Return the visible (key, value) pairs with start <= key <= end."""
        _check_key(start)
        _check_key(end)
        newest = {}
        for key, value, seq in self.mem.entries():
            newest[key] = (value, seq)
        for segment in reversed(self.sstables):
            for key, value, seq in segment.entries:
                current = newest.get(key)
                if current is None or current[1] < seq:
                    newest[key] = (value, seq)
        result = []
        for key in sorted(newest):
            if key < start:
                continue
            if key > end:
                continue
            value, _seq = newest[key]
            if value is TOMBSTONE:
                continue
            result.append((key, value))
        return result

    # ------------------------------------------------------------------
    # maintenance
    # ------------------------------------------------------------------
    def _select_merge_group(self):
        """Return the newest run of neighbouring segments that can be merged."""
        group = []
        for segment in self.sstables:
            if not group:
                group.append(segment)
                continue
            if group[-1].overlaps(segment):
                group.append(segment)
            else:
                break
        return group

    def _merge_entries(self, segments):
        """Combine the entries of segments, keeping the newest version per key."""
        merged = {}
        for segment in sorted(segments, key=lambda item: item.seq):
            for key, value, seq in segment.entries:
                current = merged.get(key)
                if current is not None and current[1] > seq:
                    continue
                merged[key] = (value, seq)
        combined = [(key, value, seq) for key, (value, seq) in merged.items()]
        combined.sort(key=lambda item: item[0])
        return combined

    def compact(self):
        """Merge the newest run of neighbouring segments."""
        group = self._select_merge_group()
        if len(group) < 2:
            return False
        combined = self._merge_entries(group)
        segment = SSTable(combined, seq=self._seq, level=0)
        index = self.sstables.index(group[0])
        for victim in group:
            self.sstables.remove(victim)
        self.sstables.insert(index, segment)
        return True

    def segment_count(self):
        """Return how many segments the engine currently holds."""
        return len(self.sstables)

    def stats(self):
        """Return a small snapshot of the engine counters."""
        return {
            "memtable_entries": self.mem.size(),
            "segments": len(self.sstables),
            "wal_records": len(self.wal),
            "applied_writes": self._seq,
        }

    @classmethod
    def recover(cls, wal, memtable_threshold=DEFAULT_MEMTABLE_THRESHOLD):
        """Rebuild an engine by replaying the records held by wal."""
        engine = cls(memtable_threshold=memtable_threshold)
        wal.replay_into(engine)
        engine.wal = wal
        return engine

    def __repr__(self):
        return "LSMEngine(memtable_entries=%d, segments=%d)" % (
            self.mem.size(),
            len(self.sstables),
        )
