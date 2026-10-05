"""Behavioural tests for the in-memory LSM storage engine.

The tests only describe the values a caller is allowed to observe; they do
not depend on the internal layout of the engine beyond the counters that the
public API exposes.
"""

import unittest

from lsm.core import LSMEngine


class ReadWriteTests(unittest.TestCase):
    """Plain reads and writes that never leave the memtable."""

    def test_last_write_wins_for_every_key(self):
        engine = LSMEngine(memtable_threshold=4)
        engine.put("alpha", "one")
        engine.put("beta", "two")
        engine.put("alpha", "three")
        self.assertEqual(engine.get("alpha"), "three")
        self.assertEqual(engine.get("beta"), "two")
        self.assertIsNone(engine.get("gamma"))

    def test_invalid_arguments_are_rejected(self):
        engine = LSMEngine(memtable_threshold=2)
        with self.assertRaises(ValueError):
            engine.get("")
        with self.assertRaises(ValueError):
            engine.put("key", "")
        with self.assertRaises(ValueError):
            engine.put(7, "value")
        with self.assertRaises(ValueError):
            engine.delete(None)
        with self.assertRaises(ValueError):
            engine.scan("a", None)
        engine.put("key", "value")
        self.assertEqual(engine.get("key"), "value")


class FlushTests(unittest.TestCase):
    """Writes that spill out of the memtable into a segment."""

    def test_memtable_flushes_once_the_threshold_is_reached(self):
        engine = LSMEngine(memtable_threshold=4)
        for key in ("a", "b", "c", "d"):
            engine.put(key, "v-" + key)
        self.assertEqual(engine.stats()["segments"], 1)
        self.assertEqual(engine.stats()["memtable_entries"], 0)
        engine.put("e", "v-e")
        engine.put("f", "v-f")
        self.assertEqual(engine.stats()["segments"], 1)
        self.assertEqual(engine.stats()["memtable_entries"], 2)

    def test_key_equal_to_the_last_key_of_a_segment_is_readable(self):
        engine = LSMEngine(memtable_threshold=4)
        for key in ("k1", "k2", "k3", "k4"):
            engine.put(key, "v-" + key)
        self.assertEqual(engine.segment_count(), 1)
        self.assertEqual(engine.get("k1"), "v-k1")
        self.assertEqual(engine.get("k3"), "v-k3")
        self.assertEqual(engine.get("k4"), "v-k4")

    def test_deleted_key_stays_absent_after_flush(self):
        engine = LSMEngine(memtable_threshold=2)
        engine.put("a", "v-a")
        engine.put("b", "v-b")
        self.assertEqual(engine.segment_count(), 1)
        engine.delete("a")
        engine.put("c", "v-c")
        self.assertEqual(engine.segment_count(), 2)
        self.assertIsNone(engine.get("a"))
        self.assertEqual(engine.get("b"), "v-b")
        self.assertEqual(engine.get("c"), "v-c")

    def test_range_scan_includes_both_bounds(self):
        engine = LSMEngine(memtable_threshold=4)
        for key in ("a", "b", "c", "d"):
            engine.put(key, "v-" + key)
        engine.put("e", "v-e")
        self.assertEqual(
            engine.scan("b", "d"),
            [("b", "v-b"), ("c", "v-c"), ("d", "v-d")],
        )
        self.assertEqual(engine.scan("a", "a"), [("a", "v-a")])
        self.assertEqual(engine.scan("x", "z"), [])


class CompactionTests(unittest.TestCase):
    """Merging neighbouring segments must keep the visible state intact."""

    def test_compaction_keeps_the_newest_version_readable(self):
        engine = LSMEngine(memtable_threshold=2)
        engine.put("d", "old-d")
        engine.put("j", "v-j")
        engine.put("k", "v-k")
        engine.put("u", "v-u")
        engine.put("d", "new-d")
        engine.put("k", "v-k2")
        self.assertEqual(engine.segment_count(), 3)
        self.assertTrue(engine.compact())
        self.assertEqual(engine.segment_count(), 2)
        self.assertEqual(engine.get("d"), "new-d")
        self.assertEqual(engine.get("k"), "v-k2")

    def test_compaction_preserves_deletions(self):
        engine = LSMEngine(memtable_threshold=2)
        engine.put("a", "v-a")
        engine.put("b", "v-b")
        engine.delete("b")
        engine.put("c", "v-c")
        self.assertEqual(engine.segment_count(), 2)
        self.assertTrue(engine.compact())
        self.assertEqual(engine.segment_count(), 1)
        self.assertIsNone(engine.get("b"))
        self.assertEqual(engine.get("a"), "v-a")


class RecoveryTests(unittest.TestCase):
    """The write-ahead log has to describe the same state after recovery."""

    def test_recovery_replays_every_wal_record(self):
        engine = LSMEngine(memtable_threshold=4)
        engine.put("a", "v-a")
        engine.put("b", "v-b")
        engine.delete("a")
        engine.put("c", "v-c")
        engine.put("d", "v-d")
        engine.delete("b")
        recovered = LSMEngine.recover(engine.wal, memtable_threshold=4)
        self.assertIsNone(recovered.get("a"))
        self.assertIsNone(recovered.get("b"))
        self.assertEqual(recovered.scan("a", "z"), [("c", "v-c"), ("d", "v-d")])


if __name__ == "__main__":
    unittest.main()
