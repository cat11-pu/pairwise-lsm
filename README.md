# pairwise-lsm

一个内存版的 LSM 存储引擎实现，用于练习读路径、刷盘、段合并与日志恢复。
只依赖 Python 标准库，不需要安装任何第三方包。

## 目录结构

- `lsm/core.py` ：引擎内核，包含 MemTable、SSTable、WAL 与 LSMEngine
- `tests/test_core.py` ：引擎的行为测试

## 运行测试

在项目根目录执行：

```
python3 -m unittest discover -s tests -v
```

Windows 上如果 `python3` 不在 PATH 中，可以换成完整路径的 Python 解释器，
例如：

```
C:/Users/Administrator/AppData/Local/Programs/Python/Python313/python.exe -m unittest discover -s tests -v
```

测试全部通过时，unittest 结尾会打印 `OK`。
