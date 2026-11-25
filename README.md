# 单源 HTTP 条件读取器

供进程内调用方读取单一受控 HTTP 源站的完整资源。按精确的 path/query 保留有界
LRU 实体；已存实体的每次读取都发 `If-None-Match`。匹配的 304 复用旧字节，
新的 200 取代旧实体；错误不会用旧内容兜底。调用方获得字节及是否经 304 确认。

Python 3.9+，标准库。演示使用自制资源和两个版本，经真实回环 HTTP 服务读取：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install --no-index -r requirements.txt
.venv/bin/python -m unittest -v
.venv/bin/python demo.py
```

预期依次 `200 build=101`、`304 build=101`、`200 build=102`。
使用 RFC 9110 §13.1.2 的条件 GET 机制，不实现通用代理缓存。
源站约定：200 必须有受限 Content-Length，304 须带回请求里相同的强 ETag。
仅保留带强 ETag 且无 Vary、Set-Cookie、Cache-Control 的完整实体，
无 TTL、授权请求、TLS、重定向、范围传输或并发共享；只用于调用方管理的单一源站。

## 并发调度

`ScheduledReader` 在进程内合并相同精确 target 的并发读取：

```python
import time
from reader import ScheduledReader, QueueFull, RequestCancelled

with ScheduledReader(
    "127.0.0.1", port,
    parallelism=2,   # 1..8 个在途传输/收尾槽
    max_pending=16,  # 0..64 个未派发工作项（合并不新增项）
    max_waiters=64,  # 1..128 张未终结且未到期的票
) as scheduler:
    ticket = scheduler.submit("/gateway.json",
                              deadline=time.monotonic() + 1.0)
    try:
        representation = ticket.result()
    except TimeoutError:
        ...
    except RequestCancelled:
        ...

    # 只有本票从未终结、未到期时才返回 True。
    ticket.cancel()
```

相同 target 的有效票共享一次条件 GET，但每张票独立兑现结果、取消和截止时间。
请求实际派发时才读取当前 LRU validator 并生成 `If-None-Match`；在途 304 会使用
派发时快照的实体字节，即使该实体随后被其它 target 挤出单项 LRU。旧工作失去最后
一张有效票后，其迟到响应不会写入或清除缓存，也不会交付给之后的新工作。

运行全部标准库测试：

```sh
python3 -m unittest -v
```

`test_scheduled_reader.py` 使用 `ThreadingHTTPServer`、`threading.Event` 和可控
时钟观察请求合并、FIFO 派发、容量拒绝、取消/到期、LRU 交叉完成与关闭行为。
