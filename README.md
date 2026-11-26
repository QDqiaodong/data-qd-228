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

## 整版发布读取

`ReleaseReader` 供网关进程从固定源站、固定清单 target 读取并原子安装一整版配置。
构造只在调用方提供的 `scratch_dir` 下建立实例私有目录，不联网：

```python
from reader import OriginError, ReleaseReader, ReleaseUnavailable

with ReleaseReader(
    "127.0.0.1", port, "/release/manifest.json",
    scratch_dir="/var/cache/gateway-release",
    timeout=2,
) as releases:
    try:
        version = releases.refresh()       # 成功后整版才可读
    except OriginError:
        ...                                # 旧版是否仍可读取决于 serve_min

    if releases.active_version is not None:
        routes = releases.read_active("/config/routes.json")
```

清单是不超过 32 KiB 的严格 UTF-8 JSON 对象，只能包含 `version`、`serve_min`、
`members`；成员只能包含 `target`、`size`、`sha256`。所有整数拒绝布尔值，
成员 target 与清单 target 都使用本库精确 ASCII path/query 校验且不超过 256 字节。
成员最多 64 个，每个最多 1 MiB；摘要必须是响应原始字节的 64 位小写 SHA-256。

`refresh()` 对清单和未复用成员发不带 validator 的普通 GET，不跟随重定向、不重试。
只接受带精确受限 `Content-Length` 的 200；拒绝 `Transfer-Encoding`、非 identity
`Content-Encoding`，以及带 `Vary`、`Set-Cookie`、`Cache-Control` 的响应。
下载采用分块哈希和落盘，不把整版配置读入 Python 堆。

已接受清单会先推进版本基准：拒绝版本倒退、`serve_min` 倒退，以及相同版本的不同
清单。若清单的 `serve_min` 高于本次刷新开始时的活动版本，旧版立即退役；即使新版
安装失败，`active_version` 也变为 `None`。否则失败后继续读取原整版。target、size、
SHA-256 全部匹配的活动成员直接硬链接/复制复用，不发 GET；没有活动版时绝不从旧
候选、LRU 或历史版本补成员。所有成员验证通过后才整体切换版本和 target 集合。

`read_active(target)` 完全本地读盘：无活动版抛 `ReleaseUnavailable`；有活动版但
没有该精确 target 抛 `KeyError`。`close()` 可重复调用，只删除该实例的私有目录。

`test_release_reader.py` 使用回环源站覆盖题设的 v10/v11/v12 复用、退役、拒绝倒退、
恢复安装、target 消失、非法清单、截断成员、留存响应，以及两套完整 64 MiB 发布的
`tracemalloc`/逻辑磁盘预算和关闭清理。

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
