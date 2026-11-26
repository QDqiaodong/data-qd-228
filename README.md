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

## 进程内发布源站

`ReleaseOrigin` 是上面的对侧：一个进程内、受配额约束的配置发布源站，直接与
`ReleaseReader` 的清单/成员协议互通。它在 `127.0.0.1` 的临时端口提供 HTTP，
状态不跨重启，也不提供上传服务：

```python
from release_origin import ReleaseCapacityError, ReleaseOrigin

with ReleaseOrigin(max_bytes=12 * 1024, max_versions=8) as origin:
    origin.publish(10, {"S": b"...3 KiB...", "A": b"...2 KiB..."})
    origin.publish(11, {"S": b"...3 KiB...", "B": b"...3 KiB..."})

    origin.hold(10)          # 护住已知良好的回退点
    try:
        origin.restore(10, 12)   # 以严格递增的新版本号重发原始成员集
    except ReleaseCapacityError:
        ...                      # 无合规清理方案，状态完全未变

    origin.retained_versions()   # 升序元组，含当前版本
    origin.active_version        # 尚未发布时为 None
    reader = ReleaseReader(
        "127.0.0.1", origin.port, origin.manifest_target,
        scratch_dir="/var/cache/gateway-release")
```

`publish(version, members)` 的版本号是 1 至 2⁶³−1 的严格整数且必须大于当前发布；
成员是 1–64 个、逻辑名匹配 `[A-Za-z0-9][A-Za-z0-9_.-]{0,63}`、值至多 1 MiB 的
不可变 `bytes`（允许空）。发布时复制字节，调用方之后改动原字典不影响源站。每个成员
的 target 为 `/members/<逻辑名>/<内容 SHA-256>`，同名同字节沿用同一 target。清单固定
在 `/release/manifest.json`，只公布当前发布，`serve_min` 恒为 0，无发布时 404。
`hold`/`unhold` 幂等、不切换发布也不清理对象；对不存在（含已退役）的版本抛
`KeyError`，非法版本号抛 `ValueError`。

`restore(source_version, new_version)` 从仍留存的版本取完整原始成员集，以严格递增的
`new_version` 重新发布；来源版本在该次操作中不会被清理，恢复不继承也不清空保留
标记。留存准入同时受两项配额约束：所有留存版本引用的不同成员 target 的字节总和
不超过 `max_bytes`（0–512 MiB），留存版本数不超过 `max_versions`（2–8）。新版本、
切换前的当前版本（直接回退点）、所有保留标记对应版本以及恢复来源都必须留存；若需
删除历史版本以同时满足两项配额，删除的版本数最少，多版共享的 target 只计一份字节。
没有合规方案时抛 `ReleaseCapacityError`，不先清理一部分，也不消耗版本号；成功后
清单、可读成员与留存集合一起生效。

单次 GET 在选中清单或成员的那一刻取快照，与管理操作重叠时只会看到切换前或切换后
的完整状态；已选中的成员响应不因其版本随后被清理而截断。target 只要仍被任一留存
版本引用就能读到原字节，最后一个引用消失后返回 404；未保留的历史成员不承诺长期
可读，值班流程应先 `hold` 再跨多次操作读取。

`test_release_origin.py` 用真实回环 GET 与 `ReleaseReader` 覆盖题设的 v10–v14 回退
与恢复、容量拒绝的无副作用与重试、共享成员的最后引用释放、按版数准入、保留标记
语义、在途响应留存，以及参数校验和关闭行为。

## 大型二进制页表读取

`PagedReader` 按 4096 字节固定页读取加载器从发布端拿到的单个大型对象（规则索引或
规则页）。实例在构造时绑定调用方给出的精确 target、总字节数和带引号强 ETag，之后
不发现、不切换源站版本，构造本身不联网：

```python
from reader import OriginError, PageBudgetExceeded, PagedReader

reader = PagedReader(
    "127.0.0.1", port, "/rules/index.bin?table=main",
    size=4096 * 4096,          # 4096 的正整数倍，最大 256 MiB
    etag='"v1-strong-tag"',    # 沿用库内带引号强标签规则
    retained_pages=64,         # 本地留页名额，0..64
    timeout=2,
)
try:
    pages = reader.read_pages(
        [43, 40, 43],           # 至多 32 个页号，按输入顺序返回、保留重复
        max_gets=1,             # 本批可用 GET 数，0..32
        max_body_bytes=16384,   # 本批响应体字节预算，0..1 MiB
        keep=False,
    )
except PageBudgetExceeded:
    ...                        # 预算内无可行方案或留页容量不足，发包前拒绝
except OriginError:
    ...                        # 源站协议、标签或传输失败，整批不交付不留存
```

target 沿用库内精确 ASCII origin-form path/query 校验且不超过 256 字节；size 必须是
4096 的正整数倍且不超过 256 MiB；所有整数参数拒绝布尔值，`keep` 必须是 bool；
格式或范围错误抛 `ValueError`，且不回源、不改变已保留页。

### 留页与回源预算是两个独立约束

留页是该实例绑定对象的本地字节，后续读取不再向源站确认，源站换版或不可用时仍可
读出；这些字节属于调用方指定的旧对象，不会被用来冒充另一版本。只有 `keep=True`
且整批成功时，本次明确请求的页才进入保留集合：重复页只占一个名额，为跨空洞顺带
收到的页不留存，且不淘汰旧页；容量不足以容纳全部新增页时整批失败。普通读可以
使用已有留页但不新增。

每批先按已保留页确定缺页，并在发出任何 GET 前判断可行性；全部命中时不发 GET。
单个 GET 取一个连续页段，段长不超过 8 页，可以跨过未请求的空洞或已保留页以节省
请求数；整段字节（含空洞和重复下载的字节）都计入 `max_body_bytes`。只有两个预算
都能覆盖全部缺页时才开始发包；库内规划最小化 GET 数，再最小化回源字节，但不保证
在所有可行方案中总选最优。预算内无可行方案或留页容量不足时抛 `PageBudgetExceeded`，
保留集合不变。

### 回源协议与失败语义

每个段对原 target 发一个带 `Range: bytes=起点-终点`（终点包含）和 `If-Match: <etag>`
的 GET，固定源站、不跟随重定向、不重试。只接受单段 `206`：状态、ETag、恰好出现
一次的 `Content-Range: bytes 起点-终点/总长` 和与段长精确相等的单一
`Content-Length` 全部先校验，再读响应体；拒绝 `Transfer-Encoding`、非 identity 的
`Content-Encoding` 和截断响应。忽略 Range 的 200、412、边界/总长/标签不符及传输
失败都抛 `OriginError`，不读诊断体，也不回退成整对象 GET。

一段失败即整批失败：已经收到的页既不交付也不留存；此前成功保留的页仍可正常读取。
`close()` 清空实例持有的页且可重复调用，关闭后的读取抛 `RuntimeError`。实例按串行
使用设计，不含线程、定时器或磁盘 I/O。

`test_paged_reader.py` 使用自制回环源站按页生成可区分字节，记录实际 target、
Range 与 If-Match，覆盖预算收紧/放宽下的合并段与单页段、全命中零回源、留页满
拒绝、顺带页不留存、源站换版后旧留页可读、412/错误 Content-Range/截断/忽略 Range/
重复头/Transfer-Encoding 失败不留存，以及全部参数校验和关闭行为。

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
