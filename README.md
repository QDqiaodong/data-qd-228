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
