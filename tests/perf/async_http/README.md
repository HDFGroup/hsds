# async http example

Example of using aiohttp and asyncio for http requests.

Derived from code in: https://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html

Start server:

```$python hello_serv.py```

Sync client:

```python sync_client.py [--run_count=n]```

Async client:

```python async_client.py [--run_count=n]```

(runs without failure up to ~1000 requess)

async_batch_client:

```python async_batch_client.py [--run_count=n] [--batch_size=m]```

