import requests
import config

r = int(config.get("run_count"))

url = url = config.get("server_url") + "{}"
for i in range(r):
    res = requests.get(url.format(i))
    delay = res.headers.get("DELAY")
    d = res.headers.get("DATE")
    print("{}:{} delay {}".format(d, res.url, delay))
