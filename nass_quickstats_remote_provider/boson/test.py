import requests as r

res = r.post("http://localhost:8000/search")
js = res.json()
if "detail" in js:
    print(js["detail"])
else:
    print(js)
