import requests as r

res = r.post("http://localhost:8000/search")
print(res.content)
