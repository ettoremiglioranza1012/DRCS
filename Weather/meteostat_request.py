import http.client

conn = http.client.HTTPSConnection("meteostat.p.rapidapi.com")

headers = {
    'x-rapidapi-key': "fb0eba011emsh1233c476ec1f6b4p1d1667jsn7377fb731294",
    'x-rapidapi-host': "meteostat.p.rapidapi.com"
}

conn.request("GET", "/stations/hourly?station=16023&start=2025-01-01&end=2025-01-15&tz=Europe%2FBerlin", headers=headers)

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))