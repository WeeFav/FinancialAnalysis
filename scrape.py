from bs4 import BeautifulSoup
import requests

url = "https://finance.yahoo.com/news/nvidia-corporation-nvda-best-money-164657872.html"
header = {'Connection': 'keep-alive',
                'Expires': '-1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) \
                AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36'
                }
result = requests.get(url, headers=header)
doc = BeautifulSoup(result.text, "html.parser")
x = doc.find(class_="article yf-l7apfj")
y = x.find_all(class_="yf-1090901")

for z in y:
  print(z.prettify())