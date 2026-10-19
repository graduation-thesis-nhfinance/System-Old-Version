import requests
import re

url = "http://localhost:8085/dags/01_binance_klines_etl_dag/grid"
text = requests.get(url).text

css_files = re.findall(r'href="([^"]*\.css)"', text)
print("CSS files on Grid View:")
for c in css_files:
    print(" -", c)

js_files = re.findall(r'src="([^"]*\.js)"', text)
print("\nJS files on Grid View:")
for j in js_files:
    print(" -", j)
