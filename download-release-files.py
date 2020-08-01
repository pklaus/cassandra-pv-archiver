#!/usr/bin/env python

import sys, re, os
from datetime import datetime as dt
import requests
from bs4 import BeautifulSoup

overview_url = sys.argv[1]
download_url = sys.argv[2]
html = requests.get(overview_url).content
soup = BeautifulSoup(html, features="lxml")
# https://stackoverflow.com/a/24618186/183995
for script in soup(["script", "style"]): script.extract()
text = soup.get_text()
release = re.compile(r"Release (?P<version>\d.\d.\d) \((?P<date>[A-Z][a-z]* \d+[a-z]{2}, \d{4})\)")
for line in (line.strip() for line in text.splitlines() if line.strip()):
    match = release.match(line)
    if match:
        date = match.group("date").replace("1st", "1th").replace("2nd", "2th").replace("3rd", "3th")
        ts = dt.strptime(date, '%B %dth, %Y').timestamp()
        dl = download_url.replace("VERSION", match.group("version"))
        filename = os.path.basename(dl)
        print(f"Downloading {filename} ({date} = {ts}) now...")
        open(filename, 'wb').write(requests.get(dl).content)
        os.utime(filename, (ts, ts))
