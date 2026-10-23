import os
import re
import time
from datetime import datetime
from urllib.robotparser import RobotFileParser

import psycopg2
import requests
from airflow.decorators import dag, task
from bs4 import BeautifulSoup
from psycopg2.extras import execute_values

BASE = "https://talata.co.th"
LIST_URL = BASE + "/en/categories/electronics/laptops"
UA = "Mozilla/5.0 (compatible; KU-Student-Project/1.0)"
MAX_PAGES = 6
PRICE_MIN, PRICE_MAX = 5000, 30000

BRANDS = [("ThinkPad", "Lenovo"), ("IdeaPad", "Lenovo"), ("Lenovo", "Lenovo"),
          ("MacBook", "Apple"), ("Apple", "Apple"), ("Dell", "Dell"),
          ("ZBook", "HP"), ("EliteBook", "HP"), ("ProBook", "HP"), ("HP", "HP"),
          ("Asus", "Asus"), ("Acer", "Acer"), ("MSI", "MSI"),
          ("Surface", "Microsoft"), ("Samsung", "Samsung"), ("Huawei", "Huawei")]

COLS = ["source", "listing_id", "url", "title", "brand", "cpu",
        "ram_gb", "storage_gb", "price", "province", "condition"]


def parse_specs(title):
    brand = next((b for k, b in BRANDS
                  if re.search(rf"\b{k}\b", title, re.I)), None)
    m = re.search(r"(Core Ultra \d|\bi[3579]\b|Ryzen \d|Xeon|\bM[1-4]\b)", title, re.I)
    sizes = [int(n) * (1024 if u.upper() == "TB" else 1)
             for n, u in re.findall(r"(\d+)\s*(GB|TB)", title, re.I)]
    return {
        "brand": brand,
        "cpu": m.group(1) if m else None,
        "ram_gb": next((s for s in sizes if s <= 64), None),
        "storage_gb": next((s for s in sizes if s >= 120), None),
    }


@task
def extract():
    rp = RobotFileParser(BASE + "/robots.txt")
    rp.read()
    if not rp.can_fetch(UA, LIST_URL):
        raise RuntimeError("robots.txt ไม่อนุญาตให้ดึงหน้านี้")

    found = {}
    for page in range(1, MAX_PAGES + 1):
        r = requests.get(LIST_URL, params={"page": page},
                         headers={"User-Agent": UA}, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        anchors = soup.select('a[href*="/listing/"]')
        if not anchors:
            break
        for a in anchors:
            href = a["href"]
            lid = href.rstrip("/").split("/")[-1]
            url = href if href.startswith("http") else BASE + href
            found[lid] = {"listing_id": lid, "url": url,
                          "text": a.get_text(" ", strip=True)}
        time.sleep(1.5)
    return list(found.values())


@task
def transform(raw):
    rows = {}
    for item in raw:
        m = re.search(r"฿\s*([\d,]+)\s+(.*?)\s+(Used|New)\s+(.*?)\s*Write", item["text"])
        if not m:
            continue
        price = int(m.group(1).replace(",", ""))
        title, cond = m.group(2), m.group(3)
        if cond != "Used" or not (PRICE_MIN <= price <= PRICE_MAX):
            continue
        parts = [p.strip() for p in m.group(4).split(",")]
        if parts and parts[-1] == "Thailand":
            parts = parts[:-1]
        rows[item["listing_id"]] = {
            "source": "talata", "listing_id": item["listing_id"],
            "url": item["url"], "title": title, "price": price,
            "province": parts[-1] if parts else None, "condition": cond,
            **parse_specs(title),
        }
    return list(rows.values())


@task
def load(rows):
    conn = psycopg2.connect(host=os.getenv("WH_HOST", "warehouse"),
                            dbname=os.environ["WH_DB"],
                            user=os.environ["WH_USER"],
                            password=os.environ["WH_PASSWORD"])
    sql = (f"INSERT INTO listings ({','.join(COLS)}) VALUES %s "
           "ON CONFLICT (source, listing_id) DO UPDATE SET "
           + ", ".join(f"{c}=EXCLUDED.{c}" for c in COLS[2:])
           + ", scraped_at = now()")
    with conn, conn.cursor() as cur:
        execute_values(cur, sql, [tuple(r.get(c) for c in COLS) for r in rows])
    conn.close()


@dag(dag_id="notebook_pipeline", start_date=datetime(2026, 1, 1),
     schedule=None, catchup=False)
def notebook_pipeline():
    load(transform(extract()))


notebook_pipeline()