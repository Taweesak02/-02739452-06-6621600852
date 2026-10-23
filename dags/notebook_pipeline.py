import os
import re
import time
from datetime import datetime
from urllib.robotparser import RobotFileParser

import psycopg2
from datetime import timedelta
import requests
from airflow.decorators import dag, task, task_group
from bs4 import BeautifulSoup
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


UA = "Mozilla/5.0 (compatible; KU-Student-Project/1.0)"
PRICE_MIN, PRICE_MAX = 5000, 30000

COLS = ["source", "listing_id", "url", "title", "brand", "cpu",
        "ram_gb", "storage_gb", "price", "province", "condition"]

BRANDS = [("ThinkPad", "Lenovo"), ("IdeaPad", "Lenovo"), ("Lenovo", "Lenovo"),
          ("MacBook", "Apple"), ("Apple", "Apple"), ("Dell", "Dell"),
          ("ZBook", "HP"), ("EliteBook", "HP"), ("ProBook", "HP"), ("HP", "HP"),
          ("Asus", "Asus"), ("Acer", "Acer"), ("MSI", "MSI"),
          ("Surface", "Microsoft"), ("Samsung", "Samsung"), ("Huawei", "Huawei")]


# ---------- helpers ----------
def make_session():
    s = requests.Session()
    s.headers["User-Agent"] = UA
    retry = Retry(total=4, connect=4, read=2, backoff_factor=2,
                  status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def check_robots(base, url):
    r = make_session().get(base + "/robots.txt", timeout=(5, 30))
    if r.status_code == 200:
        rp = RobotFileParser()
        rp.parse(r.text.splitlines())
        if not rp.can_fetch(UA, url):
            raise RuntimeError(f"robots.txt ไม่อนุญาต: {url}")
    elif r.status_code in (401, 403):
        raise RuntimeError(f"อ่าน robots.txt ไม่ได้ ({r.status_code})")


def parse_specs(title):
    brand = next((b for k, b in BRANDS
                  if re.search(rf"\b{k}\b", title, re.I)), "Other")
    m = re.search(r"(Core Ultra \d|\bi[3579]\b|Ryzen \d|Xeon|\bM[1-4]\b)", title, re.I)
    sizes = [int(n) * (1024 if u.upper() == "TB" else 1)
             for n, u in re.findall(r"(\d+)\s*(GB|TB)", title, re.I)]
    r = re.search(r"RAM\s*(\d+)\s*GB|(\d+)\s*GB\s*(?:DDR\d\s*)?RAM", title, re.I)
    if r:
        ram = int(r.group(1) or r.group(2))
    else:
        ram = next((s for s in sizes if s <= 64), None)
    rest = list(sizes)
    if ram in rest:
        rest.remove(ram)
    storage = next((s for s in rest if s >= 32), None)
    return {"brand": brand, "cpu": m.group(1) if m else None,
            "ram_gb": ram, "storage_gb": storage}


# ---------- Talata ----------
TL_BASE = "https://talata.co.th"
TL_LIST = TL_BASE + "/en/categories/electronics/laptops"


@task
def extract_talata():
    session = make_session()
    check_robots(TL_BASE, TL_LIST)
    found = {}
    for page in range(1, 7):
        r = session.get(TL_LIST, params={"page": page}, timeout=(5, 30))
        if r.status_code == 404:      # เกินหน้าสุดท้ายแล้ว
            break
        r.raise_for_status()
        anchors = BeautifulSoup(r.text, "html.parser").select('a[href*="/listing/"]')
        if not anchors:
            break
        for a in anchors:
            href = a["href"]
            lid = href.rstrip("/").split("/")[-1]
            found[lid] = {"listing_id": lid,
                          "url": href if href.startswith("http") else TL_BASE + href,
                          "text": a.get_text(" ", strip=True)}
        time.sleep(1.5)
    return list(found.values())


@task
def transform_talata(raw):
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
            **parse_specs(title)}
    return list(rows.values())


# ---------- S Sale Com ----------
SS_BASE = "https://s-salecom.com"
SS_LIST = SS_BASE + "/product-category/notebook/"
SS_PARAMS = {"min_price": PRICE_MIN, "max_price": PRICE_MAX,
             "filter_product_cat": "notebook", "query_type_product_cat": "or"}
PRICE_RE = re.compile(r"([\d,]+)\s*บาท")


@task
def extract_ssale():
    check_robots(SS_BASE, SS_LIST)
    found = {}
    for page in range(1, 21):
        session = make_session()
        url = SS_LIST if page == 1 else f"{SS_LIST}page/{page}/"
        r = session.get(TL_LIST, params={"page": page}, timeout=(5, 30))
        if r.status_code == 404:
            break
        r.raise_for_status()
        anchors = BeautifulSoup(r.text, "html.parser").select('a[href*="/product/"]')
        n_before = len(found)
        for a in anchors:
            text = a.get_text(" ", strip=True)
            if "บาท" not in text:
                continue
            href = a["href"]
            lid = href.rstrip("/").split("/")[-1]
            found[lid] = {"listing_id": lid, "url": href, "text": text}
        if len(found) == n_before:
            break
        time.sleep(1.5)
    return list(found.values())


@task
def transform_ssale(raw):
    rows = {}
    for item in raw:
        text = item["text"]
        prices = PRICE_RE.findall(text)
        if not prices:
            continue
        price = int(prices[-1].replace(",", ""))
        if not (PRICE_MIN <= price <= PRICE_MAX):
            continue
        title = text[:PRICE_RE.search(text).start()].strip()
        half = len(title) // 2
        if title[:half].strip() == title[half:].strip():
            title = title[:half].strip()
        rows[item["listing_id"]] = {
            "source": "ssalecom", "listing_id": item["listing_id"],
            "url": item["url"], "title": title, "price": price,
            "province": "Pathum Thani", "condition": "Used",
            **parse_specs(title)}
    return list(rows.values())


# ---------- load ----------
@task
def load(talata_rows, ssale_rows):
    rows = talata_rows + ssale_rows
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


# ---------- DAG ----------
@dag(dag_id="notebook_pipeline", start_date=datetime(2026, 1, 1),
     schedule=None, catchup=False,
     default_args={"retries": 2, "retry_delay": timedelta(seconds=30)})
def notebook_pipeline():
    @task_group(group_id="talata")
    def talata():
        return transform_talata(extract_talata())

    @task_group(group_id="ssalecom")
    def ssalecom():
        return transform_ssale(extract_ssale())

    load(talata(), ssalecom())


notebook_pipeline()