import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

url = (f"postgresql+psycopg2://{os.environ['WH_USER']}:{os.environ['WH_PASSWORD']}"
       f"@{os.environ.get('WH_HOST', 'localhost')}:5432/{os.environ['WH_DB']}")
df = pd.read_sql("SELECT * FROM listings", create_engine(url))

st.title("Notebook มือสอง 5,000–30,000 บาท")
if df.empty:
    st.warning("ยังไม่มีข้อมูล กรุณารัน DAG ก่อน")
    st.stop()

df["price"] = df["price"].astype(float)

st.sidebar.header("ตัวกรอง")
src_all = sorted(df["source"].unique())
brand_all = sorted(df["brand"].dropna().unique())
sources = st.sidebar.multiselect("แหล่งข้อมูล", src_all, default=src_all)
brands = st.sidebar.multiselect("แบรนด์", brand_all, default=brand_all)
lo, hi = int(df["price"].min()), int(df["price"].max())
price = st.sidebar.slider("ช่วงราคา (บาท)", lo, hi, (lo, hi))

f = df[df["source"].isin(sources) & df["brand"].isin(brands)
       & df["price"].between(*price)]

c1, c2, c3, c4 = st.columns(4)
c1.metric("จำนวนประกาศ", f"{len(f):,}")
c2.metric("ราคาเฉลี่ย (บาท)", f"{f['price'].mean():,.0f}" if len(f) else "-")
c3.metric("ราคาต่ำสุด (บาท)", f"{f['price'].min():,.0f}" if len(f) else "-")
c4.metric("RAM มัธยฐาน (GB)", f"{f['ram_gb'].median():.0f}" if f["ram_gb"].notna().any() else "-")

if len(f):
    st.subheader("ราคาเฉลี่ยตามแบรนด์")
    st.bar_chart(f.groupby("brand")["price"].mean())
st.dataframe(f)