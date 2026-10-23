import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

url = (f"postgresql+psycopg2://{os.environ['WH_USER']}:{os.environ['WH_PASSWORD']}"
       f"@{os.environ.get('WH_HOST','localhost')}:5432/{os.environ['WH_DB']}")
df = pd.read_sql("SELECT * FROM listings", create_engine(url))

st.title("Notebook มือสอง 5,000–30,000 บาท")
st.dataframe(df)
if not df.empty:
    st.bar_chart(df.groupby("source")["price"].mean())