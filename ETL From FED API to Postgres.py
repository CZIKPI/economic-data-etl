#!/usr/bin/env python
# coding: utf-8

# # 📈 FRED Economic Data ETL Pipeline
# 
# This notebook allows you to extract, transform, and load (ETL) time-series economic data from the [FRED](https://fred.stlouisfed.org/) API into a PostgreSQL database using an interactive widget interface.
# 
# ### ✅ Features:
# - Select multiple economic indicators (e.g., Unemployment Rate, CPI, GDP, etc.)
# - Choose custom start and end dates for the data
# - Automatically updates dimension tables (`date_dim`, `series_dim`)
# - Appends clean records to `fed.series_fact` for Power BI or analytics use
# - Hides API credentials using a `.env` file
# 
# ### 🔐 Setup Instructions:
# 1. Store your FRED API key and PostgreSQL connection string in a `.env` file (e.g., `FRED.env`):
#     ```
#     FRED_API_KEY=your_api_key
#     DB_URL=your_postgres_connection_url
#     ```
# 
# 2. Modify the notebook to load the correct `.env` file:
#     ```python
#     load_dotenv("FRED.env")
#     ```
# 
# 3. Run the notebook, select your data series and date range using the widgets, then click **Run ETL**.
# 
# ---
# 
# > This setup is ideal for building a reusable data pipeline that feeds clean macroeconomic indicators into a business intelligence workflow (e.g., Power BI).
# 

# ## PYthon Code for ETL

# In[4]:


import requests
import pandas as pd
from sqlalchemy import create_engine, text
import ipywidgets as widgets
from IPython.display import display
from datetime import datetime, date

# --- CONFIG ---
from dotenv import load_dotenv
load_dotenv("FRED.env")  # Make sure this file contains FRED_API_KEY and DB_URL

import os
API_KEY = os.getenv("FRED_API_KEY")
DB_URL = os.getenv("DB_URL")

if not API_KEY or not DB_URL:
    raise ValueError("❌ Missing credentials in .env file.")

engine = create_engine(DB_URL)

# --- FRED Series (Label → ID) ---
series_dict = {
    "Unemployment Rate": "UNRATE",
    "M2 Money Stock": "M2SL",
    "Federal Funds Rate": "FEDFUNDS",
    "Housing Starts": "HOUST",
    "Gross Domestic Product": "GDP",
    "Discount Rate": "DFF",
    "Gold Prices": "GOLDAMGBD228NLBM",
    "Consumer Price Index (Inflation)": "CPIAUCSL",
    "S&P 500 Index": "SP500"
}

# --- WIDGETS ---
multi_select = widgets.SelectMultiple(
    options=list(series_dict.keys()),
    description='Select Series:',
    style={'description_width': 'initial'},
    layout=widgets.Layout(width='50%', height='120px')
)

start_date_picker = widgets.DatePicker(
    description='Start Date:',
    value=date(1959, 1, 1),
    style={'description_width': 'initial'}
)

end_date_picker = widgets.DatePicker(
    description='End Date:',
    value=date.today(),
    style={'description_width': 'initial'}
)

button = widgets.Button(description="Run ETL for Selected Series")

# --- ETL Function ---
def load_fred_series(series_id, series_name, start, end):
    api_url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": API_KEY,
        "file_type": "json",
        "observation_start": start,
        "observation_end": end
    }
    response = requests.get(api_url, params=params)
    if response.status_code != 200:
        raise Exception(f"FRED API Error: {response.status_code} - {response.text}")

    observations = response.json()["observations"]
    df = pd.DataFrame(observations)
    df["full_date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[["full_date", "value"]].dropna()

    # --- Create date_dim ---
    date_dim = df[["full_date"]].drop_duplicates().copy()
    date_dim["year"] = date_dim["full_date"].dt.year
    date_dim["quarter"] = date_dim["full_date"].dt.quarter
    date_dim["month"] = date_dim["full_date"].dt.month
    date_dim["day"] = date_dim["full_date"].dt.day

    # --- Load date_dim ---
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.date_dim (
                date_id SERIAL PRIMARY KEY,
                full_date DATE UNIQUE,
                year INT,
                quarter INT,
                month INT,
                day INT
            )
        """))
        existing_dates = pd.read_sql("SELECT full_date FROM public.date_dim", conn)
        existing_dates["full_date"] = pd.to_datetime(existing_dates["full_date"])
        new_dates = date_dim[~date_dim["full_date"].isin(existing_dates["full_date"])]
        if not new_dates.empty:
            new_dates.to_sql("date_dim", conn, schema="public", if_exists="append", index=False)

    # --- Load series_dim ---
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fed.series_dim (
                series_id TEXT PRIMARY KEY,
                series_name TEXT
            )
        """))
        result = conn.execute(
            text("SELECT series_id FROM fed.series_dim WHERE series_id = :sid"),
            {"sid": series_id}
        ).fetchone()
        if result is None:
            conn.execute(
                text("INSERT INTO fed.series_dim (series_id, series_name) VALUES (:sid, :sname)"),
                {"sid": series_id, "sname": series_name}
            )

    # --- Load series_fact ---
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fed.series_fact (
                fact_id SERIAL PRIMARY KEY,
                date_id INT REFERENCES public.date_dim(date_id),
                series_id TEXT REFERENCES fed.series_dim(series_id),
                value NUMERIC
            )
        """))
        date_lookup = pd.read_sql("SELECT date_id, full_date FROM public.date_dim", conn)
        date_lookup["full_date"] = pd.to_datetime(date_lookup["full_date"])

    df = df.merge(date_lookup, on="full_date", how="left")
    fact_df = pd.DataFrame({
        "date_id": df["date_id"],
        "series_id": series_id,
        "value": df["value"]
    })

    # 🔒 Filter out any rows without a valid date_id
    fact_df = fact_df.dropna(subset=["date_id"])

    with engine.begin() as conn:
        fact_df.to_sql("series_fact", conn, schema="fed", if_exists="append", index=False)

    print(f"✅ {series_id} ({series_name}) loaded from {start} to {end}")

# --- Button Click Handler ---
def on_button_clicked(b):
    selected_names = multi_select.value
    start_date = start_date_picker.value
    end_date = end_date_picker.value

    if not selected_names:
        print("⚠️ Please select at least one series.")
        return
    if not start_date or not end_date:
        print("⚠️ Please select both start and end dates.")
        return
    if start_date > end_date:
        print("❌ Start date must be before end date.")
        return

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    for name in selected_names:
        series_id = series_dict[name]
        try:
            load_fred_series(series_id, name, start=start_str, end=end_str)
        except Exception as e:
            print(f"❌ Failed to load {series_id}: {e}")

button.on_click(on_button_clicked)

# --- Display UI ---
display(multi_select, start_date_picker, end_date_picker, button)


# In[ ]:




