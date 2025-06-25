# Economic Data ETL Pipeline with FRED API, PostgreSQL & Power BI

This project demonstrates a complete **ETL pipeline** that extracts economic time series data from the [Federal Reserve Economic Data (FRED) API](https://fred.stlouisfed.org/docs/api/fred/), loads it into a normalized **PostgreSQL** database, and enables interactive reporting via **Power BI**.

---

## 🚀 Project Overview

- **Extract**: Pulls multiple economic indicators such as Unemployment Rate, M2 Money Stock, Federal Funds Rate, Housing Starts, GDP, CPI, and more from the FRED API.
- **Transform**: Cleans data, builds dimensional tables (`date_dim`, `series_dim`), and prepares fact tables.
- **Load**: Inserts data into PostgreSQL tables with referential integrity.
- **Visualize**: Use Power BI (sample dashboard included) to create interactive reports and trend analysis.

---

## 🛠️ Tech Stack

| Component            | Technology                 |
| -------------------- | --------------------------|
| Data Extraction      | Python (requests, pandas)  |
| Data Loading         | SQLAlchemy, PostgreSQL     |
| Scheduling (optional)| Windows Task Scheduler     |
| Visualization        | Power BI                   |
| Environment Config   | Python-dotenv (.env file)  |

---

## 📁 Repository Structure

