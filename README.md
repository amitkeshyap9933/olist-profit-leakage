# E-Commerce Profit Leakage & RFM Engine

**Where does a marketplace lose money between checkout and a repeat customer — and what should it fix first?**

An end-to-end analytics project on ~100k real orders from Olist, a Brazilian e-commerce marketplace (2016–2018). The pipeline goes from an Excel data audit to a Python ETL, a PostgreSQL warehouse, eight analytical SQL queries and a two-page Power BI dashboard.

![Python](https://img.shields.io/badge/Python-3.11-blue) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791) ![Power BI](https://img.shields.io/badge/Power%20BI-Service-F2C811) ![Excel](https://img.shields.io/badge/Excel-Power%20Query-217346)

---

## Business impact

| Finding | Number | So what |
| --- | --- | --- |
| Late deliveries destroy reviews | Avg score **4.28** on time vs **2.25** late; past 7 days late, about **80%** of reviews are negative | Every day of delay costs more than a shipping refund would |
| Almost nobody comes back | Repeat purchase rate **3.0%** | Growth depends on paid acquisition; retention is the cheapest lever |
| Revenue at risk | **BRL 4.53M** spent by customers now in *At Risk* / *Can't Lose* RFM segments | A targeted win-back campaign has a clear, sized prize |
| Freight eats small baskets | Freight = **16.7%** of product price nationally, **26–28%** in Roraima and Maranhão | Regional free-shipping thresholds or seller subsidies |
| Fulfilment failures | **1.24%** of orders cancelled/unavailable, **BRL 108k** lost GMV | Seller stock-sync alerts |
| Payment reconciliation | **0.37%** of orders don't match billing; 69% of those are instalment-card overpayments | Finance can automate the check instead of sampling |

Total identified leakage: **BRL 5.0M** on BRL 15.7M GMV (lost GMV + vouchers + payment gaps + revenue at risk).

---

## Architecture

```mermaid
flowchart LR
    A[8 raw CSVs<br/>Olist public dataset] --> B[Excel audit<br/>20 DQ checks<br/>mapping tables]
    B -->|mapping + assumption CSVs| C[Python ETL<br/>pandas · SQLAlchemy]
    A --> C
    C -->|cleaned tables| D[(PostgreSQL 16<br/>schema olist)]
    C -->|KPI + DQ report| E[etl_summary_report.xlsx]
    D --> F[SQL analysis<br/>8 queries]
    C -->|compact gz tables| G[Power BI Service<br/>semantic model + 2-page report]
```

---

## Repository structure

```
olist-profit-leakage/
├── data/
│   ├── raw/                  # 8 Olist CSVs (not committed - see Setup)
│   └── mappings/             # status, region, payment, delay, review, category maps + assumptions.csv
├── docs/
│   └── 01_data_audit.xlsx    # Excel audit workbook (DQ checks, Recon, Mappings)
├── src/
│   └── etl_pipeline.py       # extract → clean → features → Excel report → PostgreSQL
├── sql/
│   ├── 01_schema.sql         # DDL: 9 tables, PK/FK, CHECK constraints, indexes
│   └── 02_analysis.sql       # 8 business queries (CTEs, window functions, GROUPING SETS)
├── notebooks/                # exploratory notebook (optional)
├── data/processed/           # compact gzipped tables that feed the Power BI model
├── dashboard/
│   ├── model/                # measures.tmdl (34 DAX measures) + measures_query.dax
│   └── screenshots/          # 01_executive_overview.png, 02_diagnostics.png
├── outputs/
│   ├── etl_summary_report.xlsx
│   └── dq_log.csv
├── requirements.txt
└── README.md
```

---

## Setup

```bash
# 1. clone and install
git clone https://github.com/amitkeshyap9933/olist-profit-leakage.git
cd olist-profit-leakage
pip install -r requirements.txt

# 2. data: download "Brazilian E-Commerce Public Dataset by Olist" from Kaggle
#    and put the 8 CSVs in data/raw/  (the geolocation file is not needed)

# 3. database
createdb olist_dw
export DATABASE_URL="postgresql+psycopg2://USER:PASSWORD@localhost:5432/olist_dw"   # Windows: set DATABASE_URL=...

# 4. run the pipeline (≈2 minutes)
python src/etl_pipeline.py

# 5. run the analysis
psql -d olist_dw -f sql/02_analysis.sql
```

The Power BI model reads `data/processed/*.csv.gz` straight from this repository (Web connector, anonymous access, `Binary.Decompress` + `Csv.Document`), so it refreshes in the Power BI service without a gateway. To rebuild it: create a semantic model from the nine tables, add the relationships, then apply `dashboard/model/measures.tmdl` in TMDL view.

---

## Pipeline steps

1. **Excel audit** — Power Query loads every file; 20 data-quality rules (duplicates, orphans, date order, money sign, payment types, review duplicates) each carry a formula, a threshold and an ETL action. Mapping tables and assumptions are exported as CSV so no rule is hard-coded.
2. **Python ETL** — one function per table, every fix logged to `dq_log.csv` with its rule ID. Feature engineering: delay days and buckets, freight ratio, high-freight and late-handover flags, order-level reconciliation (paid vs billed), RFM per real customer (`customer_unique_id`).
3. **PostgreSQL** — normalised schema with keys, constraints and indexes; the loader verifies row counts after every run.
4. **SQL analysis** — eight queries, one per business question.
5. **Power BI** — a star-style semantic model (7 relationships, 34 DAX measures with formats and display folders) and a two-page report. Every headline measure was checked against the Python KPI output before the report was built.

### Data quality found

| Check | Rows | Action |
| --- | --- | --- |
| Products with no category | 610 | Filled "unknown" |
| Broken date sequence | 1,382 | Excluded from delay KPIs |
| Orders with no items | 775 | Excluded from revenue |
| Extra reviews on one order | 559 | Kept the latest |
| Reconciliation exceptions | 366 | Flagged for finance |

---

## SQL highlights

**Review score by days late** — the headline finding:

```sql
SELECT o.delay_bucket,
       COUNT(*)                                          AS orders,
       ROUND(AVG(r.review_score), 2)                     AS avg_score,
       ROUND(100.0 * AVG((r.review_score <= 2)::int), 1) AS pct_negative
FROM olist.orders o
JOIN olist.reviews r USING (order_id)
WHERE o.delay_bucket IS NOT NULL
GROUP BY o.delay_bucket
ORDER BY MIN(o.delay_days);
```

| Delay | Orders | Avg score | Negative |
| --- | --- | --- | --- |
| On time / early | 88,587 | 4.28 | 9.5% |
| Late 1–3 days | 1,860 | 3.28 | 32.5% |
| Late 4–7 days | 1,795 | 2.09 | 68.1% |
| Late 8–14 days | 1,476 | 1.67 | 80.4% |
| Late 15+ days | 1,379 | 1.72 | 78.6% |

Other queries: freight leakage by state with `RANK()` against the national average, a category Pareto with running totals, a seller risk list driven by parameters stored in the database, reconciliation gaps classified with `CASE`, month-on-month failure trend with `LAG()`, and RFM revenue at risk by region with `GROUPING SETS`.

---

## Dashboard

Built in the Power BI service on a semantic model loaded from this repository.

| Page | What it shows |
| --- | --- |
| 1 · Executive overview | GMV, total leakage, leakage % of GMV, on-time rate and revenue at risk; leakage waterfall from gross demand to net; monthly GMV against on-time rate; customers by RFM segment |
| 2 · Diagnostics | Freight cost as % of price by state; review score by delivery delay; on-time rate by region; category scorecard (GMV, freight ratio, negative reviews, lost GMV) |

![Page 1 – Executive overview](dashboard/screenshots/01_executive_overview.png)
![Page 2 – Diagnostics](dashboard/screenshots/02_diagnostics.png)

Measure check (Power BI vs Python ETL): GMV R$ 15,740,217 · Product revenue R$ 13,221,498 · Lost GMV R$ 108,026 · On-time 93.2% · Avg review on time 4.28 / late 2.25 · Revenue at risk R$ 4,534,859 · Total leakage R$ 4,995,839 — all identical.

---

## Assumptions and limits

- Olist publishes no product costs, so "platform contribution" uses an assumed 10% take rate (`assumptions.csv`), clearly labelled as an assumption.
- Revenue at risk is past spend of customers whose recency and value put them in at-risk segments — a size of the prize, not a forecast.
- The reviews file used here (a public mirror) has 100,000 rows; the Kaggle version has slightly fewer. The ETL keeps one review per order either way.

## Data source

Brazilian E-Commerce Public Dataset by Olist, published on Kaggle. Check the dataset page for its licence terms before reuse; raw files are not committed to this repository.
