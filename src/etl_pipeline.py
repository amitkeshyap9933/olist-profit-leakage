"""
E-Commerce Profit Leakage & RFM Engine -- ETL pipeline
=======================================================

Extract  : 8 raw Olist CSVs (data/raw) + mapping/assumption CSVs exported
           from the Excel audit workbook (data/mappings).
Transform: clean types, keys, duplicates and orphans; log every fix;
           engineer freight, delivery, reconciliation and RFM features.
Load     : (1) outputs/etl_summary_report.xlsx  -- KPI + diagnostic tables
           (2) PostgreSQL schema "olist"         -- normalised tables

Usage
-----
    # 1. set the database URL (never hard-code credentials)
    export DATABASE_URL="postgresql+psycopg2://user:password@localhost:5432/olist_dw"
    # 2. run
    python src/etl_pipeline.py
    # options
    python src/etl_pipeline.py --skip-db          # Excel report only
    python src/etl_pipeline.py --raw-dir other/path

Requires: pandas>=2.0, numpy, sqlalchemy>=2.0, psycopg2-binary, openpyxl
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_FILES = {
    "orders": "olist_orders_dataset.csv",
    "items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "category": "product_category_name_translation.csv",
}

ORDER_DATE_COLS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

REVIEW_COLS = [
    "review_id", "order_id", "review_score", "review_comment_title",
    "review_comment_message", "review_creation_date", "review_answer_timestamp",
]

log = logging.getLogger("etl")


# ---------------------------------------------------------------------------
# Data-quality log: every rule that changes or drops data leaves a record
# ---------------------------------------------------------------------------
@dataclass
class DQLog:
    rows: list[dict] = field(default_factory=list)

    def add(self, rule_id: str, table: str, check: str, affected: int, action: str) -> None:
        self.rows.append(
            {"rule_id": rule_id, "table": table, "check": check,
             "rows_affected": int(affected), "action": action}
        )
        if affected:
            log.info("%s %-9s %-45s %8d -> %s", rule_id, table, check, affected, action)

    def frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows)


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
def load_raw(raw_dir: Path) -> dict[str, pd.DataFrame]:
    """Read the raw CSVs. IDs and zip prefixes stay text; dates are parsed later."""
    missing = [f for f in RAW_FILES.values() if not (raw_dir / f).exists()]
    if missing:
        raise FileNotFoundError(f"Missing raw files in {raw_dir}: {missing}")

    id_text = {"customer_zip_code_prefix": str, "seller_zip_code_prefix": str}
    raw = {
        "orders": pd.read_csv(raw_dir / RAW_FILES["orders"]),
        "items": pd.read_csv(raw_dir / RAW_FILES["items"]),
        "payments": pd.read_csv(raw_dir / RAW_FILES["payments"]),
        "customers": pd.read_csv(raw_dir / RAW_FILES["customers"], dtype=id_text),
        "products": pd.read_csv(raw_dir / RAW_FILES["products"]),
        "sellers": pd.read_csv(raw_dir / RAW_FILES["sellers"], dtype=id_text),
        # Some copies of the reviews file carry blank trailing columns: keep the 7 real ones.
        "reviews": pd.read_csv(raw_dir / RAW_FILES["reviews"], usecols=range(7)),
        # The translation file starts with a byte-order mark.
        "category": pd.read_csv(raw_dir / RAW_FILES["category"], encoding="utf-8-sig"),
    }
    raw["reviews"].columns = REVIEW_COLS
    for name, df in raw.items():
        log.info("loaded %-10s %8d rows x %2d cols", name, len(df), df.shape[1])
    return raw


def load_mappings(map_dir: Path) -> dict[str, pd.DataFrame]:
    """Mapping tables and assumptions exported from the Excel audit workbook."""
    names = ["map_status", "map_region", "map_payment", "map_delay_bucket",
             "map_review_band", "map_category", "assumptions"]
    maps = {n: pd.read_csv(map_dir / f"{n}.csv", encoding="utf-8-sig") for n in names}
    return maps


def assumption_dict(maps: dict[str, pd.DataFrame]) -> dict[str, float]:
    a = maps["assumptions"].set_index("name")["value"].astype(float).to_dict()
    required = {"freight_flag_ratio", "recon_tolerance", "otd_target", "seller_late_flag",
                "min_seller_items", "take_rate", "price_outlier_pct"}
    gap = required - a.keys()
    if gap:
        raise KeyError(f"assumptions.csv is missing: {sorted(gap)}")
    return a


# ---------------------------------------------------------------------------
# Transform: generic helpers
# ---------------------------------------------------------------------------
def strip_text(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace in every text column."""
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].str.strip()
    return df


def drop_duplicate_keys(df: pd.DataFrame, keys: list[str], table: str, rule: str,
                        dq: DQLog, keep: str = "first") -> pd.DataFrame:
    dupes = df.duplicated(subset=keys, keep=keep)
    dq.add(rule, table, f"duplicate key {keys}", dupes.sum(), "dropped duplicate rows")
    return df.loc[~dupes].copy()


def drop_orphans(df: pd.DataFrame, key: str, parent_keys: pd.Series, table: str,
                 rule: str, dq: DQLog) -> pd.DataFrame:
    orphan = ~df[key].isin(parent_keys)
    dq.add(rule, table, f"{key} not found in parent", orphan.sum(), "dropped orphan rows")
    return df.loc[~orphan].copy()


# ---------------------------------------------------------------------------
# Transform: one function per table
# ---------------------------------------------------------------------------
def clean_customers(df: pd.DataFrame, maps, dq: DQLog) -> pd.DataFrame:
    df = strip_text(df)
    df = drop_duplicate_keys(df, ["customer_id"], "customers", "DQ16", dq)
    df["customer_state"] = df["customer_state"].str.upper()
    df["customer_city"] = df["customer_city"].str.title()
    df["customer_zip_code_prefix"] = df["customer_zip_code_prefix"].str.zfill(5)
    region = maps["map_region"].set_index("state")["region"]
    df["region"] = df["customer_state"].map(region).fillna("Unknown")
    return df.rename(columns={
        "customer_zip_code_prefix": "zip_code_prefix",
        "customer_city": "city",
        "customer_state": "state",
    })[["customer_id", "customer_unique_id", "zip_code_prefix", "city", "state", "region"]]


def clean_sellers(df: pd.DataFrame, maps, dq: DQLog) -> pd.DataFrame:
    df = strip_text(df)
    df = drop_duplicate_keys(df, ["seller_id"], "sellers", "DQ21", dq)
    df["seller_state"] = df["seller_state"].str.upper()
    df["seller_city"] = df["seller_city"].str.title()
    df["seller_zip_code_prefix"] = df["seller_zip_code_prefix"].str.zfill(5)
    region = maps["map_region"].set_index("state")["region"]
    df["region"] = df["seller_state"].map(region).fillna("Unknown")
    return df.rename(columns={
        "seller_zip_code_prefix": "zip_code_prefix",
        "seller_city": "city",
        "seller_state": "state",
    })[["seller_id", "zip_code_prefix", "city", "state", "region"]]


def clean_products(df: pd.DataFrame, maps, dq: DQLog) -> pd.DataFrame:
    df = strip_text(df)
    df = drop_duplicate_keys(df, ["product_id"], "products", "DQ22", dq)

    missing_cat = df["product_category_name"].isna()
    dq.add("DQ17", "products", "missing category", missing_cat.sum(), "filled 'unknown'")
    df["category_pt"] = df["product_category_name"].fillna("unknown")

    cat_map = maps["map_category"].set_index("product_category_name")["product_category_name_english"]
    df["category_en"] = df["category_pt"].map(cat_map)
    untranslated = df["category_en"].isna() & (df["category_pt"] != "unknown")
    dq.add("DQ18", "products", "category with no English name", untranslated.sum(),
           "kept Portuguese name - add it to map_category.csv")
    df["category_en"] = df["category_en"].fillna(df["category_pt"])

    # Olist's own headers are misspelt ("lenght"); rename to clean names.
    df = df.rename(columns={
        "product_photos_qty": "photos_qty",
        "product_weight_g": "weight_g",
        "product_length_cm": "length_cm",
        "product_height_cm": "height_cm",
        "product_width_cm": "width_cm",
    })
    for col in ["photos_qty", "weight_g", "length_cm", "height_cm", "width_cm"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")
    return df[["product_id", "category_pt", "category_en", "photos_qty",
               "weight_g", "length_cm", "height_cm", "width_cm"]]


def clean_orders(df: pd.DataFrame, customers: pd.DataFrame, maps, dq: DQLog) -> pd.DataFrame:
    df = strip_text(df)
    df = drop_duplicate_keys(df, ["order_id"], "orders", "DQ01", dq)
    df["order_status"] = df["order_status"].str.lower()

    for col in ORDER_DATE_COLS:
        parsed = pd.to_datetime(df[col], errors="coerce")
        bad = parsed.isna() & df[col].notna()
        dq.add("DQ02", "orders", f"unparseable date in {col}", bad.sum(), "set to null")
        df[col] = parsed

    df = drop_orphans(df, "customer_id", customers["customer_id"], "orders", "DQ23", dq)

    status = maps["map_status"].set_index("order_status")["status_group"]
    df["status_group"] = df["order_status"].map(status).fillna("In progress")

    delivered = df["order_status"].eq("delivered")
    no_date = delivered & df["order_delivered_customer_date"].isna()
    dq.add("DQ03", "orders", "delivered but no delivery date", no_date.sum(),
           "kept in GMV, excluded from delivery KPIs")

    conflict = ~delivered & df["order_delivered_customer_date"].notna()
    dq.add("DQ04", "orders", "not delivered but has delivery date", conflict.sum(),
           "flagged status_conflict")
    df["status_conflict"] = conflict

    p, a, c, d = (df["order_purchase_timestamp"], df["order_approved_at"],
                  df["order_delivered_carrier_date"], df["order_delivered_customer_date"])
    seq_bad = (a.notna() & (a < p)) | (c.notna() & a.notna() & (c < a)) | \
              (d.notna() & c.notna() & (d < c))
    dq.add("DQ05", "orders", "date sequence broken", seq_bad.sum(),
           "flagged date_sequence_issue, excluded from delay KPIs")
    df["date_sequence_issue"] = seq_bad

    # Delay in whole calendar days: delivered date minus promised date.
    usable = delivered & d.notna() & df["order_estimated_delivery_date"].notna() & ~seq_bad
    delay = (d.dt.normalize() - df["order_estimated_delivery_date"].dt.normalize()).dt.days
    df["delay_days"] = delay.where(usable).astype("Int64")
    df["is_late"] = (df["delay_days"] > 0).astype("boolean").where(usable)

    buckets = maps["map_delay_bucket"].sort_values("lower_days")
    edges = list(buckets["lower_days"]) + [np.inf]
    df["delay_bucket"] = pd.cut(df["delay_days"].astype("float"), bins=edges, right=False,
                                labels=list(buckets["delay_bucket"])).astype("object")
    df["delay_bucket"] = df["delay_bucket"].where(usable, None)

    df["purchase_month"] = df["order_purchase_timestamp"].dt.to_period("M").dt.to_timestamp()
    return df


def clean_items(df: pd.DataFrame, orders: pd.DataFrame, products: pd.DataFrame,
                sellers: pd.DataFrame, a: dict, dq: DQLog) -> pd.DataFrame:
    df = strip_text(df)
    df = drop_duplicate_keys(df, ["order_id", "order_item_id"], "items", "DQ24", dq)
    df = drop_orphans(df, "order_id", orders["order_id"], "items", "DQ07", dq)
    df = drop_orphans(df, "product_id", products["product_id"], "items", "DQ25", dq)
    df = drop_orphans(df, "seller_id", sellers["seller_id"], "items", "DQ26", dq)

    bad_money = (df["price"] <= 0) | (df["freight_value"] < 0)
    dq.add("DQ08", "items", "price <= 0 or freight < 0", bad_money.sum(), "dropped")
    df = df.loc[~bad_money].copy()

    df["shipping_limit_date"] = pd.to_datetime(df["shipping_limit_date"], errors="coerce")
    df["freight_ratio"] = (df["freight_value"] / df["price"]).round(4)
    df["high_freight"] = df["freight_ratio"] > a["freight_flag_ratio"]
    dq.add("DQ10", "items", "high-freight lines (info)", df["high_freight"].sum(), "flagged high_freight")

    cutoff = df["price"].quantile(a["price_outlier_pct"])
    df["is_price_outlier"] = df["price"] > cutoff
    dq.add("DQ09", "items", f"price above p{a['price_outlier_pct']:.3f} (BRL {cutoff:,.0f})",
           df["is_price_outlier"].sum(), "flagged is_price_outlier")

    carrier = df["order_id"].map(orders.set_index("order_id")["order_delivered_carrier_date"])
    known = carrier.notna() & df["shipping_limit_date"].notna()
    df["late_handover"] = (carrier > df["shipping_limit_date"]).astype("boolean").where(known)
    dq.add("DQ11", "items", "late handover to carrier (info)",
           int(df["late_handover"].fillna(False).sum()), "flagged late_handover")

    return df.rename(columns={"shipping_limit_date": "shipping_limit_ts"})[
        ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_ts",
         "price", "freight_value", "freight_ratio", "high_freight", "is_price_outlier",
         "late_handover"]]


def clean_payments(df: pd.DataFrame, orders: pd.DataFrame, maps, dq: DQLog) -> pd.DataFrame:
    df = strip_text(df)
    df = drop_duplicate_keys(df, ["order_id", "payment_sequential"], "payments", "DQ27", dq)
    df = drop_orphans(df, "order_id", orders["order_id"], "payments", "DQ28", dq)

    dq.add("DQ12", "payments", "payment_type = not_defined",
           df["payment_type"].eq("not_defined").sum(), "label 'Unknown'")
    label = maps["map_payment"].set_index("payment_type")["payment_label"]
    df["payment_label"] = df["payment_type"].map(label).fillna("Unknown")

    zero_inst = df["payment_installments"] < 1
    dq.add("DQ13", "payments", "installments < 1", zero_inst.sum(), "set to 1")
    df.loc[zero_inst, "payment_installments"] = 1

    dq.add("DQ14", "payments", "payment_value <= 0 (info)", (df["payment_value"] <= 0).sum(),
           "kept for reconciliation, excluded from payment mix")
    return df.rename(columns={"payment_installments": "installments"})[
        ["order_id", "payment_sequential", "payment_type", "payment_label",
         "installments", "payment_value"]]


def clean_reviews(df: pd.DataFrame, orders: pd.DataFrame, maps, dq: DQLog) -> pd.DataFrame:
    df = strip_text(df)
    df["review_score"] = pd.to_numeric(df["review_score"], errors="coerce")
    bad = ~df["review_score"].between(1, 5)
    dq.add("DQ20", "reviews", "score missing or outside 1-5", bad.sum(), "dropped")
    df = df.loc[~bad].copy()
    df = drop_orphans(df, "order_id", orders["order_id"], "reviews", "DQ29", dq)

    for col in ["review_creation_date", "review_answer_timestamp"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Keep one review per order: the most recently answered one.
    before = len(df)
    df = (df.sort_values(["order_id", "review_answer_timestamp", "review_creation_date"])
            .drop_duplicates("order_id", keep="last"))
    dq.add("DQ19", "reviews", "more than one review per order", before - len(df),
           "kept latest by review_answer_timestamp")

    band = maps["map_review_band"].set_index("review_score")["review_band"]
    df["review_score"] = df["review_score"].astype(int)
    df["review_band"] = df["review_score"].map(band)
    df["has_comment"] = df["review_comment_message"].notna()
    return df.rename(columns={"review_creation_date": "creation_date",
                              "review_answer_timestamp": "answer_ts"})[
        ["order_id", "review_id", "review_score", "review_band", "has_comment",
         "creation_date", "answer_ts"]]


# ---------------------------------------------------------------------------
# Transform: order-level reconciliation (paid vs billed)
# ---------------------------------------------------------------------------
def add_order_financials(orders: pd.DataFrame, items: pd.DataFrame,
                         payments: pd.DataFrame, a: dict, dq: DQLog) -> pd.DataFrame:
    billed = items.groupby("order_id").agg(
        item_count=("order_item_id", "count"),
        items_value=("price", "sum"),
        freight_value=("freight_value", "sum"),
    )
    paid = payments.groupby("order_id")["payment_value"].sum().rename("paid_value")

    o = orders.join(billed, on="order_id").join(paid, on="order_id")
    has_items, has_pay = o["item_count"].notna(), o["paid_value"].notna()
    dq.add("DQ06", "orders", "orders with no items", (~has_items).sum(), "excluded from revenue")

    o[["item_count", "items_value", "freight_value", "paid_value"]] = \
        o[["item_count", "items_value", "freight_value", "paid_value"]].fillna(0)
    o["item_count"] = o["item_count"].astype(int)
    o["billed_value"] = (o["items_value"] + o["freight_value"]).round(2)
    o["recon_gap"] = (o["paid_value"] - o["billed_value"]).round(2)
    o["recon_exception"] = has_items & has_pay & (o["recon_gap"].abs() > a["recon_tolerance"])
    dq.add("DQ15", "orders", "reconciliation exceptions (info)", o["recon_exception"].sum(),
           "flagged recon_exception")
    return o


# ---------------------------------------------------------------------------
# Transform: RFM
# ---------------------------------------------------------------------------
def build_rfm(orders: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
    """One row per customer_unique_id, delivered orders only."""
    snapshot = orders["order_purchase_timestamp"].max().normalize() + pd.Timedelta(days=1)

    d = orders.loc[orders["status_group"].eq("Delivered"),
                   ["order_id", "customer_id", "order_purchase_timestamp", "paid_value"]]
    d = d.merge(customers[["customer_id", "customer_unique_id", "state", "region"]],
                on="customer_id", how="inner")
    d = d.sort_values("order_purchase_timestamp")

    rfm = d.groupby("customer_unique_id").agg(
        state=("state", "last"),                 # state of the latest order
        region=("region", "last"),
        first_purchase_ts=("order_purchase_timestamp", "min"),
        last_purchase_ts=("order_purchase_timestamp", "max"),
        frequency=("order_id", "nunique"),
        monetary=("paid_value", "sum"),
    ).reset_index()

    rfm["recency_days"] = (snapshot - rfm["last_purchase_ts"].dt.normalize()).dt.days
    rfm["monetary"] = rfm["monetary"].round(2)

    # Quintiles on rank so ties never break the bins (same idea as SQL NTILE(5)).
    rfm["r_score"] = 6 - pd.qcut(rfm["recency_days"].rank(method="first"), 5, labels=False).add(1)
    rfm["m_score"] = pd.qcut(rfm["monetary"].rank(method="first"), 5, labels=False).add(1)
    # Fixed F bins: ~97% of Olist buyers order once, so quintiles are meaningless.
    rfm["f_score"] = np.select(
        [rfm["frequency"] >= 4, rfm["frequency"] == 3, rfm["frequency"] == 2], [5, 4, 3], default=1)

    r, f, m = rfm["r_score"], rfm["f_score"], rfm["m_score"]
    rules = [  # first match wins -- same order as the blueprint
        ("Champions",       (r >= 4) & (f >= 3) & (m >= 4)),
        ("Can't Lose",      (r <= 2) & (f >= 3)),
        ("Loyal",           (f >= 3)),
        ("At Risk",         (r <= 2) & (m >= 4)),
        ("Big One-Timers",  (f == 1) & (m == 5)),
        ("New & Promising", (r >= 4) & (f == 1)),
        ("Hibernating",     (r <= 2) & (m <= 3)),
    ]
    rfm["segment"] = np.select([c for _, c in rules], [n for n, _ in rules], default="Need Attention")
    rfm["rfm_cell"] = r.astype(str) + f.astype(str) + m.astype(str)
    rfm["snapshot_date"] = snapshot.date()

    return rfm[["customer_unique_id", "state", "region", "first_purchase_ts", "last_purchase_ts",
                "recency_days", "frequency", "monetary", "r_score", "f_score", "m_score",
                "rfm_cell", "segment", "snapshot_date"]]


# ---------------------------------------------------------------------------
# Summary report (Excel)
# ---------------------------------------------------------------------------
def kpi_summary(orders, items, payments, reviews, rfm, a: dict) -> pd.DataFrame:
    ok = ~orders["status_group"].eq("Failed")
    delivered = orders["status_group"].eq("Delivered")
    i = items.merge(orders[["order_id", "status_group"]], on="order_id")
    i_ok, i_del, i_fail = i[i["status_group"] != "Failed"], i[i["status_group"] == "Delivered"], \
        i[i["status_group"] == "Failed"]

    gmv = (i_ok["price"] + i_ok["freight_value"]).sum()
    product_rev = i_del["price"].sum()
    timed = orders[delivered & orders["is_late"].notna()]
    rev = reviews.merge(orders[["order_id", "is_late"]], on="order_id")
    p_ok = payments.merge(orders.loc[ok, ["order_id"]], on="order_id")
    p_ok = p_ok[p_ok["payment_value"] > 0]
    with_both = orders[(orders["item_count"] > 0) & (orders["paid_value"] > 0)]
    voucher = p_ok.loc[p_ok["payment_type"].eq("voucher"), "payment_value"].sum()
    lost_gmv = (i_fail["price"] + i_fail["freight_value"]).sum()
    pos_gap = with_both.loc[with_both["recon_exception"] & (with_both["recon_gap"] > 0), "recon_gap"].sum()
    at_risk = rfm.loc[rfm["segment"].isin(["At Risk", "Can't Lose"]), "monetary"].sum()

    rows = [
        ("GMV (BRL)", gmv),
        ("Product Revenue (BRL)", product_rev),
        ("Freight-to-Price Ratio", i_del["freight_value"].sum() / i_del["price"].sum()),
        ("High-Freight Line Share", i_del["high_freight"].mean()),
        ("Fulfilment Failure Rate", orders["status_group"].eq("Failed").mean()),
        ("Lost GMV (BRL)", lost_gmv),
        ("On-Time Delivery Rate", 1 - timed["is_late"].astype(bool).mean()),
        ("Avg Delay Days (late orders)", timed.loc[timed["is_late"].astype(bool), "delay_days"].mean()),
        ("Seller Late-Handover Rate", items["late_handover"].dropna().astype(bool).mean()),
        ("Negative Review Rate", reviews["review_score"].le(2).mean()),
        ("Avg Review Score - on time", rev.loc[rev["is_late"] == False, "review_score"].mean()),  # noqa: E712
        ("Avg Review Score - late", rev.loc[rev["is_late"] == True, "review_score"].mean()),  # noqa: E712
        ("Reconciliation Exception Rate", with_both["recon_exception"].mean()),
        ("Reconciliation Gap, positive (BRL)", pos_gap),
        ("Voucher Dependency", voucher / p_ok["payment_value"].sum()),
        ("Repeat Purchase Rate", rfm["frequency"].ge(2).mean()),
        ("Revenue at Risk (BRL)", at_risk),
        ("Estimated Platform Contribution (BRL, assumed take rate)", product_rev * a["take_rate"]),
        ("Total Leakage (BRL)", lost_gmv + voucher + pos_gap + at_risk),
    ]
    out = pd.DataFrame(rows, columns=["kpi", "value"])
    out["value"] = out["value"].astype(float).round(4)
    return out


def diagnostic_tables(orders, items, products, sellers, customers, rfm, a) -> dict[str, pd.DataFrame]:
    i = (items.merge(orders[["order_id", "customer_id", "status_group"]], on="order_id")
              .merge(customers[["customer_id", "state", "region"]], on="customer_id")
              .merge(products[["product_id", "category_en"]], on="product_id"))
    i = i[i["status_group"] == "Delivered"]

    freight_state = (i.groupby(["region", "state"])
                      .agg(lines=("order_id", "size"), price=("price", "sum"),
                           freight=("freight_value", "sum"), high_freight_lines=("high_freight", "sum"))
                      .assign(freight_ratio=lambda x: (x["freight"] / x["price"]).round(4))
                      .sort_values("freight_ratio", ascending=False).reset_index())

    freight_cat = (i.groupby("category_en")
                    .agg(lines=("order_id", "size"), price=("price", "sum"), freight=("freight_value", "sum"))
                    .assign(freight_ratio=lambda x: (x["freight"] / x["price"]).round(4))
                    .query("lines >= 100").sort_values("freight_ratio", ascending=False).reset_index())

    o = orders.merge(customers[["customer_id", "state"]], on="customer_id")
    o = o[o["is_late"].notna()]
    otd_state = (o.groupby("state")
                  .agg(delivered_orders=("order_id", "size"),
                       late_orders=("is_late", lambda s: s.astype(bool).sum()),
                       avg_delay_late=("delay_days", lambda s: s[s > 0].mean()))
                  .assign(on_time_rate=lambda x: (1 - x["late_orders"] / x["delivered_orders"]).round(4))
                  .sort_values("on_time_rate").reset_index())

    s = items.dropna(subset=["late_handover"])
    seller_risk = (s.groupby("seller_id")
                    .agg(items=("order_id", "size"), late=("late_handover", lambda x: x.astype(bool).sum()),
                         revenue=("price", "sum"))
                    .assign(late_rate=lambda x: (x["late"] / x["items"]).round(4))
                    .query("items >= @a['min_seller_items'] and late_rate > @a['seller_late_flag']")
                    .merge(sellers[["seller_id", "state"]], on="seller_id")
                    .sort_values("revenue", ascending=False).reset_index(drop=True))

    seg = (rfm.groupby("segment")
              .agg(customers=("customer_unique_id", "size"), revenue=("monetary", "sum"),
                   avg_recency=("recency_days", "mean"), avg_frequency=("frequency", "mean"))
              .assign(revenue_share=lambda x: (x["revenue"] / x["revenue"].sum()).round(4))
              .sort_values("revenue", ascending=False).reset_index())

    recon = (orders.loc[orders["recon_exception"],
                        ["order_id", "order_status", "billed_value", "paid_value", "recon_gap"]]
                   .sort_values("recon_gap", key=abs, ascending=False).head(500))

    return {"Freight_by_State": freight_state, "Freight_by_Category": freight_cat,
            "OTD_by_State": otd_state, "Seller_Risk": seller_risk,
            "RFM_Segments": seg, "Recon_Exceptions_Top500": recon}


def export_excel(path: Path, kpis: pd.DataFrame, dq: pd.DataFrame,
                 diag: dict[str, pd.DataFrame], a: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        kpis.to_excel(xw, sheet_name="KPI_Summary", index=False)
        dq.to_excel(xw, sheet_name="DQ_Log", index=False)
        for name, df in diag.items():
            df.to_excel(xw, sheet_name=name[:31], index=False)
        pd.DataFrame(a.items(), columns=["assumption", "value"]).to_excel(
            xw, sheet_name="Assumptions", index=False)
        for ws in xw.book.worksheets:               # readable widths + frozen header
            ws.freeze_panes = "A2"
            for col in ws.columns:
                width = max(len(str(c.value)) if c.value is not None else 0 for c in col[:200])
                ws.column_dimensions[col[0].column_letter].width = min(max(width + 2, 10), 60)
    log.info("wrote %s", path)


# ---------------------------------------------------------------------------
# Load (PostgreSQL)
# ---------------------------------------------------------------------------
def load_to_db(db_url: str, schema_sql: Path, tables: dict[str, pd.DataFrame]) -> None:
    engine = create_engine(db_url)
    with engine.begin() as con:
        con.exec_driver_sql(schema_sql.read_text())          # drop + recreate schema
    log.info("schema created from %s", schema_sql.name)

    load_order = ["assumptions", "customers", "sellers", "products", "orders",
                  "order_items", "payments", "reviews", "customer_rfm"]
    for name in load_order:
        df = tables[name]
        df.to_sql(name, engine, schema="olist", if_exists="append", index=False,
                  method="multi", chunksize=5_000)
        log.info("loaded olist.%-13s %8d rows", name, len(df))

    with engine.connect() as con:                             # row-count check
        for name in load_order:
            n = con.execute(text(f"SELECT COUNT(*) FROM olist.{name}")).scalar_one()
            if n != len(tables[name]):
                raise RuntimeError(f"row count mismatch in olist.{name}: {n} vs {len(tables[name])}")
    log.info("row counts verified")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run(raw_dir: Path, map_dir: Path, out_dir: Path, db_url: str | None) -> dict[str, pd.DataFrame]:
    dq = DQLog()
    raw = load_raw(raw_dir)
    maps = load_mappings(map_dir)
    a = assumption_dict(maps)

    customers = clean_customers(raw["customers"], maps, dq)
    sellers = clean_sellers(raw["sellers"], maps, dq)
    products = clean_products(raw["products"], maps, dq)
    orders = clean_orders(raw["orders"], customers, maps, dq)
    items = clean_items(raw["items"], orders, products, sellers, a, dq)
    payments = clean_payments(raw["payments"], orders, maps, dq)
    reviews = clean_reviews(raw["reviews"], orders, maps, dq)
    orders = add_order_financials(orders, items, payments, a, dq)
    rfm = build_rfm(orders, customers)

    kpis = kpi_summary(orders, items, payments, reviews, rfm, a)
    diag = diagnostic_tables(orders, items, products, sellers, customers, rfm, a)
    export_excel(out_dir / "etl_summary_report.xlsx", kpis, dq.frame(), diag, a)
    dq.frame().to_csv(out_dir / "dq_log.csv", index=False)

    orders_db = orders.rename(columns={
        "order_purchase_timestamp": "purchase_ts", "order_approved_at": "approved_ts",
        "order_delivered_carrier_date": "carrier_ts", "order_delivered_customer_date": "delivered_ts",
        "order_estimated_delivery_date": "estimated_date"})
    orders_db["estimated_date"] = orders_db["estimated_date"].dt.date
    orders_db = orders_db[[
        "order_id", "customer_id", "order_status", "status_group", "purchase_ts", "approved_ts",
        "carrier_ts", "delivered_ts", "estimated_date", "purchase_month", "delay_days",
        "delay_bucket", "is_late", "date_sequence_issue", "status_conflict", "item_count",
        "items_value", "freight_value", "billed_value", "paid_value", "recon_gap", "recon_exception"]]

    tables = {
        "assumptions": maps["assumptions"][["name", "value", "reason"]],
        "customers": customers, "sellers": sellers, "products": products,
        "orders": orders_db, "order_items": items, "payments": payments,
        "reviews": reviews, "customer_rfm": rfm,
    }
    if db_url:
        load_to_db(db_url, PROJECT_ROOT / "sql" / "01_schema.sql", tables)
    else:
        log.info("no database URL - skipped the SQL load")

    print("\nKPI summary\n" + kpis.to_string(index=False))
    return tables


def main() -> None:
    ap = argparse.ArgumentParser(description="Olist profit-leakage ETL")
    ap.add_argument("--raw-dir", type=Path, default=PROJECT_ROOT / "data" / "raw")
    ap.add_argument("--mappings-dir", type=Path, default=PROJECT_ROOT / "data" / "mappings")
    ap.add_argument("--out-dir", type=Path, default=PROJECT_ROOT / "outputs")
    ap.add_argument("--db-url", default=os.getenv("DATABASE_URL"),
                    help="SQLAlchemy URL; defaults to the DATABASE_URL environment variable")
    ap.add_argument("--skip-db", action="store_true", help="write the Excel report only")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S", stream=sys.stdout)
    run(args.raw_dir, args.mappings_dir, args.out_dir, None if args.skip_db else args.db_url)


if __name__ == "__main__":
    main()
