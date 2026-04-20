"""
etl/transform.py
SAP BDC Capstone — Data Transformation Layer
Applies BDC/Datasphere-compatible cleansing and enrichment logic
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def transform_sales_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich sales order data for BDC fact table."""
    logger.info("Transforming sales orders...")
    original_count = len(df)

    # 1. Drop duplicates
    df = df.drop_duplicates(subset=["order_id"])

    # 2. Handle nulls
    df["discount_pct"] = df["discount_pct"].fillna(0)
    df["net_value"] = df["net_value"].fillna(0)
    df = df.dropna(subset=["order_id", "customer_id", "product_id", "order_date"])

    # 3. Type casting
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = df["unit_price"].astype(float)
    df["net_value"] = df["net_value"].astype(float)

    # 4. Derived date fields (Datasphere date dimension)
    df["year"] = df["order_date"].dt.year
    df["month"] = df["order_date"].dt.month
    df["month_name"] = df["order_date"].dt.strftime("%B")
    df["quarter"] = df["order_date"].dt.quarter
    df["quarter_label"] = "Q" + df["quarter"].astype(str)
    df["week"] = df["order_date"].dt.isocalendar().week.astype(int)
    df["day_of_week"] = df["order_date"].dt.day_name()
    df["is_weekend"] = df["order_date"].dt.dayofweek >= 5

    # 5. Revenue fields
    df["gross_value"] = df["quantity"] * df["unit_price"] / (1 - df["discount_pct"] / 100 + 1e-9)
    df["discount_amount"] = (df["gross_value"] - df["net_value"]).round(2)
    df["gross_value"] = df["gross_value"].round(2)

    # 6. Order status classification
    status_map = {
        "Delivered": "Closed",
        "In Transit": "Open",
        "Processing": "Open",
        "Cancelled": "Cancelled"
    }
    df["status_group"] = df["status"].map(status_map)

    # 7. Exclude cancelled orders from revenue (BDC business rule)
    df["revenue_eligible"] = df["status"] != "Cancelled"
    df["revenue"] = df.apply(
        lambda r: r["net_value"] if r["revenue_eligible"] else 0.0, axis=1
    )

    # 8. Surrogate key (BDC fact table primary key)
    df["fact_key"] = range(1, len(df) + 1)

    # 9. Load timestamp
    df["load_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"Transformed sales orders: {original_count} → {len(df)} records")
    return df


def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean customer dimension for BDC."""
    logger.info("Transforming customer master...")
    df = df.drop_duplicates(subset=["customer_id"])
    df["active"] = df["active"].astype(bool)
    df["credit_limit"] = df["credit_limit"].astype(float)
    df["dim_customer_key"] = range(1, len(df) + 1)
    df["load_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Transformed {len(df)} customer records")
    return df


def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    """Clean product dimension for BDC."""
    logger.info("Transforming product master...")
    df = df.drop_duplicates(subset=["product_id"])
    df["base_price"] = df["base_price"].astype(float)
    df["active"] = df["active"].astype(bool)
    df["dim_product_key"] = range(1, len(df) + 1)
    df["load_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Transformed {len(df)} product records")
    return df


def compute_kpis(sales_df: pd.DataFrame) -> dict:
    """
    Compute key performance indicators.
    Replicates what Datasphere analytical views produce.
    """
    logger.info("Computing KPIs...")
    eligible = sales_df[sales_df["revenue_eligible"]]

    kpis = {
        "total_revenue": round(eligible["revenue"].sum(), 2),
        "total_orders": len(sales_df),
        "delivered_orders": len(sales_df[sales_df["status"] == "Delivered"]),
        "cancelled_orders": len(sales_df[sales_df["status"] == "Cancelled"]),
        "unique_customers": sales_df["customer_id"].nunique(),
        "avg_order_value": round(eligible["revenue"].mean(), 2),
        "revenue_by_region": eligible.groupby("region")["revenue"].sum().round(2).to_dict(),
        "revenue_by_category": eligible.groupby("category")["revenue"].sum().round(2).to_dict(),
        "revenue_by_month": eligible.groupby("month_name")["revenue"].sum().round(2).to_dict(),
        "revenue_by_quarter": eligible.groupby("quarter_label")["revenue"].sum().round(2).to_dict(),
        "top_customers": (
            eligible.groupby("customer_name")["revenue"]
            .sum().nlargest(5).round(2).to_dict()
        ),
        "top_products": (
            eligible.groupby("product_name")["revenue"]
            .sum().nlargest(5).round(2).to_dict()
        ),
        "order_fulfillment_rate": round(
            len(sales_df[sales_df["status"] == "Delivered"]) / len(sales_df) * 100, 2
        ),
    }

    logger.info(f"KPIs computed. Total Revenue: ₹{kpis['total_revenue']:,.2f}")
    return kpis


if __name__ == "__main__":
    import os
    raw_path = "./data/raw"
    sales_raw = pd.read_csv(os.path.join(raw_path, "raw_sales_orders.csv"))
    customers_raw = pd.read_csv(os.path.join(raw_path, "raw_customers.csv"))
    products_raw = pd.read_csv(os.path.join(raw_path, "raw_products.csv"))

    sales_clean = transform_sales_orders(sales_raw)
    customers_clean = transform_customers(customers_raw)
    products_clean = transform_products(products_raw)

    kpis = compute_kpis(sales_clean)
    print("\n=== KPI Summary ===")
    for k, v in kpis.items():
        if not isinstance(v, dict):
            print(f"  {k}: {v}")
