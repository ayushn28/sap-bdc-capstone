"""
etl/extract.py
SAP BDC Capstone — Data Extraction Layer
Simulates extraction from SAP S/4HANA SD Module (VBAK/VBAP tables)
"""

import pandas as pd
import numpy as np
import json
import os
import logging
from datetime import datetime, timedelta
import random

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Seed for reproducibility
random.seed(42)
np.random.seed(42)

REGIONS = ["North", "South", "East", "West", "Central"]
PRODUCTS = {
    "P001": ("Laptop Pro 15", "Electronics", 85000),
    "P002": ("Wireless Headphones", "Electronics", 8500),
    "P003": ("Office Chair Ergonomic", "Furniture", 22000),
    "P004": ("Standing Desk", "Furniture", 35000),
    "P005": ("Smartphone X12", "Electronics", 65000),
    "P006": ("Tablet Air", "Electronics", 45000),
    "P007": ("Mechanical Keyboard", "Electronics", 7500),
    "P008": ("LED Monitor 27inch", "Electronics", 28000),
    "P009": ("Filing Cabinet", "Furniture", 12000),
    "P010": ("Conference Table", "Furniture", 55000),
}
CUSTOMERS = {
    f"C{str(i).zfill(4)}": {
        "name": name,
        "region": random.choice(REGIONS),
        "city": city,
        "segment": seg
    }
    for i, (name, city, seg) in enumerate([
        ("Infosys Ltd", "Bengaluru", "Enterprise"),
        ("TCS Technologies", "Mumbai", "Enterprise"),
        ("Wipro Solutions", "Pune", "Enterprise"),
        ("Reliance Retail", "Mumbai", "Retail"),
        ("HDFC Bank", "Mumbai", "BFSI"),
        ("Tata Motors", "Pune", "Manufacturing"),
        ("Mahindra Group", "Mumbai", "Manufacturing"),
        ("Flipkart Pvt Ltd", "Bengaluru", "E-Commerce"),
        ("Amazon India", "Hyderabad", "E-Commerce"),
        ("Zomato Ltd", "Gurugram", "Food-Tech"),
        ("Ola Cabs", "Bengaluru", "Transport"),
        ("Byju's Learning", "Bengaluru", "EdTech"),
        ("Nykaa Fashion", "Mumbai", "Retail"),
        ("Swiggy India", "Bengaluru", "Food-Tech"),
        ("Paytm Services", "Noida", "FinTech"),
    ], start=1)
}


def extract_sales_orders(start_date: str = "2024-01-01", end_date: str = "2024-12-31") -> pd.DataFrame:
    """
    Simulates extraction from SAP S/4HANA VBAK (Sales Order Header)
    and VBAP (Sales Order Item) tables.
    In production: replaced by JDBC/OData API call to Datasphere connector.
    """
    logger.info(f"Extracting sales orders from {start_date} to {end_date}")

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_range = [start + timedelta(days=i) for i in range((end - start).days + 1)]

    records = []
    order_id = 1000

    for date in date_range:
        # Simulate 3–10 orders per day with slight weekend dip
        n_orders = random.randint(1, 5) if date.weekday() >= 5 else random.randint(4, 12)
        for _ in range(n_orders):
            customer_id = random.choice(list(CUSTOMERS.keys()))
            product_id = random.choice(list(PRODUCTS.keys()))
            product_name, category, base_price = PRODUCTS[product_id]
            qty = random.randint(1, 20)
            discount_pct = random.choice([0, 5, 10, 15, 20])
            unit_price = base_price * (1 - discount_pct / 100)
            net_value = round(unit_price * qty, 2)
            status = random.choices(
                ["Delivered", "In Transit", "Processing", "Cancelled"],
                weights=[65, 15, 15, 5]
            )[0]
            records.append({
                "order_id": f"SO{order_id}",
                "order_date": date.strftime("%Y-%m-%d"),
                "customer_id": customer_id,
                "customer_name": CUSTOMERS[customer_id]["name"],
                "region": CUSTOMERS[customer_id]["region"],
                "city": CUSTOMERS[customer_id]["city"],
                "customer_segment": CUSTOMERS[customer_id]["segment"],
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "quantity": qty,
                "unit_price": round(unit_price, 2),
                "discount_pct": discount_pct,
                "net_value": net_value,
                "currency": "INR",
                "status": status,
                "sales_rep": f"SR{random.randint(1, 10):02d}",
            })
            order_id += 1

    df = pd.DataFrame(records)
    logger.info(f"Extracted {len(df)} sales order records")
    return df


def extract_customer_master() -> pd.DataFrame:
    """Simulates KNA1 (Customer Master) extraction from S/4HANA."""
    logger.info("Extracting customer master data")
    rows = []
    for cust_id, info in CUSTOMERS.items():
        rows.append({
            "customer_id": cust_id,
            "customer_name": info["name"],
            "region": info["region"],
            "city": info["city"],
            "segment": info["segment"],
            "credit_limit": random.choice([500000, 1000000, 2000000, 5000000]),
            "currency": "INR",
            "active": True,
        })
    df = pd.DataFrame(rows)
    logger.info(f"Extracted {len(df)} customer master records")
    return df


def extract_product_master() -> pd.DataFrame:
    """Simulates MARA (Material Master) extraction from S/4HANA."""
    logger.info("Extracting product master data")
    rows = []
    for prod_id, (name, category, price) in PRODUCTS.items():
        rows.append({
            "product_id": prod_id,
            "product_name": name,
            "category": category,
            "base_price": price,
            "currency": "INR",
            "unit_of_measure": "EA",
            "active": True,
        })
    df = pd.DataFrame(rows)
    logger.info(f"Extracted {len(df)} product master records")
    return df


def save_raw(df: pd.DataFrame, name: str, path: str = "./data/raw"):
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, f"{name}.csv")
    df.to_csv(filepath, index=False)
    logger.info(f"Saved raw data: {filepath} ({len(df)} rows)")
    return filepath


if __name__ == "__main__":
    sales_df = extract_sales_orders()
    customer_df = extract_customer_master()
    product_df = extract_product_master()

    save_raw(sales_df, "raw_sales_orders")
    save_raw(customer_df, "raw_customers")
    save_raw(product_df, "raw_products")

    print("\n=== Extraction Summary ===")
    print(f"Sales Orders : {len(sales_df):,} records")
    print(f"Customers    : {len(customer_df):,} records")
    print(f"Products     : {len(product_df):,} records")
