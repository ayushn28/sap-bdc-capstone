"""
etl/pipeline.py
SAP BDC Capstone — Main ETL Pipeline Orchestrator
Run this script to execute the full Extract → Transform → Load cycle.
"""

import time
import logging
from extract import extract_sales_orders, extract_customer_master, extract_product_master, save_raw
from transform import transform_sales_orders, transform_customers, transform_products, compute_kpis
from load import load_to_datasphere, load_kpis, load_pipeline_log

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run_pipeline():
    logger.info("=" * 60)
    logger.info("  SAP BDC CAPSTONE — ETL Pipeline Starting")
    logger.info("  Project: Sales Revenue Analytics Platform")
    logger.info("=" * 60)
    start_time = time.time()

    # ─── EXTRACT ───────────────────────────────────────────────
    logger.info("\n[PHASE 1] EXTRACT — Reading from SAP S/4HANA sources")
    sales_raw = extract_sales_orders(start_date="2024-01-01", end_date="2024-12-31")
    customers_raw = extract_customer_master()
    products_raw = extract_product_master()

    save_raw(sales_raw, "raw_sales_orders")
    save_raw(customers_raw, "raw_customers")
    save_raw(products_raw, "raw_products")

    # ─── TRANSFORM ─────────────────────────────────────────────
    logger.info("\n[PHASE 2] TRANSFORM — Cleansing & enriching data")
    sales_clean = transform_sales_orders(sales_raw)
    customers_clean = transform_customers(customers_raw)
    products_clean = transform_products(products_raw)
    kpis = compute_kpis(sales_clean)

    # ─── LOAD ──────────────────────────────────────────────────
    logger.info("\n[PHASE 3] LOAD — Writing to BDC (Datasphere)")
    load_to_datasphere(sales_clean, "fact_sales")
    load_to_datasphere(customers_clean, "dim_customer")
    load_to_datasphere(products_clean, "dim_product")
    load_kpis(kpis)

    elapsed = round(time.time() - start_time, 2)

    stats = {
        "pipeline": "SAP BDC Sales Analytics ETL",
        "duration_seconds": elapsed,
        "records_extracted": {
            "sales_orders": len(sales_raw),
            "customers": len(customers_raw),
            "products": len(products_raw),
        },
        "records_loaded": {
            "fact_sales": len(sales_clean),
            "dim_customer": len(customers_clean),
            "dim_product": len(products_clean),
        },
        "kpis": {
            "total_revenue": kpis["total_revenue"],
            "total_orders": kpis["total_orders"],
            "fulfillment_rate": kpis["order_fulfillment_rate"],
        }
    }
    load_pipeline_log(stats)

    logger.info("\n" + "=" * 60)
    logger.info("  PIPELINE COMPLETED SUCCESSFULLY")
    logger.info(f"  Duration       : {elapsed}s")
    logger.info(f"  Sales Records  : {len(sales_clean):,}")
    logger.info(f"  Total Revenue  : ₹{kpis['total_revenue']:,.2f}")
    logger.info(f"  Fulfillment    : {kpis['order_fulfillment_rate']}%")
    logger.info("=" * 60)
    return stats


if __name__ == "__main__":
    run_pipeline()
