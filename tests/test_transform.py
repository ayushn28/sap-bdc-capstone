"""
tests/test_transform.py
Unit tests for the BDC ETL transformation logic
"""

import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../etl"))
from transform import transform_sales_orders, transform_customers, transform_products, compute_kpis


# ─── Fixtures ──────────────────────────────────────────────────

@pytest.fixture
def sample_sales():
    return pd.DataFrame([
        {
            "order_id": "SO1001", "order_date": "2024-03-15",
            "customer_id": "C0001", "customer_name": "Infosys Ltd",
            "region": "South", "city": "Bengaluru", "customer_segment": "Enterprise",
            "product_id": "P001", "product_name": "Laptop Pro 15", "category": "Electronics",
            "quantity": 5, "unit_price": 85000, "discount_pct": 10,
            "net_value": 382500, "currency": "INR", "status": "Delivered", "sales_rep": "SR01"
        },
        {
            "order_id": "SO1002", "order_date": "2024-03-16",
            "customer_id": "C0002", "customer_name": "TCS Technologies",
            "region": "West", "city": "Mumbai", "customer_segment": "Enterprise",
            "product_id": "P002", "product_name": "Wireless Headphones", "category": "Electronics",
            "quantity": 20, "unit_price": 8500, "discount_pct": 0,
            "net_value": 170000, "currency": "INR", "status": "Cancelled", "sales_rep": "SR02"
        },
        {
            "order_id": "SO1003", "order_date": "2024-06-01",
            "customer_id": "C0001", "customer_name": "Infosys Ltd",
            "region": "South", "city": "Bengaluru", "customer_segment": "Enterprise",
            "product_id": "P005", "product_name": "Smartphone X12", "category": "Electronics",
            "quantity": 3, "unit_price": 65000, "discount_pct": 5,
            "net_value": 185250, "currency": "INR", "status": "In Transit", "sales_rep": "SR01"
        },
    ])


@pytest.fixture
def sample_customers():
    return pd.DataFrame([
        {"customer_id": "C0001", "customer_name": "Infosys Ltd", "region": "South",
         "city": "Bengaluru", "segment": "Enterprise", "credit_limit": 5000000,
         "currency": "INR", "active": True},
        {"customer_id": "C0002", "customer_name": "TCS Technologies", "region": "West",
         "city": "Mumbai", "segment": "Enterprise", "credit_limit": 2000000,
         "currency": "INR", "active": True},
    ])


@pytest.fixture
def sample_products():
    return pd.DataFrame([
        {"product_id": "P001", "product_name": "Laptop Pro 15", "category": "Electronics",
         "base_price": 85000, "currency": "INR", "unit_of_measure": "EA", "active": True},
        {"product_id": "P002", "product_name": "Wireless Headphones", "category": "Electronics",
         "base_price": 8500, "currency": "INR", "unit_of_measure": "EA", "active": True},
    ])


# ─── Sales Order Tests ─────────────────────────────────────────

class TestTransformSalesOrders:

    def test_output_has_required_columns(self, sample_sales):
        result = transform_sales_orders(sample_sales)
        required = ["order_id", "year", "month", "quarter", "revenue", "revenue_eligible", "fact_key"]
        for col in required:
            assert col in result.columns, f"Missing column: {col}"

    def test_date_fields_derived(self, sample_sales):
        result = transform_sales_orders(sample_sales)
        assert result["year"].iloc[0] == 2024
        assert result["month"].iloc[0] == 3
        assert result["quarter"].iloc[0] == 1

    def test_cancelled_orders_have_zero_revenue(self, sample_sales):
        result = transform_sales_orders(sample_sales)
        cancelled = result[result["status"] == "Cancelled"]
        assert (cancelled["revenue"] == 0).all(), "Cancelled orders must have 0 revenue"

    def test_delivered_orders_have_positive_revenue(self, sample_sales):
        result = transform_sales_orders(sample_sales)
        delivered = result[result["status"] == "Delivered"]
        assert (delivered["revenue"] > 0).all()

    def test_no_duplicate_orders(self, sample_sales):
        # Add a duplicate
        dup = sample_sales.copy()
        dup = pd.concat([dup, dup.iloc[[0]]], ignore_index=True)
        result = transform_sales_orders(dup)
        assert result["order_id"].duplicated().sum() == 0

    def test_fact_key_is_unique(self, sample_sales):
        result = transform_sales_orders(sample_sales)
        assert result["fact_key"].nunique() == len(result)

    def test_load_timestamp_exists(self, sample_sales):
        result = transform_sales_orders(sample_sales)
        assert result["load_timestamp"].notna().all()


# ─── Customer Tests ─────────────────────────────────────────────

class TestTransformCustomers:

    def test_has_surrogate_key(self, sample_customers):
        result = transform_customers(sample_customers)
        assert "dim_customer_key" in result.columns

    def test_credit_limit_is_float(self, sample_customers):
        result = transform_customers(sample_customers)
        assert result["credit_limit"].dtype == float

    def test_no_duplicates(self, sample_customers):
        dup = pd.concat([sample_customers, sample_customers.iloc[[0]]], ignore_index=True)
        result = transform_customers(dup)
        assert result["customer_id"].duplicated().sum() == 0


# ─── KPI Tests ─────────────────────────────────────────────────

class TestComputeKPIs:

    def test_kpi_keys_present(self, sample_sales):
        transformed = transform_sales_orders(sample_sales)
        kpis = compute_kpis(transformed)
        required_keys = [
            "total_revenue", "total_orders", "unique_customers",
            "avg_order_value", "revenue_by_region", "top_customers"
        ]
        for key in required_keys:
            assert key in kpis, f"Missing KPI: {key}"

    def test_total_revenue_excludes_cancelled(self, sample_sales):
        transformed = transform_sales_orders(sample_sales)
        kpis = compute_kpis(transformed)
        # Only SO1001 (382500) and SO1003 (185250) are eligible
        assert kpis["total_revenue"] == pytest.approx(382500 + 185250, rel=0.01)

    def test_fulfillment_rate_in_range(self, sample_sales):
        transformed = transform_sales_orders(sample_sales)
        kpis = compute_kpis(transformed)
        assert 0 <= kpis["order_fulfillment_rate"] <= 100
