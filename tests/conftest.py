#!/usr/bin/env python3
"""
Shared test fixtures for the Dataloom test suite.
"""
import pytest
import sys
import os

# Add the project root to the path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from validator import set_join_paths


@pytest.fixture(scope="session")
def sample_schema():
    """Sample schema for testing."""
    return {
        "fact_orders": [
            "order_id", "customer_id", "product_id", "employee_id",
            "order_date", "ship_date", "quantity", "unit_price", 
            "freight", "status", "region"
        ],
        "dim_customers": [
            "customer_id", "name", "email", "city", "country", "age"
        ],
        "dim_products": [
            "product_id", "product_name", "category", "price"
        ],
        "dim_employees": [
            "employee_id", "name", "department", "region"
        ]
    }


@pytest.fixture(scope="session")
def sample_join_paths():
    """Sample join paths for testing."""
    return {
        "fact_orders": {
            "dim_customers": "fact_orders.customer_id = dim_customers.customer_id",
            "dim_products": "fact_orders.product_id = dim_products.product_id",
            "dim_employees": "fact_orders.employee_id = dim_employees.employee_id",
        },
        "dim_customers": {
            "fact_orders": "fact_orders.customer_id = dim_customers.customer_id"
        },
        "dim_products": {
            "fact_orders": "fact_orders.product_id = dim_products.product_id"
        },
        "dim_employees": {
            "fact_orders": "fact_orders.employee_id = dim_employees.employee_id"
        }
    }


@pytest.fixture(autouse=True)
def setup_join_paths(sample_join_paths):
    """Automatically set up join paths for each test."""
    set_join_paths(sample_join_paths)
    yield
    # Cleanup if needed