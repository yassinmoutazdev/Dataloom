#!/usr/bin/env python3
"""
Shared pytest fixtures for the Dataloom v3.0 test suite.

Provides session-scoped schema and join-path fixtures that are used across
all sprint test files. Join paths are re-applied before each test via an
autouse fixture to prevent state leakage between files.
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from validator import set_join_paths


@pytest.fixture(scope="session")
def sample_schema():
    """Return the canonical five-table schema used across unit test files."""
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
    """Return join conditions for the canonical five-table schema."""
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
    """Apply join paths before every test and clear them on teardown.

    Declared autouse so every test in the suite gets a clean, consistent
    join-path state regardless of import order or cross-file pollution.
    """
    set_join_paths(sample_join_paths)
    yield
    set_join_paths({})
