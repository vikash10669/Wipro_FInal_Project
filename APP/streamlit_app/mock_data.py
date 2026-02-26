"""
Mock data generator for Streamlit dashboard
Produces DataFrames that mirror the DW query results so the
dashboard works without a live Snowflake connection.
"""
import random
from datetime import date, timedelta

import numpy as np
import pandas as pd

random.seed(42)
np.random.seed(42)

REGIONS   = ['NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL']
STORES    = [f"Store {i:02d}" for i in range(1, 21)]
STORE_IDS = list(range(1, 21))
STORE_TYPES = ['FLAGSHIP', 'STANDARD', 'OUTLET', 'KIOSK']
CATEGORIES  = ['Electronics', 'Clothing', 'Food & Beverages', 'Home & Garden', 'Sports & Outdoors']
CHANNELS    = ['IN_STORE', 'ONLINE', 'MOBILE']
PAY_METHODS = ['CASH', 'CREDIT_CARD', 'DEBIT_CARD', 'DIGITAL_WALLET', 'GIFT_CARD']
LOYALTY_TIERS = ['BRONZE', 'SILVER', 'GOLD', 'PLATINUM']
AGE_GROUPS    = ['YOUTH', 'ADULT', 'SENIOR']
RETURN_REASONS = ['DEFECTIVE_PRODUCT', 'WRONG_SIZE', 'CHANGED_MIND',
                  'DAMAGED_IN_TRANSIT', 'NOT_AS_DESCRIBED']

BRANDS   = ['NovaBrand', 'PureLife', 'EcoStyle', 'UrbanEdge', 'ClearPath',
            'TechPulse', 'NaturalChoice', 'SwiftLine', 'PeakForm', 'DailyWear']
PRODUCTS = [f"{b} Item {i}" for i, b in enumerate([random.choice(BRANDS) for _ in range(50)], 1)]


def _month_range(start='2023-01-01', end='2024-12-31'):
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    months = []
    d = s.replace(day=1)
    while d <= e:
        months.append(d)
        m = d.month + 1
        y = d.year + (1 if m > 12 else 0)
        d = date(y, m % 12 or 12, 1)
    return months


def get_kpi_summary():
    revenue = round(random.uniform(8_000_000, 12_000_000), 2)
    cogs    = round(revenue * random.uniform(0.45, 0.55), 2)
    profit  = revenue - cogs
    disc    = round(revenue * 0.07, 2)
    return {
        'gross_revenue':       round(revenue + disc, 2),
        'total_discounts':     disc,
        'net_revenue':         revenue,
        'total_cogs':          cogs,
        'gross_profit':        profit,
        'gross_margin_pct':    round(profit / revenue * 100, 2),
        'total_transactions':  random.randint(25_000, 35_000),
        'unique_customers':    random.randint(8_000, 12_000),
        'units_sold':          random.randint(80_000, 120_000),
        'avg_transaction_value': round(revenue / 30_000, 2),
        'total_returns':       random.randint(800, 1_500),
        'return_rate_pct':     round(random.uniform(3, 7), 2),
    }


def get_monthly_trend():
    months = _month_range()
    rows = []
    base = 400_000
    for m in months:
        seasonal = 1 + 0.3 * np.sin((m.month - 3) * np.pi / 6)
        rev   = round(base * seasonal * random.uniform(0.9, 1.1), 2)
        cogs  = round(rev * random.uniform(0.45, 0.55), 2)
        disc  = round(rev * 0.07, 2)
        rows.append({
            'year_month':       m.strftime('%Y-%m'),
            'year_number':      m.year,
            'month_number':     m.month,
            'month_name':       m.strftime('%b'),
            'net_revenue':      rev,
            'gross_profit':     rev - cogs,
            'transactions':     random.randint(1100, 1800),
            'unique_customers': random.randint(400, 800),
            'units_sold':       random.randint(3500, 6000),
            'avg_basket_size':  round(rev / 1400, 2),
        })
    df = pd.DataFrame(rows)
    df['mom_growth_pct'] = df['net_revenue'].pct_change().mul(100).round(2)
    return df


def get_top_products(n=20):
    rows = []
    for i, prod in enumerate(random.sample(PRODUCTS, min(n, len(PRODUCTS))), 1):
        rev  = round(random.uniform(50_000, 500_000), 2)
        cogs = round(rev * random.uniform(0.4, 0.6), 2)
        rows.append({
            'rank':         i,
            'product_name': prod,
            'category_name': random.choice(CATEGORIES),
            'brand':        random.choice(BRANDS),
            'units_sold':   random.randint(200, 5000),
            'net_revenue':  rev,
            'gross_profit': rev - cogs,
            'margin_pct':   round((rev - cogs) / rev * 100, 2),
            'transactions': random.randint(150, 2000),
        })
    df = pd.DataFrame(rows).sort_values('net_revenue', ascending=False).reset_index(drop=True)
    df['rank'] = df.index + 1
    return df


def get_store_performance():
    rows = []
    for i, (sid, sname) in enumerate(zip(STORE_IDS, STORES)):
        rev  = round(random.uniform(200_000, 1_500_000), 2)
        cogs = round(rev * random.uniform(0.45, 0.55), 2)
        rows.append({
            'store_id':     sid,
            'store_name':   sname,
            'store_type':   random.choice(STORE_TYPES),
            'region':       REGIONS[i % len(REGIONS)],
            'city':         f"City {i+1}",
            'state':        random.choice(['CA', 'TX', 'NY', 'FL', 'IL']),
            'net_revenue':  rev,
            'gross_profit': rev - cogs,
            'margin_pct':   round((rev - cogs) / rev * 100, 2),
            'transactions': random.randint(2000, 8000),
            'unique_customers': random.randint(500, 3000),
            'units_sold':   random.randint(5000, 30000),
            'avg_basket_value': round(rev / 5000, 2),
        })
    return pd.DataFrame(rows).sort_values('net_revenue', ascending=False).reset_index(drop=True)


def get_category_performance():
    rows = []
    for cat in CATEGORIES:
        rev  = round(random.uniform(500_000, 3_000_000), 2)
        cogs = round(rev * random.uniform(0.4, 0.6), 2)
        rows.append({
            'category_name': cat,
            'product_count': random.randint(10, 50),
            'units_sold':    random.randint(10_000, 50_000),
            'net_revenue':   rev,
            'gross_profit':  rev - cogs,
            'margin_pct':    round((rev - cogs) / rev * 100, 2),
        })
    df = pd.DataFrame(rows).sort_values('net_revenue', ascending=False)
    total = df['net_revenue'].sum()
    df['pct_of_revenue'] = (df['net_revenue'] / total * 100).round(2)
    return df.reset_index(drop=True)


def get_customer_segments():
    rows = []
    for tier in LOYALTY_TIERS:
        for age in AGE_GROUPS:
            cust  = random.randint(200, 1500)
            rev   = round(random.uniform(50_000, 500_000), 2)
            rows.append({
                'loyalty_tier':      tier,
                'age_group':         age,
                'customer_count':    cust,
                'total_revenue':     rev,
                'avg_revenue_per_customer': round(rev / cust, 2),
                'avg_purchases_per_customer': round(random.uniform(1.5, 8), 2),
                'avg_order_value':   round(random.uniform(40, 200), 2),
                'total_purchases':   random.randint(500, 5000),
            })
    return pd.DataFrame(rows).sort_values('total_revenue', ascending=False).reset_index(drop=True)


def get_top_customers(n=10):
    rows = []
    names = [f"Customer {i:04d}" for i in range(1, n+1)]
    for i, name in enumerate(names, 1):
        ltv = round(random.uniform(5_000, 80_000), 2)
        rows.append({
            'rank':            i,
            'customer_id':     i,
            'full_name':       name,
            'loyalty_tier':    random.choice(LOYALTY_TIERS),
            'region':          random.choice(REGIONS),
            'total_orders':    random.randint(10, 150),
            'total_items':     random.randint(20, 500),
            'lifetime_value':  ltv,
            'avg_order_value': round(ltv / random.randint(10, 150), 2),
        })
    return pd.DataFrame(rows).sort_values('lifetime_value', ascending=False).reset_index(drop=True)


def get_payment_channel_mix():
    rows = []
    for ch in CHANNELS:
        for pm in PAY_METHODS:
            txns = random.randint(100, 3000)
            aov  = round(random.uniform(40, 250), 2)
            rows.append({
                'channel_name':   ch,
                'payment_method': pm,
                'transactions':   txns,
                'net_revenue':    round(txns * aov, 2),
                'avg_order_value': aov,
            })
    df = pd.DataFrame(rows)
    total = df['net_revenue'].sum()
    df['revenue_share_pct'] = (df['net_revenue'] / total * 100).round(2)
    return df.sort_values('net_revenue', ascending=False).reset_index(drop=True)


def get_regional_quarterly():
    rows = []
    for region in REGIONS:
        for year in [2023, 2024]:
            for q in [1, 2, 3, 4]:
                rev  = round(random.uniform(300_000, 1_200_000), 2)
                rows.append({
                    'region':       region,
                    'year_number':  year,
                    'quarter_name': f'Q{q}',
                    'net_revenue':  rev,
                    'gross_profit': round(rev * random.uniform(0.3, 0.5), 2),
                    'transactions': random.randint(2000, 8000),
                    'revenue_per_store': round(rev / 4, 2),
                })
    return pd.DataFrame(rows)


def get_inventory_health():
    rows = []
    statuses = ['HEALTHY', 'HEALTHY', 'HEALTHY', 'REORDER_NEEDED', 'SLOW_MOVING', 'OUT_OF_STOCK']
    for store in random.sample(STORES, 8):
        for prod in random.sample(PRODUCTS, 6):
            on_hand = random.randint(0, 300)
            reorder_pt = random.randint(10, 40)
            avail = max(0, on_hand - random.randint(0, 20))
            rows.append({
                'store_name':        store,
                'region':            random.choice(REGIONS),
                'product_name':      prod,
                'category_name':     random.choice(CATEGORIES),
                'quantity_on_hand':  on_hand,
                'quantity_available': avail,
                'reorder_point':     reorder_pt,
                'inventory_value_cost':   round(on_hand * random.uniform(10, 150), 2),
                'inventory_value_retail': round(on_hand * random.uniform(20, 250), 2),
                'days_since_last_sale':   random.randint(0, 120),
                'days_since_restock':     random.randint(0, 60),
                'inventory_status': random.choice(statuses),
                'below_reorder_flag': avail <= reorder_pt,
            })
    return pd.DataFrame(rows)


def get_return_analysis():
    rows = []
    months = _month_range()
    for m in months:
        for reason in RETURN_REASONS:
            rows.append({
                'year_month':     m.strftime('%Y-%m'),
                'return_reason':  reason,
                'return_count':   random.randint(5, 80),
                'total_refunds':  round(random.uniform(1000, 15000), 2),
                'return_rate_pct': round(random.uniform(1, 8), 2),
            })
    return pd.DataFrame(rows)


def get_yoy_comparison():
    rows = []
    for year in [2022, 2023, 2024]:
        rev = round(random.uniform(7_000_000 + year * 500_000, 9_000_000 + year * 600_000), 2)
        profit = round(rev * random.uniform(0.35, 0.45), 2)
        rows.append({
            'year_number':  year,
            'net_revenue':  rev,
            'gross_profit': profit,
            'transactions': random.randint(25_000, 40_000),
            'customers':    random.randint(8_000, 15_000),
            'margin_pct':   round(profit / rev * 100, 2),
        })
    df = pd.DataFrame(rows)
    df['yoy_growth_pct'] = df['net_revenue'].pct_change().mul(100).round(2)
    return df
