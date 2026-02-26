"""
Retail Chain Data Generator
Generates realistic CSV data for all OLTP tables
"""
import csv
import os
import random
from datetime import date, datetime, timedelta

from faker import Faker

fake = Faker('en_US')
random.seed(42)
Faker.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'csv')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Constants ────────────────────────────────────────────────
REGIONS       = ['NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL']
STORE_TYPES   = ['FLAGSHIP', 'STANDARD', 'OUTLET', 'KIOSK']
LOYALTY_TIERS = ['BRONZE', 'SILVER', 'GOLD', 'PLATINUM']
GENDERS       = ['MALE', 'FEMALE', 'NON_BINARY', 'PREFER_NOT_TO_SAY']
CHANNELS      = ['IN_STORE', 'ONLINE', 'MOBILE']
PAY_METHODS   = ['CASH', 'CREDIT_CARD', 'DEBIT_CARD', 'DIGITAL_WALLET', 'GIFT_CARD']
RETURN_REASONS = [
    'DEFECTIVE_PRODUCT', 'WRONG_SIZE', 'CHANGED_MIND',
    'DAMAGED_IN_TRANSIT', 'NOT_AS_DESCRIBED', 'DUPLICATE_ORDER'
]
START_DATE = date(2023, 1, 1)
END_DATE   = date(2024, 12, 31)

BRANDS = ['NovaBrand', 'PureLife', 'EcoStyle', 'UrbanEdge', 'ClearPath',
          'TechPulse', 'NaturalChoice', 'SwiftLine', 'PeakForm', 'DailyWear']
COLORS = ['Red', 'Blue', 'Green', 'Black', 'White', 'Yellow', 'Gray', 'Brown', 'Pink', 'Purple']
SIZES  = ['XS', 'S', 'M', 'L', 'XL', 'XXL', 'ONE_SIZE', '28', '30', '32', '34']

CATEGORIES = [
    (1, 'ELECT', 'Electronics',         None),
    (2, 'CLOTH', 'Clothing',            None),
    (3, 'FOOD',  'Food & Beverages',    None),
    (4, 'HOME',  'Home & Garden',       None),
    (5, 'SPORT', 'Sports & Outdoors',   None),
    (6, 'PHONE', 'Smartphones',         1),
    (7, 'LAPT',  'Laptops',             1),
    (8, 'MENS',  "Men's Clothing",      2),
    (9, 'WOMN',  "Women's Clothing",    2),
    (10,'GROC',  'Groceries',           3),
    (11,'BVGS',  'Beverages',           3),
    (12,'FURN',  'Furniture',           4),
    (13,'FITT',  'Fitness Equipment',   5),
]

def rand_date(start=START_DATE, end=END_DATE):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def fmt_dt(d):
    if isinstance(d, datetime):
        return d.strftime('%Y-%m-%d %H:%M:%S')
    return d.strftime('%Y-%m-%d %H:%M:%S') if hasattr(d, 'hour') else str(d)

def fmt_bool(b):
    return 'TRUE' if b else 'FALSE'

# ── Locations ────────────────────────────────────────────────
def gen_locations(n=50):
    rows = []
    states = ['CA', 'TX', 'NY', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI',
              'WA', 'AZ', 'MA', 'TN', 'IN', 'MO', 'MD', 'WI', 'CO', 'MN']
    state_region = {
        'CA':'WEST','WA':'WEST','AZ':'WEST','CO':'WEST',
        'TX':'SOUTH','FL':'SOUTH','GA':'SOUTH','NC':'SOUTH','TN':'SOUTH',
        'NY':'EAST','PA':'EAST','MA':'EAST','MD':'EAST',
        'IL':'CENTRAL','OH':'CENTRAL','IN':'CENTRAL','MO':'CENTRAL','WI':'CENTRAL','MN':'CENTRAL','MI':'CENTRAL',
    }
    for i in range(1, n + 1):
        state = random.choice(states)
        ts = fake.date_time_between(start_date='-5y', end_date='-3y')
        rows.append({
            'location_id': i,
            'street_address': fake.street_address(),
            'city': fake.city(),
            'state': state,
            'zip_code': fake.zipcode(),
            'country': 'USA',
            'region': state_region.get(state, 'CENTRAL'),
            'created_at': fmt_dt(ts),
            'updated_at': fmt_dt(ts),
        })
    return rows

# ── Stores ───────────────────────────────────────────────────
def gen_stores(locations, n=20):
    rows = []
    for i in range(1, n + 1):
        loc = random.choice(locations)
        open_d = rand_date(date(2018, 1, 1), date(2022, 12, 31))
        ts = datetime.combine(open_d, datetime.min.time())
        rows.append({
            'store_id': i,
            'store_code': f'STR{i:04d}',
            'store_name': f"{fake.city()} {random.choice(['Mall', 'Plaza', 'Centre', 'Square', 'Market'])}",
            'store_type': random.choice(STORE_TYPES),
            'location_id': loc['location_id'],
            'manager_name': fake.name(),
            'phone_number': fake.phone_number()[:20],
            'email': fake.company_email(),
            'open_date': str(open_d),
            'close_date': '',
            'is_active': fmt_bool(True),
            'square_footage': random.randint(2000, 50000),
            'created_at': fmt_dt(ts),
            'updated_at': fmt_dt(ts),
        })
    return rows

# ── Customers ────────────────────────────────────────────────
def gen_customers(locations, n=500):
    rows = []
    for i in range(1, n + 1):
        loc = random.choice(locations)
        dob = fake.date_of_birth(minimum_age=18, maximum_age=80)
        reg = rand_date(date(2020, 1, 1), date(2024, 6, 30))
        points = random.randint(0, 50000)
        tier = ('PLATINUM' if points > 30000 else
                'GOLD'     if points > 15000 else
                'SILVER'   if points > 5000  else 'BRONZE')
        ts = datetime.combine(reg, datetime.min.time())
        rows.append({
            'customer_id': i,
            'customer_code': f'CUST{i:06d}',
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.unique.email(),
            'phone_number': fake.phone_number()[:20],
            'date_of_birth': str(dob),
            'gender': random.choice(GENDERS),
            'loyalty_tier': tier,
            'loyalty_points': points,
            'registration_date': str(reg),
            'location_id': loc['location_id'],
            'is_active': fmt_bool(random.random() > 0.05),
            'created_at': fmt_dt(ts),
            'updated_at': fmt_dt(ts),
        })
    return rows

# ── Product Categories ───────────────────────────────────────
def gen_categories():
    rows = []
    ts = '2020-01-01 00:00:00'
    for cat_id, code, name, parent in CATEGORIES:
        rows.append({
            'category_id': cat_id,
            'category_code': code,
            'category_name': name,
            'parent_category_id': parent if parent else '',
            'description': f'{name} category',
            'is_active': 'TRUE',
            'created_at': ts,
        })
    return rows

# ── Products ─────────────────────────────────────────────────
def gen_products(n=200):
    rows = []
    leaf_cats = [c for c in CATEGORIES if c[3] is not None]
    for i in range(1, n + 1):
        cat = random.choice(leaf_cats)
        cost  = round(random.uniform(5, 300), 2)
        price = round(cost * random.uniform(1.2, 2.5), 2)
        launch = rand_date(date(2019, 1, 1), date(2023, 12, 31))
        ts = datetime.combine(launch, datetime.min.time())
        rows.append({
            'product_id': i,
            'product_code': f'PRD{i:05d}',
            'sku': f'SKU-{fake.bothify("??##-????##")}',
            'product_name': f"{random.choice(BRANDS)} {fake.word().title()} {cat[2]}",
            'category_id': cat[0],
            'supplier_id': random.randint(1, 20),
            'unit_cost': cost,
            'unit_price': price,
            'discount_pct': round(random.choice([0, 0, 0, 5, 10, 15, 20]), 2),
            'weight_kg': round(random.uniform(0.1, 20), 3),
            'brand': random.choice(BRANDS),
            'size': random.choice(SIZES),
            'color': random.choice(COLORS),
            'is_perishable': fmt_bool(cat[0] in [10, 11]),
            'is_active': fmt_bool(random.random() > 0.08),
            'launch_date': str(launch),
            'discontinue_date': '',
            'created_at': fmt_dt(ts),
            'updated_at': fmt_dt(ts),
        })
    return rows

# ── Sales Transactions ───────────────────────────────────────
def gen_sales(stores, customers, products, n=3000):
    transactions, lines, payments = [], [], []
    line_id = 1
    payment_id = 1

    for txn_id in range(1, n + 1):
        store    = random.choice(stores)
        customer = random.choice(customers) if random.random() > 0.1 else None
        txn_date = datetime.combine(
            rand_date(START_DATE, END_DATE),
            datetime.min.time()
        ) + timedelta(hours=random.randint(8, 21), minutes=random.randint(0, 59))

        num_lines   = random.randint(1, 6)
        txn_products = random.sample(products, min(num_lines, len(products)))
        subtotal = discount_total = tax_total = 0.0
        txn_lines = []

        for ln, prod in enumerate(txn_products, start=1):
            qty       = random.randint(1, 5)
            up        = float(prod['unit_price'])
            uc        = float(prod['unit_cost'])
            disc_pct  = float(prod['discount_pct'])
            disc_amt  = round(up * qty * disc_pct / 100, 2)
            line_tot  = round(up * qty - disc_amt, 2)
            line_cost = round(uc * qty, 2)
            tax_rate  = 0.08
            tax_amt   = round(line_tot * tax_rate, 2)

            subtotal      += round(up * qty, 2)
            discount_total += disc_amt
            tax_total      += tax_amt

            txn_lines.append({
                'line_id': line_id,
                'transaction_id': txn_id,
                'line_number': ln,
                'product_id': prod['product_id'],
                'quantity': qty,
                'unit_price': up,
                'unit_cost': uc,
                'discount_pct': disc_pct,
                'discount_amount': disc_amt,
                'line_total_amount': line_tot,
                'line_cost_amount': line_cost,
                'tax_rate': round(tax_rate * 100, 2),
                'tax_amount': tax_amt,
                'created_at': fmt_dt(txn_date),
            })
            line_id += 1

        total = round(subtotal - discount_total + tax_total, 2)
        points_earned = int(total * 1)

        transactions.append({
            'transaction_id': txn_id,
            'transaction_code': f'TXN{txn_id:08d}',
            'transaction_date': fmt_dt(txn_date),
            'store_id': store['store_id'],
            'customer_id': customer['customer_id'] if customer else '',
            'cashier_id': random.randint(1, 50),
            'transaction_type': 'SALE',
            'channel': random.choice(CHANNELS),
            'subtotal_amount': round(subtotal, 2),
            'discount_amount': round(discount_total, 2),
            'tax_amount': round(tax_total, 2),
            'total_amount': total,
            'loyalty_points_earned': points_earned,
            'loyalty_points_redeemed': 0,
            'notes': '',
            'created_at': fmt_dt(txn_date),
        })
        lines.extend(txn_lines)

        # Payment
        method = random.choice(PAY_METHODS)
        payments.append({
            'payment_id': payment_id,
            'transaction_id': txn_id,
            'payment_method': method,
            'payment_amount': total,
            'payment_status': 'COMPLETED',
            'payment_reference': fake.uuid4()[:20],
            'payment_date': fmt_dt(txn_date),
            'card_last_four': str(random.randint(1000, 9999)) if method in ('CREDIT_CARD', 'DEBIT_CARD') else '',
            'created_at': fmt_dt(txn_date),
        })
        payment_id += 1

    return transactions, lines, payments

# ── Returns ──────────────────────────────────────────────────
def gen_returns(transactions, stores, customers, pct=0.05):
    rows = []
    for i, txn in enumerate(random.sample(transactions, int(len(transactions) * pct)), start=1):
        ret_date = datetime.strptime(txn['transaction_date'], '%Y-%m-%d %H:%M:%S') + timedelta(days=random.randint(1, 30))
        rows.append({
            'return_id': i,
            'return_code': f'RET{i:07d}',
            'original_transaction_id': txn['transaction_id'],
            'return_date': fmt_dt(ret_date),
            'store_id': txn['store_id'],
            'customer_id': txn['customer_id'],
            'return_reason': random.choice(RETURN_REASONS),
            'refund_method': random.choice(['ORIGINAL_PAYMENT', 'STORE_CREDIT', 'CASH']),
            'refund_amount': round(float(txn['total_amount']) * random.uniform(0.1, 1.0), 2),
            'is_restocked': fmt_bool(random.random() > 0.3),
            'created_at': fmt_dt(ret_date),
        })
    return rows

# ── Inventory ────────────────────────────────────────────────
def gen_inventory(stores, products):
    rows = []
    inv_id = 1
    for prod in products:
        sampled_stores = random.sample(stores, min(len(stores), random.randint(5, len(stores))))
        for store in sampled_stores:
            on_hand = random.randint(0, 500)
            reserved = random.randint(0, min(50, on_hand))
            available = on_hand - reserved
            reorder_pt = random.randint(5, 30)
            last_restock = rand_date(date(2024, 1, 1), date(2024, 12, 31))
            last_sold    = rand_date(last_restock, date(2024, 12, 31))
            ts = datetime.now()
            rows.append({
                'inventory_id': inv_id,
                'store_id': store['store_id'],
                'product_id': prod['product_id'],
                'quantity_on_hand': on_hand,
                'quantity_reserved': reserved,
                'quantity_available': available,
                'reorder_point': reorder_pt,
                'reorder_quantity': random.randint(50, 200),
                'last_restock_date': str(last_restock),
                'last_sold_date': str(last_sold),
                'snapshot_date': str(date.today()),
                'created_at': fmt_dt(ts),
                'updated_at': fmt_dt(ts),
            })
            inv_id += 1
    return rows

# ── CSV Writer ───────────────────────────────────────────────
def write_csv(filename, rows):
    if not rows:
        return
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Written {len(rows):>6,} rows → {path}")

# ── Main ─────────────────────────────────────────────────────
def main():
    print("Generating retail chain data...")

    locations   = gen_locations(50)
    stores      = gen_stores(locations, 20)
    customers   = gen_customers(locations, 500)
    categories  = gen_categories()
    products    = gen_products(200)
    txns, lines, payments = gen_sales(stores, customers, products, 3000)
    returns     = gen_returns(txns, stores, customers)
    inventory   = gen_inventory(stores, products)

    write_csv('location.csv',           locations)
    write_csv('store.csv',              stores)
    write_csv('customer.csv',           customers)
    write_csv('product_category.csv',   categories)
    write_csv('product.csv',            products)
    write_csv('sales_transaction.csv',  txns)
    write_csv('sales_line.csv',         lines)
    write_csv('payment.csv',            payments)
    write_csv('return_transaction.csv', returns)
    write_csv('inventory.csv',          inventory)

    print(f"\nDone. Files in: {os.path.abspath(OUTPUT_DIR)}")

if __name__ == '__main__':
    main()
