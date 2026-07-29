"""
generate_fixtures.py
--------------------
Generates a deterministic, small-scale SQL fixture file for CI/CD testing.

Produces tests/fixtures/retail_db_test.sql with:
  - 600 rows in transactions
  - 200 rows in inventory
  - 200 rows in supplier_orders

Every dirty pattern is guaranteed to appear at least once.

Usage:
    python generate_fixtures.py
    python generate_fixtures.py --output path/to/custom_fixture.sql
"""

import random
import argparse
import os
from datetime import datetime, timedelta
import numpy as np

# ── config ────────────────────────────────────────────────────────────────────
N_TRANSACTIONS    = 600
N_INVENTORY       = 200
N_SUPPLIER_ORDERS = 200

RANDOM_SEED = 99

# numpy rng — seeded inside main() for full determinism
_rng = np.random.default_rng(RANDOM_SEED)

# ── realistic distribution samplers ──────────────────────────────────────────
def sample_quantity_sold():   return min(max(int(_rng.geometric(p=0.45)),1),20)
def sample_unit_price():      return round(min(max(float(_rng.lognormal(2.0,0.9)),0.50),49.99),2)
def sample_retail_price():    return round(min(max(float(_rng.lognormal(2.0,0.9)),0.50),49.99),2)
def sample_units_on_hand():   return min(max(int(_rng.lognormal(3.5,1.2)),0),500)
def sample_reorder_point():   return min(max(int(_rng.lognormal(3.0,0.7)),5),200)
def sample_days_on_shelf():   return min(max(int(_rng.exponential(25)),1),180)
def sample_out_of_stock_days():
    if _rng.random()<0.40: return 0
    return min(max(int(_rng.geometric(p=0.30)),1),30)
def sample_qty_ordered():     return min(max(int(_rng.lognormal(4.5,0.8)),50),1000)
def sample_fill_rate():
    if _rng.random()<0.02: return round(float(_rng.uniform(0,30)),2)
    return round(min(max(float(_rng.beta(8,1.5))*100,0),100),2)
def sample_discount_rate():   return round(float(_rng.beta(1.5,5))*0.25,4)
def sample_shrinkage():
    if _rng.random()>0.15: return 0.0
    return round(min(float(_rng.exponential(0.4)),3.50),2)

# ── product catalog ───────────────────────────────────────────────────────────
CATALOG = {
    "Beverages":     ["Coca-Cola 20oz","Pepsi 20oz","Gatorade Fruit Punch 32oz","Red Bull 8.4oz","Dasani Water 20oz"],
    "Snacks":        ["Doritos Nacho Cheese 9.25oz","Ruffles Cheddar Sour Cream 8.5oz","Snickers Bar 1.86oz","Oreo Cookies 14.3oz","Jack Link's Beef Jerky 3.25oz"],
    "Dairy":         ["Chobani Plain Greek Yogurt 32oz","Kraft Singles American 16 slices","Lucerne Eggs Large 12ct","Philadelphia Cream Cheese 8oz","Horizon Organic Whole Milk 1gal"],
    "Frozen":        ["DiGiorno Rising Crust Pepperoni Pizza 27.5oz","Ore-Ida Golden Crinkles 32oz","Ben & Jerry's Chocolate Chip Cookie Dough 16oz","Eggo Homestyle Waffles 12.3oz","Stouffer's Mac & Cheese 12oz"],
    "Produce":       ["Bananas 1lb","Strawberries 1lb Clamshell","Baby Spinach 5oz Bag","Avocado Each","Russet Potatoes 5lb Bag"],
    "Bakery":        ["Wonder Classic White Bread 20oz","Pillsbury Grands Homestyle Biscuits 8ct","Hostess Twinkies 10ct","Dave's Killer Bread 21 Whole Grains 27oz","Entenmann's Classic Glazed Donuts 8ct"],
    "Meat":          ["Oscar Mayer Classic Beef Franks 10ct","Hormel Pepperoni 6oz","Spam Classic 12oz","Hillshire Farm Smoked Sausage 14oz","StarKist Chunk Light Tuna 5oz"],
    "Household":     ["Bounty Select-A-Size Paper Towels 6 rolls","Tide Original Liquid Detergent 64oz","Dawn Ultra Original Dish Soap 21.6oz","Glad ForceFlex 13gal Trash Bags 40ct","Lysol Disinfecting Spray 19oz"],
    "Personal Care": ["Tylenol Extra Strength 100ct","Colgate Total Whitening Toothpaste 4.8oz","Dove Men+Care Body Wash 13.5oz","Gillette Fusion5 Razor 1ct","Advil Liqui-Gels 80ct"],
    "Tobacco":       ["Marlboro Red Box 20ct","Newport Menthol Box 20ct","Zyn Cool Mint Nicotine Pouches 15ct"],
    "Lottery":       ["Powerball $2 Ticket","Mega Millions $2 Ticket","$5 Scratch-Off Ticket"],
    "Electronics":   ["Duracell AA Batteries 8ct","Energizer AAA Batteries 8ct","SanDisk 128GB USB Flash Drive","Apple Lightning Cable 3ft","Anker PowerCore 10000mAh Power Bank"],
}

SKU_CATALOG = {}
sku_counter = 1000
for cat, products in CATALOG.items():
    for prod in products:
        SKU_CATALOG[f"SKU-{sku_counter}"] = (prod, cat)
        sku_counter += 1
SKU_IDS = list(SKU_CATALOG.keys())

SUPPLIERS = ["FreshFarm Co","National Beverage Dist","Metro Foods LLC",
             "QuickShip Wholesale","Sunrise Grocery Supply",
             "Pacific Coast Foods","Heartland Distribution"]
STORES = [f"{d} {n}" for d in ["North","South","East","West"]
          for n in ["Main St","Plaza","Corner"]]

# ── dirty pattern pools ───────────────────────────────────────────────────────
SUPPLIER_VARIANTS = {
    "FreshFarm Co":           ["FreshFarm Co","Freshfarm co","FRESHFARM CO","freshfarm co"],
    "National Beverage Dist": ["National Beverage Dist","national beverage dist","NATIONAL BEVERAGE DIST"],
    "Metro Foods LLC":        ["Metro Foods LLC","Metro foods LLC","metro foods llc","METRO FOODS LLC"],
    "QuickShip Wholesale":    ["QuickShip Wholesale","quickship wholesale","QUICKSHIP WHOLESALE"],
    "Sunrise Grocery Supply": ["Sunrise Grocery Supply","sunrise grocery supply","SUNRISE GROCERY SUPPLY"],
    "Pacific Coast Foods":    ["Pacific Coast Foods","pacific coast foods","PACIFIC COAST FOODS"],
    "Heartland Distribution": ["Heartland Distribution","heartland distribution","HEARTLAND DISTRIBUTION"],
}
CATEGORY_TYPOS = {
    "Beverages":    ["Beveragse","BEVERAGES","beverages"],
    "Snacks":       ["Snaks","SNACKS","snacks"],
    "Dairy":        ["DAIRY","dairy","Dairry"],
    "Frozen":       ["FROZEN","frozen","Frozn"],
    "Produce":      ["PRODUCE","produce","Prodce"],
    "Bakery":       ["BAKERY","bakery","Bakrey"],
    "Meat":         ["MEAT","meat","Mea"],
    "Household":    ["HOUSEHOLD","household","Houshold"],
    "Personal Care":["PERSONAL CARE","personal care","Pesonal Care"],
    "Tobacco":      ["TOBACCO","tobacco","Tobaco"],
    "Lottery":      ["LOTTERY","lottery","Lotery"],
    "Electronics":  ["ELECTRONICS","electronics","Electroics"],
}
STORE_TYPOS = {
    "North": ["north","NORTH","Nrth"],
    "South": ["south","SOUTH","Soth"],
    "East":  ["east","EAST","Eas"],
    "West":  ["west","WEST","Wst"],
}
PAYMENT_VARIANTS = {
    "Credit Card": {"weight": 0.42, "dirty": ["Credit Card","credit card","Credit card","CREDIT CARD"]},
    "Debit Card":  {"weight": 0.32, "dirty": ["Debit Card","debit card","Debit card"]},
    "Cash":        {"weight": 0.17, "dirty": ["Cash","cash","CASH"]},
    "Mobile Pay":  {"weight": 0.07, "dirty": ["Mobile Pay","mobile pay","Mobile pay"]},
    "Gift Card":   {"weight": 0.02, "dirty": ["Gift Card","gift card","Gift card"]},
}
PAYMENT_METHODS = list(PAYMENT_VARIANTS.keys())
PAYMENT_WEIGHTS = [PAYMENT_VARIANTS[m]["weight"] for m in PAYMENT_METHODS]

def sample_payment():
    method = random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1)[0]
    return random.choice(PAYMENT_VARIANTS[method]["dirty"])

RETURN_VARIANTS = ["Y","N","Yes","No","1","0","y","n"]
BOOL_VARIANTS   = ["Y","N","Yes","No","1","0","TRUE","FALSE","y","n"]
DATE_SENTINELS  = ["N/A","TBD","??",""]

CATEGORIES_ALWAYS_EXPIRY = {"Beverages","Snacks","Dairy","Frozen","Produce","Bakery","Meat"}
PRODUCTS_WITH_EXPIRY = {
    "Tylenol Extra Strength 100ct","Advil Liqui-Gels 80ct",
    "Colgate Total Whitening Toothpaste 4.8oz","Dove Men+Care Body Wash 13.5oz",
    "Duracell AA Batteries 8ct","Energizer AAA Batteries 8ct",
}
def has_expiry(prod, cat):
    return cat in CATEGORIES_ALWAYS_EXPIRY or prod in PRODUCTS_WITH_EXPIRY

# ── date constants ────────────────────────────────────────────────────────────
T_START = datetime(2021, 1, 1)
T_END   = datetime(2024, 12, 31)
I_START = datetime(2023, 1, 1)
PENDING_WINDOW_DAYS = 45
PENDING_CUTOFF      = T_END - timedelta(days=PENDING_WINDOW_DAYS)
GHOST_ORDER_RATE    = 0.005

# ── helpers ───────────────────────────────────────────────────────────────────
def rand_dt(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def dirty_store(store):
    parts = store.split(" ")
    direction = parts[0]
    suffix    = " ".join(parts[1:])
    dirty_dir = random.choice(STORE_TYPOS.get(direction, [direction]))
    return f"{dirty_dir} {suffix}"

def fmt_ts(dt):
    h,m,s = random.randint(0,23), random.randint(0,59), random.randint(0,59)
    return dt.replace(hour=h, minute=m, second=s).strftime("%Y-%m-%d %H:%M:%S")

def fmt_date(dt):
    return dt.strftime("%Y-%m-%d")

def sql_str(v):
    if v is None: return "NULL"
    return "'" + str(v).replace("'","''") + "'"

def sql_num(v):
    if v is None: return "NULL"
    return str(v)

CUST_POOL = [f"CUST-{i}" for i in range(1000, 2000)]

# NOTE: lookups built inside main() after seeding
SKU_STORE_SUPPLIER = {}
SKU_STORE_REORDER  = {}
SKU_UNIT_COST      = {}
SKU_UNIT_PRICE     = {}

# ── CyclingPool ───────────────────────────────────────────────────────────────
class CyclingPool:
    def __init__(self, items):
        self.items = items
        self.idx   = 0
    def next(self):
        val = self.items[self.idx % len(self.items)]
        self.idx += 1
        return val


# ── transactions generator ────────────────────────────────────────────────────
def generate_transactions(n):
    rows = []
    date_sentinel_pool = CyclingPool(DATE_SENTINELS)
    all_payment_dirty  = [v for m in PAYMENT_METHODS for v in PAYMENT_VARIANTS[m]["dirty"]]
    payment_pool       = CyclingPool(all_payment_dirty)
    return_pool        = CyclingPool(RETURN_VARIANTS)
    cat_typo_pool      = CyclingPool([t for typos in CATEGORY_TYPOS.values() for t in typos])

    guaranteed = []
    guaranteed.append(("cust_null",))
    guaranteed.append(("cust_lower",))
    guaranteed.append(("cust_no_hyphen",))
    guaranteed.append(("cust_na",))
    for s in DATE_SENTINELS:
        guaranteed.append(("date_sentinel", s))
    guaranteed.append(("qty_negative",))
    guaranteed.append(("price_negative",))
    for method in PAYMENT_METHODS:
        guaranteed.append(("payment", PAYMENT_VARIANTS[method]["dirty"][0]))
    for rv in RETURN_VARIANTS:
        guaranteed.append(("return", rv))
    for d, variants in STORE_TYPOS.items():
        for v in variants:
            for s in STORES:
                if d in s:
                    guaranteed.append(("store_typo", s.replace(d, v)))
                    break
    for cat, typos in CATEGORY_TYPOS.items():
        for typo in typos:
            guaranteed.append(("cat_typo", cat, typo))

    for i in range(n):
        sku = random.choice(SKU_IDS)
        prod, cat = SKU_CATALOG[sku]
        store = random.choice(STORES)
        base_qty   = sample_quantity_sold()
        base_price = SKU_UNIT_PRICE[sku]
        disc       = sample_discount_rate() if _rng.random() < 0.3 else 0.0
        shrink     = sample_shrinkage()
        cid        = random.choice(CUST_POOL)
        ts         = fmt_ts(rand_dt(T_START, T_END))
        store_val  = store
        cat_val    = cat
        qty        = base_qty
        price      = base_price
        payment    = sample_payment()
        ret        = random.choice(RETURN_VARIANTS)

        if i < len(guaranteed):
            g   = guaranteed[i]
            tag = g[0]
            if tag == "cust_null":        cid = None
            elif tag == "cust_lower":     cid = random.choice(CUST_POOL).lower()
            elif tag == "cust_no_hyphen": cid = random.choice(CUST_POOL).replace("-","")
            elif tag == "cust_na":        cid = "N/A"
            elif tag == "date_sentinel":  ts  = g[1]
            elif tag == "qty_negative":   qty = -int(_rng.integers(1,6))
            elif tag == "price_negative": price = -abs(price)
            elif tag == "payment":        payment = g[1]
            elif tag == "return":         ret = g[1]
            elif tag == "store_typo":     store_val = g[1]
            elif tag == "cat_typo":       cat_val = g[2]
        else:
            r = random.random()
            if r < 0.28:   cid = None
            elif r < 0.30: cid = cid.lower()
            elif r < 0.31: cid = cid.replace("-","")
            elif r < 0.32: cid = "N/A"
            if random.random() < 0.05: ts        = random.choice(DATE_SENTINELS)
            if random.random() < 0.10: store_val = dirty_store(store)
            if random.random() < 0.12: cat_val   = cat_typo_pool.next()
            if random.random() < 0.04: qty       = -int(_rng.integers(1,6))
            if random.random() < 0.04: price     = round(-price, 2)

        # returns: negate price when qty is negative, no discount
        if qty < 0:
            price = -abs(price)
            disc  = 0.0
        # no discount on refunds
        if qty < 0 or price < 0:
            disc = 0.0
        total = round(abs(qty) * abs(price) * (1 - disc), 2)
        rows.append((
            f"TXN-{100000+i}", cid, ts if ts else None,
            store_val, cat_val, sku, prod, qty, price, disc, total,
            payment, f"EMP-{random.randint(100,999)}", shrink, ret,
        ))
    return rows


# ── inventory generator ───────────────────────────────────────────────────────
def generate_inventory(n):
    rows = []
    sup_variant_pool = CyclingPool([v for variants in SUPPLIER_VARIANTS.values() for v in variants])
    bool_pool        = CyclingPool(BOOL_VARIANTS)
    date_sent_pool   = CyclingPool(DATE_SENTINELS)
    cat_typo_pool    = CyclingPool([t for typos in CATEGORY_TYPOS.values() for t in typos])

    for i in range(n):
        sku = random.choice(SKU_IDS)
        prod, cat = SKU_CATALOG[sku]
        store = random.choice(STORES)
        canonical_sup = SKU_STORE_SUPPLIER[(sku, store)]

        stock      = sample_units_on_hand()
        days       = sample_days_on_shelf()
        restock_ts = fmt_ts(rand_dt(I_START, T_END))
        store_val  = store
        cat_val    = cat
        sup_val    = random.choice(SUPPLIER_VARIANTS[canonical_sup])
        markdown   = random.choice(BOOL_VARIANTS)

        if i % 5  == 0: sup_val    = sup_variant_pool.next()
        if i % 8  == 1: markdown   = bool_pool.next()
        if i % 10 == 2: stock      = -int(_rng.integers(1,11))
        if i % 16 == 3: days       = -1
        if i % 12 == 4: restock_ts = date_sent_pool.next()
        if i % 7  == 5: store_val  = dirty_store(store)
        if i % 9  == 6: cat_val    = cat_typo_pool.next()

        expiry = fmt_date(rand_dt(datetime(2024,6,1), datetime(2026,6,1))) \
                 if has_expiry(prod, cat) else None

        rows.append((
            sku, prod, cat_val, store_val, sup_val,
            stock, SKU_STORE_REORDER[(sku, store)], days,
            restock_ts if restock_ts else None,
            SKU_UNIT_COST[sku], SKU_UNIT_PRICE[sku],
            sample_out_of_stock_days(), expiry, markdown,
        ))
    return rows


# ── supplier_orders generator ─────────────────────────────────────────────────
def generate_supplier_orders(n):
    rows = []
    sup_variant_pool = CyclingPool([v for variants in SUPPLIER_VARIANTS.values() for v in variants])
    bool_pool        = CyclingPool(BOOL_VARIANTS)
    date_sent_pool   = CyclingPool(DATE_SENTINELS)
    cat_typo_pool    = CyclingPool([t for typos in CATEGORY_TYPOS.values() for t in typos])

    for i in range(n):
        sku = random.choice(SKU_IDS)
        prod, cat = SKU_CATALOG[sku]
        store = random.choice(STORES)
        canonical_sup = SKU_STORE_SUPPLIER[(sku, store)]

        qty_ord      = sample_qty_ordered()
        fill         = sample_fill_rate()
        qty_recv     = min(max(int(round(qty_ord * fill / 100)), 0), qty_ord)
        qty_recv_val = str(qty_recv)
        unit_cost    = SKU_UNIT_COST[sku]
        order_dt     = rand_dt(T_START, T_END)
        expected_dt  = order_dt + timedelta(days=random.randint(1, 14))
        order_ts     = fmt_ts(order_dt)
        expected_ts  = fmt_date(expected_dt)
        sup_val      = random.choice(SUPPLIER_VARIANTS[canonical_sup])
        store_val    = store
        cat_val      = cat
        invoice_flag = random.choice(BOOL_VARIANTS)

        # actual_delivery_date: proximity-based NULL logic
        if order_dt >= PENDING_CUTOFF:
            actual_ts = None                          # not yet delivered
        elif _rng.random() < GHOST_ORDER_RATE:
            actual_ts = None                          # ghost order
        else:
            actual_dt = expected_dt + timedelta(days=random.randint(-2, 7))
            actual_ts = fmt_ts(actual_dt)

        if i % 5  == 0: sup_val      = sup_variant_pool.next()
        if i % 8  == 1: invoice_flag = bool_pool.next()
        if i % 12 == 2: order_ts     = date_sent_pool.next()
        if i % 14 == 3: expected_ts  = date_sent_pool.next()
        # "pending" only valid when delivery hasn't happened yet
        if i % 6  == 4 and actual_ts is None: qty_recv_val = "pending"
        if i % 11 == 5: qty_recv_val = f"{qty_recv} units"
        if i % 7  == 6: store_val    = dirty_store(store)
        if i % 9  == 7: cat_val      = cat_typo_pool.next()

        rows.append((
            f"PO-{50000+i}", order_ts if order_ts else None,
            sup_val, cat_val, sku, prod, store_val,
            qty_ord, qty_recv_val, unit_cost,
            round(qty_ord * unit_cost, 2),
            fmt_date(expected_dt), actual_ts, fill, invoice_flag,
        ))
    return rows


# ── SQL writer ────────────────────────────────────────────────────────────────
def write_sql(path, t_rows, i_rows, o_rows):
    BATCH = 100
    with open(path, "w") as f:
        f.write("-- ============================================================\n")
        f.write("-- retail_db_test.sql — CI/CD fixture\n")
        f.write("-- Auto-generated by generate_fixtures.py. Do not edit manually.\n")
        f.write(f"-- transactions: {len(t_rows):,}  inventory: {len(i_rows):,}  supplier_orders: {len(o_rows):,}\n")
        f.write("-- ============================================================\n\n")
        f.write("BEGIN;\n\n")

        f.write("DROP TABLE IF EXISTS transactions;\n")
        f.write("""\
CREATE TABLE transactions (
    transaction_id   VARCHAR(20)  NOT NULL,
    customer_id      VARCHAR(20),
    transaction_date VARCHAR(30),
    store_id         VARCHAR(50)  NOT NULL,
    category         VARCHAR(50),
    product_sku      VARCHAR(20),
    product_name     VARCHAR(100),
    quantity_sold    INTEGER,
    unit_price_usd   NUMERIC(10, 2),
    discount_rate    NUMERIC(5, 4),
    total_sales_usd  NUMERIC(10, 2),
    payment_method   VARCHAR(30),
    cashier_id       VARCHAR(20),
    shrinkage_usd    NUMERIC(10, 2),
    return_flag      VARCHAR(5)
);\n\n""")
        for start in range(0, len(t_rows), BATCH):
            batch = t_rows[start:start+BATCH]
            vals  = ",\n    ".join("({})".format(", ".join([
                sql_str(r[0]),sql_str(r[1]),sql_str(r[2]),sql_str(r[3]),sql_str(r[4]),
                sql_str(r[5]),sql_str(r[6]),sql_num(r[7]),sql_num(r[8]),sql_num(r[9]),
                sql_num(r[10]),sql_str(r[11]),sql_str(r[12]),sql_num(r[13]),sql_str(r[14]),
            ])) for r in batch)
            f.write(f"INSERT INTO transactions VALUES\n    {vals};\n")
        f.write("\n")

        f.write("DROP TABLE IF EXISTS inventory;\n")
        f.write("""\
CREATE TABLE inventory (
    sku                   VARCHAR(20),
    product_name          VARCHAR(100),
    category              VARCHAR(50),
    store_location        VARCHAR(50),
    supplier_name         VARCHAR(100),
    units_on_hand         INTEGER,
    reorder_point         INTEGER,
    days_on_shelf         INTEGER,
    last_restock_date     VARCHAR(30),
    unit_cost_usd         NUMERIC(10, 2),
    unit_retail_price_usd NUMERIC(10, 2),
    out_of_stock_days_30d INTEGER,
    expiry_date           VARCHAR(10),
    markdown_flag         VARCHAR(10)
);\n\n""")
        for start in range(0, len(i_rows), BATCH):
            batch = i_rows[start:start+BATCH]
            vals  = ",\n    ".join("({})".format(", ".join([
                sql_str(r[0]),sql_str(r[1]),sql_str(r[2]),sql_str(r[3]),sql_str(r[4]),
                sql_num(r[5]),sql_num(r[6]),sql_num(r[7]),sql_str(r[8]),
                sql_num(r[9]),sql_num(r[10]),sql_num(r[11]),sql_str(r[12]),sql_str(r[13]),
            ])) for r in batch)
            f.write(f"INSERT INTO inventory VALUES\n    {vals};\n")
        f.write("\n")

        f.write("DROP TABLE IF EXISTS supplier_orders;\n")
        f.write("""\
CREATE TABLE supplier_orders (
    order_id               VARCHAR(20)  NOT NULL,
    order_date             VARCHAR(30),
    supplier_name          VARCHAR(100),
    category               VARCHAR(50),
    sku                    VARCHAR(20),
    product_name           VARCHAR(100),
    store_location         VARCHAR(50),
    qty_ordered            INTEGER,
    qty_received           VARCHAR(20),
    unit_cost_usd          NUMERIC(10, 2),
    total_order_value_usd  NUMERIC(10, 2),
    expected_delivery_date VARCHAR(10),
    actual_delivery_date   VARCHAR(30),
    fill_rate_pct          NUMERIC(5, 2),
    invoice_matched_flag   VARCHAR(10)
);\n\n""")
        for start in range(0, len(o_rows), BATCH):
            batch = o_rows[start:start+BATCH]
            vals  = ",\n    ".join("({})".format(", ".join([
                sql_str(r[0]),sql_str(r[1]),sql_str(r[2]),sql_str(r[3]),
                sql_str(r[4]),sql_str(r[5]),sql_str(r[6]),
                sql_num(r[7]),sql_str(r[8]),sql_num(r[9]),sql_num(r[10]),
                sql_str(r[11]),sql_str(r[12]),sql_num(r[13]),sql_str(r[14]),
            ])) for r in batch)
            f.write(f"INSERT INTO supplier_orders VALUES\n    {vals};\n")
        f.write("\n")

        f.write("COMMIT;\n")


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="tests/fixtures/retail_db_test.sql")
    args = parser.parse_args()

    random.seed(RANDOM_SEED)
    global _rng
    _rng = np.random.default_rng(RANDOM_SEED)

    _reorder_rng = np.random.default_rng(RANDOM_SEED + 1)
    _cost_rng    = np.random.default_rng(RANDOM_SEED + 2)
    _price_rng   = np.random.default_rng(RANDOM_SEED + 3)

    global SKU_STORE_SUPPLIER, SKU_STORE_REORDER, SKU_UNIT_COST, SKU_UNIT_PRICE
    SKU_STORE_SUPPLIER = {(sku,store): random.choice(SUPPLIERS) for sku in SKU_IDS for store in STORES}
    SKU_STORE_REORDER  = {(sku,store): min(max(int(_reorder_rng.lognormal(3.0,0.7)),5),200)
                          for sku in SKU_IDS for store in STORES}
    SKU_UNIT_COST      = {sku: round(min(max(float(_cost_rng.lognormal(1.5,0.9)),0.20),30.00),2)
                          for sku in SKU_IDS}
    SKU_UNIT_PRICE     = {sku: round(min(max(SKU_UNIT_COST[sku] * round(float(_price_rng.uniform(1.2,2.5)),2), 0.50), 49.99), 2)
                          for sku in SKU_IDS}

    print(f"Generating fixtures (seed={RANDOM_SEED})...")
    t_rows = generate_transactions(N_TRANSACTIONS)
    i_rows = generate_inventory(N_INVENTORY)
    o_rows = generate_supplier_orders(N_SUPPLIER_ORDERS)

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    write_sql(args.output, t_rows, i_rows, o_rows)

    print(f"  transactions:    {len(t_rows):,}")
    print(f"  inventory:       {len(i_rows):,}")
    print(f"  supplier_orders: {len(o_rows):,}")
    print(f"  Written to: {args.output}")


if __name__ == "__main__":
    main()
