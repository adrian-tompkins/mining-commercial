import random
from datetime import timedelta
import glob

import numpy as np
import pandas as pd
from faker import Faker
from utils import load_pdf_to_volume, save_to_parquet

# Set environment variables for Databricks Volumes
import os
os.environ['CATALOG'] = 'demo_generator'
os.environ['SCHEMA'] = 'adrian_tompkins_mining_commercial'
os.environ['VOLUME'] = 'raw_data'



"""Mega Minerals - mining_commercial
Raw data generation for all datasources defined in demo_story.

Key story patterns encoded numerically:
- Date range 2025-08-01 .. 2025-11-30
- MM62 port inventory at Pilbara Port ~0.8-0.9Mt baseline, drops by ~0.4Mt
  between 2025-10-18 and 2025-10-26 due to MM62 ship_load > rail_in
- Three MM62 vessels (Dragon Steel, Nippon Metals, EuroSteel) in that window
  have poor coverage and high demurrage exposure
- Market indices (62FE_CFR, freight, FX) spike in mid-October
- Maintenance logs show SL-2 unplanned outage ~72h on 2025-10-18
- Asset telemetry shows elevated risk metrics for SL-2 and CV-01 around event
- Shipment revenue aligns with vessel schedule & contracts, allowing
  downstream gold tables to quantify demurrage, inventory value, and
  revenue-at-risk
"""

# Reproducibility
SEED = 42
np.random.seed(SEED)
random.seed(SEED)
Faker.seed(SEED)
fake = Faker()

# Global date range
RANGE_START = pd.Timestamp("2025-08-01")
RANGE_END = pd.Timestamp("2025-11-30")
ALL_DATES = pd.date_range(RANGE_START, RANGE_END, freq="D")

OUTAGE_START = pd.Timestamp("2025-10-18")
OUTAGE_END = pd.Timestamp("2025-10-31")
INVENTORY_DIP_START = pd.Timestamp("2025-10-20")
INVENTORY_DIP_END = pd.Timestamp("2025-10-26")

# Helper enums
MINES = ["Mine A", "Mine B", "Mine C"]
MINE_MM62_SHARE = np.array([0.5, 0.3, 0.2])
PRODUCTS = ["MM62", "MM58", "MM65", "Other"]
PRODUCT_SHARES = np.array([0.6, 0.25, 0.1, 0.05])
PORT_SITES = ["Pilbara Port", "Secondary Port"]
PORT_SHARES = np.array([0.85, 0.15])
CUSTOMERS = ["Dragon Steel", "Nippon Metals", "EuroSteel", "Other Asia", "Other Europe"]

ASSETS = ["CV-01", "SL-1", "SL-2", "SR-3", "CV-02", "SL-3", "Other"]
ASSET_TYPES = {
    "CV-01": "conveyor",
    "CV-02": "conveyor",
    "SL-1": "ship_loader",
    "SL-2": "ship_loader",
    "SL-3": "ship_loader",
    "SR-3": "stacker_reclaimer",
    "Other": "other",
}
ASSET_SITE = {
    "CV-01": "Pilbara Port",
    "CV-02": "Pilbara Port",
    "SL-1": "Pilbara Port",
    "SL-2": "Pilbara Port",
    "SL-3": "Secondary Port",
    "SR-3": "Pilbara Port",
    "Other": "Secondary Port",
}


def _normalize(probs: np.ndarray) -> np.ndarray:
    p = np.array(probs, dtype=float)
    s = p.sum()
    if s <= 0:
        p = np.ones_like(p, dtype=float)
        s = p.sum()
    return p / s


def _choose(values, probs, size):
    p = _normalize(np.array(probs, dtype=float))
    return np.random.choice(values, size=int(size), p=p)


# ============================================================
# raw_mine_production
# ============================================================

def generate_raw_mine_production() -> pd.DataFrame:
    print("Generating raw_mine_production ...")
    rows = []
    batch_per_combo = 10  # keep row count in tens of thousands

    for d in ALL_DATES:
        # baseline total production 180-220 kt/day
        base_total = np.random.normal(200_000, 15_000)
        # small weekday/weekend variation
        if d.weekday() >= 5:
            base_total *= 0.92

        # distribute across products
        prod_shares = np.random.dirichlet(PRODUCT_SHARES * 20)
        prod_tonnes = base_total * prod_shares

        for prod, prod_t in zip(PRODUCTS, prod_tonnes):
            # per mine share (MM62 skewed to Mine A)
            if prod == "MM62":
                mine_shares = MINE_MM62_SHARE
            else:
                mine_shares = np.array([1 / 3, 1 / 3, 1 / 3])
            mine_shares = _normalize(mine_shares)
            mine_tonnes = prod_t * mine_shares

            for mine, mine_tot in zip(MINES, mine_tonnes):
                # split into batches
                for b in range(batch_per_combo):
                    noise = np.random.uniform(0.7, 1.3)
                    tonnes = (mine_tot / batch_per_combo) * noise
                    if tonnes <= 0:
                        continue
                    rows.append(
                        {
                            "production_id": f"PROD-{d.strftime('%Y%m%d')}-{mine.replace(' ', '')}-{prod}-{b:02d}",
                            "production_date": d.date(),
                            "mine_site": mine,
                            "product_code": prod,
                            "tonnes_produced": float(max(tonnes, 100.0)),
                        }
                    )
    df = pd.DataFrame(rows)
    print(f"  rows: {len(df):,}")
    return df


# ============================================================
# raw_rail_movements
# ============================================================

def generate_raw_rail_movements(mine_df: pd.DataFrame) -> pd.DataFrame:
    print("Generating raw_rail_movements ...")
    rows = []
    train_seq = 1

    # approximate 15-25 trains per day
    for d in ALL_DATES:
        n_trains = np.random.randint(15, 26)
        for i in range(n_trains):
            dep_hour = np.random.randint(0, 24)
            dep_minute = np.random.randint(0, 60)
            dep_time = d + pd.Timedelta(hours=int(dep_hour), minutes=int(dep_minute))
            travel_hours = np.random.uniform(8, 14)
            arr_time = dep_time + pd.Timedelta(hours=float(travel_hours))

            origin_mine = _choose(MINES, [0.5, 0.3, 0.2], 1)[0]
            port_site = _choose(PORT_SITES, PORT_SHARES, 1)[0]
            product_code = _choose(PRODUCTS, PRODUCT_SHARES, 1)[0]

            tonnes = np.random.normal(8_000, 900)
            tonnes = float(np.clip(tonnes, 6_000, 10_000))

            rows.append(
                {
                    "rail_id": f"TRAIN-{d.strftime('%Y%m%d')}-{train_seq:03d}",
                    "departure_time": dep_time,
                    "arrival_time": arr_time,
                    "origin_mine": origin_mine,
                    "port_site": port_site,
                    "product_code": product_code,
                    "tonnes_rail": tonnes,
                }
            )
            train_seq += 1

    df = pd.DataFrame(rows)
    print(f"  rows: {len(df):,}")
    return df


# ============================================================
# raw_port_stockpile_events
# ============================================================

def generate_raw_port_stockpile_events(rail_df: pd.DataFrame) -> pd.DataFrame:
    """Generate stockpile events with MM62 Pilbara inventory drop in outage window.

    Strategy:
    - For each site/product/day, create rail_in and ship_load events whose net
      tonnes_delta follow a designed daily net pattern.
    - For MM62 at Pilbara Port, net ~0 pre-outage, then -50kt/day between
      INVENTORY_DIP_START and INVENTORY_DIP_END so cumulative inventory
      falls by ~0.4Mt.
    - Other products/sites roughly balanced.
    - CONSTRAINT: Cumulative inventory never goes negative for any site/product.
    """
    print("Generating raw_port_stockpile_events ...")
    rows = []
    event_seq = 1

    # Initialize running inventory for each site/product combination
    # Starting inventories ensure we have buffer for operations
    STARTING_INVENTORY = {
        ("Pilbara Port", "MM62"): 850_000.0,
        ("Pilbara Port", "MM58"): 300_000.0,
        ("Pilbara Port", "MM65"): 150_000.0,
        ("Pilbara Port", "Other"): 80_000.0,
        ("Secondary Port", "MM62"): 200_000.0,
        ("Secondary Port", "MM58"): 100_000.0,
        ("Secondary Port", "MM65"): 50_000.0,
        ("Secondary Port", "Other"): 30_000.0,
    }
    running_inventory = {k: v for k, v in STARTING_INVENTORY.items()}
    
    # Minimum inventory buffer - never go below this
    MIN_INVENTORY_BUFFER = 10_000.0
    
    # Generate initial inventory events on the first day
    # These establish the starting inventory so the SQL cumulative sum works correctly
    first_day = ALL_DATES[0]
    for (site, prod), start_inv in STARTING_INVENTORY.items():
        rows.append(
            {
                "event_id": f"EVT-{first_day.strftime('%Y%m%d')}-{event_seq:04d}",
                "event_time": first_day + pd.Timedelta(hours=0, minutes=1),  # Very early on first day
                "site": site,
                "stockpile_id": f"{site.split()[0][:3].upper()}-{prod}-SP01",
                "product_code": prod,
                "event_type": "initial_inventory",
                "tonnes_delta": float(start_inv),
                "shipment_id": None,
            }
        )
        event_seq += 1

    # design baseline daily net deltas in kt for MM62 Pilbara
    mm62_pilbara_net = {}
    cumulative = 850_000.0  # start around 0.85Mt
    for d in ALL_DATES:
        if INVENTORY_DIP_START <= d <= INVENTORY_DIP_END:
            net = -50_000.0  # drawdown
        else:
            # baseline small oscillation around zero
            net = np.random.normal(0, 10_000)
        mm62_pilbara_net[d] = net
        cumulative += net

    # For each day/site/product create events
    for d in ALL_DATES:
        for site in PORT_SITES:
            for prod in PRODUCTS:
                key = (site, prod)
                current_inventory = running_inventory[key]
                
                # decide number of events
                n_events = np.random.randint(8, 25)

                if site == "Pilbara Port" and prod == "MM62":
                    net = mm62_pilbara_net[d]
                    # target rail_in and ship_load totals
                    baseline_in = 160_000.0
                    rail_in_total = baseline_in + np.random.normal(0, 8_000)
                    ship_load_total = rail_in_total - net
                    ship_load_total = max(ship_load_total, 20_000.0)
                else:
                    # other combinations roughly balanced
                    rail_in_total = max(np.random.normal(40_000, 8_000), 10_000)
                    ship_load_total = max(np.random.normal(40_000, 8_000), 10_000)
                
                # CONSTRAINT: Cap ship_load_total to ensure inventory stays positive
                # Available to ship = current inventory + incoming rail - minimum buffer
                max_shippable = current_inventory + rail_in_total - MIN_INVENTORY_BUFFER
                if ship_load_total > max_shippable:
                    ship_load_total = max(max_shippable, 0.0)  # Can't ship negative

                # split totals into individual events
                rail_events = max(3, int(n_events * np.random.uniform(0.4, 0.7)))
                ship_events = max(2, n_events - rail_events)

                # rail_in events (positive tonnes)
                rail_weights = np.random.dirichlet(np.ones(rail_events))
                rail_tonnes = rail_in_total * rail_weights

                for i in range(rail_events):
                    hour = np.random.randint(0, 24)
                    minute = np.random.randint(0, 60)
                    ts = d + pd.Timedelta(hours=int(hour), minutes=int(minute))
                    rows.append(
                        {
                            "event_id": f"EVT-{d.strftime('%Y%m%d')}-{event_seq:04d}",
                            "event_time": ts,
                            "site": site,
                            "stockpile_id": f"{site.split()[0][:3].upper()}-{prod}-SP{np.random.randint(1, 6):02d}",
                            "product_code": prod,
                            "event_type": "rail_in",
                            "tonnes_delta": float(rail_tonnes[i]),
                            "shipment_id": None,
                        }
                    )
                    event_seq += 1

                # ship_load events (negative tonnes)
                ship_weights = np.random.dirichlet(np.ones(ship_events))
                ship_tonnes = ship_load_total * ship_weights

                # If MM62 Pilbara in outage window, associate some ship_loads with key shipments
                key_shipments = []
                if site == "Pilbara Port" and prod == "MM62" and OUTAGE_START <= d <= OUTAGE_END:
                    key_shipments = ["SHIP-DRAGON-23", "SHIP-NIPPON-11", "SHIP-EURO-07"]

                for i in range(ship_events):
                    hour = np.random.randint(0, 24)
                    minute = np.random.randint(0, 60)
                    ts = d + pd.Timedelta(hours=int(hour), minutes=int(minute))
                    shipment_id = None
                    if key_shipments and np.random.rand() < 0.6:
                        shipment_id = np.random.choice(key_shipments)
                    rows.append(
                        {
                            "event_id": f"EVT-{d.strftime('%Y%m%d')}-{event_seq:04d}",
                            "event_time": ts,
                            "site": site,
                            "stockpile_id": f"{site.split()[0][:3].upper()}-{prod}-SP{np.random.randint(1, 6):02d}",
                            "product_code": prod,
                            "event_type": "ship_load",
                            "tonnes_delta": float(-ship_tonnes[i]),
                            "shipment_id": shipment_id,
                        }
                    )
                    event_seq += 1

                # occasional rehandle/adjustment with small net impact
                adjustment_net = 0.0
                n_extra = np.random.randint(0, 3)
                for i in range(n_extra):
                    hour = np.random.randint(0, 24)
                    minute = np.random.randint(0, 60)
                    ts = d + pd.Timedelta(hours=int(hour), minutes=int(minute))
                    etype = np.random.choice(["rehandle", "adjustment"])
                    tonnes = float(np.random.normal(0, 2_000))
                    
                    # Ensure adjustment doesn't push inventory negative
                    projected = current_inventory + rail_in_total - ship_load_total + adjustment_net + tonnes
                    if projected < MIN_INVENTORY_BUFFER:
                        tonnes = max(0.0, MIN_INVENTORY_BUFFER - (current_inventory + rail_in_total - ship_load_total + adjustment_net))
                    
                    adjustment_net += tonnes
                    rows.append(
                        {
                            "event_id": f"EVT-{d.strftime('%Y%m%d')}-{event_seq:04d}",
                            "event_time": ts,
                            "site": site,
                            "stockpile_id": f"{site.split()[0][:3].upper()}-{prod}-SP{np.random.randint(1, 6):02d}",
                            "product_code": prod,
                            "event_type": etype,
                            "tonnes_delta": tonnes,
                            "shipment_id": None,
                        }
                    )
                    event_seq += 1
                
                # Update running inventory for this site/product
                daily_net = rail_in_total - ship_load_total + adjustment_net
                running_inventory[key] = current_inventory + daily_net

    df = pd.DataFrame(rows)
    print(f"  rows: {len(df):,}")
    return df


# ============================================================
# raw_commercial_contracts
# ============================================================

def generate_raw_commercial_contracts() -> pd.DataFrame:
    print("Generating raw_commercial_contracts ...")
    rows = []
    contract_seq = 1

    for cust in CUSTOMERS:
        for prod in PRODUCTS:
            n_contracts = 3 if prod == "MM62" else 1
            for i in range(n_contracts):
                start_year = np.random.choice([2024, 2025])
                start_month = np.random.randint(1, 13)
                start_date = pd.Timestamp(start_year, start_month, 1)
                duration_years = np.random.randint(2, 5)
                end_date = start_date + pd.DateOffset(years=int(duration_years)) - pd.Timedelta(days=1)

                if prod == "MM62":
                    pricing_index = "62FE_CFR"
                elif prod == "MM58":
                    pricing_index = "58FE_CFR"
                elif prod == "MM65":
                    pricing_index = "65FE_CFR"
                else:
                    pricing_index = "Custom"

                freight_term = np.random.choice(["FOB", "CFR_fixed_freight", "CFR_floating_freight"], p=[0.55, 0.25, 0.20])

                fx_currency = np.random.choice(["USD", "CNY", "JPY", "EUR"], p=[0.7, 0.15, 0.1, 0.05])

                fe_min = 61.7 if prod == "MM62" else np.random.uniform(58.0, 65.0)
                moisture_max = np.random.uniform(9.0, 10.0)

                has_carbon_reopener = False
                requires_scope3 = False
                if cust in ["Dragon Steel", "EuroSteel"] and prod == "MM62":
                    has_carbon_reopener = np.random.rand() < 0.6
                if cust in ["EuroSteel", "Other Europe"]:
                    requires_scope3 = np.random.rand() < 0.7

                demurrage_free_days = int(np.random.randint(3, 6))
                demurrage_rate = float(np.random.uniform(35_000, 60_000))
                base_margin = float(np.random.uniform(20, 28))

                rows.append(
                    {
                        "contract_id": f"CT-{cust.split()[0].upper()}-{start_year}-{contract_seq:02d}",
                        "customer_name": cust,
                        "product_code": prod,
                        "contract_start_date": start_date.date(),
                        "contract_end_date": end_date.date(),
                        "pricing_index": pricing_index,
                        "freight_term": freight_term,
                        "fx_currency": fx_currency,
                        "fe_min_pct": float(round(fe_min, 2)),
                        "moisture_max_pct": float(round(moisture_max, 2)),
                        "has_carbon_price_reopener": bool(has_carbon_reopener),
                        "requires_scope3_reporting": bool(requires_scope3),
                        "demurrage_free_days": demurrage_free_days,
                        "demurrage_rate_usd_per_day": demurrage_rate,
                        "base_margin_target_usd_per_t": base_margin,
                    }
                )
                contract_seq += 1

    df = pd.DataFrame(rows)
    print(f"  rows: {len(df):,}")
    return df


# ============================================================
# raw_vessel_schedule
# ============================================================

def generate_raw_vessel_schedule(contracts: pd.DataFrame) -> pd.DataFrame:
    print("Generating raw_vessel_schedule ...")
    rows = []
    vessel_seq = 1

    # Helper: pick contract matching customer/product with laycan in term
    def pick_contract(cust, prod, laycan_start):
        subset = contracts[
            (contracts["customer_name"] == cust)
            & (contracts["product_code"] == prod)
            & (contracts["contract_start_date"] <= laycan_start.date())
            & (contracts["contract_end_date"] >= laycan_start.date())
        ]
        if subset.empty:
            subset = contracts[
                (contracts["customer_name"] == cust)
                & (contracts["product_code"] == prod)
            ]
        if subset.empty:
            return None
        return subset.sample(1, random_state=np.random.randint(0, 10_000)).iloc[0]

    # Generate vessels across full range
    for d in ALL_DATES[::3]:  # vessel every ~3 days
        for _ in range(np.random.randint(1, 3)):
            customer = _choose(CUSTOMERS, [0.25, 0.2, 0.2, 0.2, 0.15], 1)[0]
            product = _choose(PRODUCTS, [0.65, 0.15, 0.1, 0.1], 1)[0]
            site = _choose(PORT_SITES, [0.9, 0.1], 1)[0]

            laycan_start = d + pd.Timedelta(days=int(np.random.randint(0, 4)))
            laycan_len = np.random.randint(4, 8)
            laycan_end = laycan_start + pd.Timedelta(days=int(laycan_len))

            planned_arrival = laycan_start - pd.Timedelta(days=np.random.randint(1, 3)) + pd.Timedelta(hours=int(np.random.randint(0, 24)))
            actual_arrival_shift = np.random.randint(-12, 24)
            actual_arrival = planned_arrival + pd.Timedelta(hours=int(actual_arrival_shift))

            planned_tonnes = float(np.random.uniform(170_000, 190_000))
            actual_loaded = planned_tonnes * np.random.uniform(0.96, 1.02)

            eff_demurrage_rate = float(np.random.uniform(38_000, 58_000))

            rows.append(
                {
                    "vessel_id": f"VES-{vessel_seq:04d}",
                    "vessel_name": f"VES-{vessel_seq:04d}",
                    "customer_name": customer,
                    "product_code": product,
                    "site": site,
                    "laycan_start_date": laycan_start.date(),
                    "laycan_end_date": laycan_end.date(),
                    "planned_arrival_time": planned_arrival,
                    "actual_arrival_time": actual_arrival,
                    "planned_tonnes": planned_tonnes,
                    "actual_loaded_tonnes": float(actual_loaded),
                    "demurrage_rate_usd_per_day": eff_demurrage_rate,
                }
            )
            vessel_seq += 1

    # Inject three key MM62 Pilbara vessels in outage window with risky laycans
    special_specs = [
        ("DRAGON-23", "Dragon Steel"),
        ("NIPPON-11", "Nippon Metals"),
        ("EURO-07", "EuroSteel"),
    ]
    laycan_ends = [pd.Timestamp("2025-10-22"), pd.Timestamp("2025-10-25"), pd.Timestamp("2025-10-28")]

    for (vname, cust), lend in zip(special_specs, laycan_ends):
        laycan_start = lend - pd.Timedelta(days=5)
        planned_arrival = laycan_start - pd.Timedelta(days=1) + pd.Timedelta(hours=8)
        actual_arrival = planned_arrival + pd.Timedelta(hours=6)
        planned_tonnes = float(np.random.uniform(175_000, 185_000))
        actual_loaded = planned_tonnes * np.random.uniform(0.94, 0.98)
        demurrage_rate = float(np.random.uniform(40_000, 55_000))

        rows.append(
            {
                "vessel_id": f"VES-{vname}",
                "vessel_name": vname,
                "customer_name": cust,
                "product_code": "MM62",
                "site": "Pilbara Port",
                "laycan_start_date": laycan_start.date(),
                "laycan_end_date": lend.date(),
                "planned_arrival_time": planned_arrival,
                "actual_arrival_time": actual_arrival,
                "planned_tonnes": planned_tonnes,
                "actual_loaded_tonnes": float(actual_loaded),
                "demurrage_rate_usd_per_day": demurrage_rate,
            }
        )

    df = pd.DataFrame(rows)
    print(f"  rows: {len(df):,}")
    return df


# ============================================================
# raw_ore_quality_assays
# ============================================================

def generate_raw_ore_quality_assays(port_events: pd.DataFrame) -> pd.DataFrame:
    print("Generating raw_ore_quality_assays ...")
    rows = []
    assay_seq = 1

    # focus samples around ship_load events
    ship_events = port_events[port_events["event_type"] == "ship_load"].copy()

    for idx, ev in ship_events.iterrows():
        site = ev["site"]
        prod = ev["product_code"]
        shipment_id = ev["shipment_id"]
        # Ensure event_time is a proper tz-naive Timestamp (handle potential string values)
        event_time = pd.to_datetime(ev["event_time"]).tz_localize(None)

        n_samples = np.random.randint(1, 4)
        for s in range(n_samples):
            offset_minutes = np.random.randint(-60, 60)
            sample_time = event_time + pd.Timedelta(minutes=int(offset_minutes))

            if prod == "MM62":
                # baseline
                fe_mean = 62.0
                if INVENTORY_DIP_START <= event_time.normalize() <= INVENTORY_DIP_END and shipment_id in ["SHIP-DRAGON-23", "SHIP-EURO-07"]:
                    fe_mean = 61.6
                fe = np.random.normal(fe_mean, 0.15)
                moisture = np.random.normal(8.7, 0.4)
                sio2 = np.random.normal(4.5, 0.5)
                al2o3 = np.random.normal(2.2, 0.3)
                p = np.random.normal(0.075, 0.01)
            else:
                base = {"MM58": 58.5, "MM65": 65.2, "Other": 60.0}[prod]
                fe = np.random.normal(base, 0.3)
                moisture = np.random.normal(8.5, 0.4)
                sio2 = np.random.normal(5.0, 0.7)
                al2o3 = np.random.normal(2.3, 0.3)
                p = np.random.normal(0.08, 0.015)

            rows.append(
                {
                    "assay_id": f"ASSAY-{assay_seq:06d}",
                    "sample_time": sample_time,
                    "site": site,
                    "product_code": prod,
                    "shipment_id": shipment_id,
                    "fe_pct": float(round(fe, 3)),
                    "moisture_pct": float(round(moisture, 3)),
                    "sio2_pct": float(round(sio2, 3)),
                    "al2o3_pct": float(round(al2o3, 3)),
                    "p_pct": float(round(p, 4)),
                }
            )
            assay_seq += 1

    # add some stockpile-only assays (no shipment_id)
    for d in ALL_DATES:
        for site in ["Pilbara Port", "Secondary Port"]:
            for prod in PRODUCTS:
                if np.random.rand() < 0.3:
                    sample_time = d + pd.Timedelta(hours=int(np.random.randint(0, 24)))
                    base = {"MM62": 62.0, "MM58": 58.5, "MM65": 65.2, "Other": 60.0}[prod]
                    fe = np.random.normal(base, 0.3)
                    moisture = np.random.normal(8.5, 0.4)
                    sio2 = np.random.normal(4.8, 0.6)
                    al2o3 = np.random.normal(2.3, 0.3)
                    p = np.random.normal(0.08, 0.015)
                    rows.append(
                        {
                            "assay_id": f"ASSAY-{assay_seq:06d}",
                            "sample_time": sample_time,
                            "site": site,
                            "product_code": prod,
                            "shipment_id": None,
                            "fe_pct": float(round(fe, 3)),
                            "moisture_pct": float(round(moisture, 3)),
                            "sio2_pct": float(round(sio2, 3)),
                            "al2o3_pct": float(round(al2o3, 3)),
                            "p_pct": float(round(p, 4)),
                        }
                    )
                    assay_seq += 1

    df = pd.DataFrame(rows)
    print(f"  rows: {len(df):,}")
    return df


# ============================================================
# raw_market_prices & raw_fx_rates
# ============================================================

def generate_raw_market_prices_and_fx():
    print("Generating raw_market_prices & raw_fx_rates ...")

    price_rows = []
    fx_rows = []

    for d in ALL_DATES:
        # 62FE_CFR baseline 110-120, spike to 130-133 mid October
        if pd.Timestamp("2025-10-10") <= d <= pd.Timestamp("2025-10-24"):
            fe62 = np.random.uniform(130, 133)
        else:
            fe62 = np.random.uniform(110, 120)
        fe58 = fe62 - np.random.uniform(15, 20)
        fe65 = fe62 + np.random.uniform(10, 15)

        # freight index baseline 9-11, +2.5 in mid October
        if pd.Timestamp("2025-10-10") <= d <= pd.Timestamp("2025-10-24"):
            freight = np.random.uniform(11.5, 13.5)
        else:
            freight = np.random.uniform(9, 11)

        price_rows.extend(
            [
                {"price_date": d.date(), "index_name": "62FE_CFR", "price_usd_per_t": float(round(fe62, 2))},
                {"price_date": d.date(), "index_name": "58FE_CFR", "price_usd_per_t": float(round(fe58, 2))},
                {"price_date": d.date(), "index_name": "65FE_CFR", "price_usd_per_t": float(round(fe65, 2))},
                {"price_date": d.date(), "index_name": "PLATTS_WA_CHINA_FREIGHT", "price_usd_per_t": float(round(freight, 2))},
            ]
        )

        # FX: AUDUSD baseline 0.68-0.70, rising to 0.72-0.73 mid-Oct
        if pd.Timestamp("2025-10-10") <= d <= pd.Timestamp("2025-10-24"):
            audusd = np.random.uniform(0.72, 0.73)
        else:
            audusd = np.random.uniform(0.68, 0.70)
        usdcny = np.random.uniform(7.1, 7.4)
        usdjpy = np.random.uniform(140, 150)
        usdeur = np.random.uniform(0.90, 0.96)

        fx_rows.extend(
            [
                {"fx_date": d.date(), "currency_pair": "AUDUSD", "fx_rate": float(round(audusd, 4))},
                {"fx_date": d.date(), "currency_pair": "USDCNY", "fx_rate": float(round(usdcny, 4))},
                {"fx_date": d.date(), "currency_pair": "USDJPY", "fx_rate": float(round(usdjpy, 4))},
                {"fx_date": d.date(), "currency_pair": "USDEUR", "fx_rate": float(round(usdeur, 4))},
            ]
        )

    price_df = pd.DataFrame(price_rows)
    fx_df = pd.DataFrame(fx_rows)
    print(f"  raw_market_prices rows: {len(price_df):,}")
    print(f"  raw_fx_rates rows: {len(fx_df):,}")
    return price_df, fx_df


# ============================================================
# raw_cost_curves
# ============================================================

def generate_raw_cost_curves() -> pd.DataFrame:
    print("Generating raw_cost_curves ...")
    rows = []
    for prod in PRODUCTS:
        for region in ["Pilbara", "Other"]:
            for quarter in ["2025Q3", "2025Q4"]:
                if prod == "MM62":
                    base_cost = np.random.uniform(40, 48)
                else:
                    base_cost = np.random.uniform(35, 55)
                fuel_sens = np.random.uniform(0.8, 1.5)
                freight_sens = np.random.uniform(0.9, 1.1)
                fx_sens = np.random.uniform(0.8, 1.2)
                rows.append(
                    {
                        "cost_curve_id": f"CC-{prod}-{quarter}-{region[0]}",
                        "product_code": prod,
                        "region": region,
                        "quarter": quarter,
                        "unit_cash_cost_usd_per_t": float(round(base_cost, 2)),
                        "fuel_cost_sensitivity_usd_per_t": float(round(fuel_sens, 2)),
                        "freight_cost_sensitivity_usd_per_t": float(round(freight_sens, 2)),
                        "fx_sensitivity_usd_per_t": float(round(fx_sens, 2)),
                    }
                )
    df = pd.DataFrame(rows)
    print(f"  rows: {len(df):,}")
    return df


# ============================================================
# raw_contract_positions
# ============================================================

def generate_raw_contract_positions(contracts: pd.DataFrame) -> pd.DataFrame:
    print("Generating raw_contract_positions ...")
    rows = []
    pos_seq = 1
    for _, c in contracts.iterrows():
        for quarter in ["2025Q3", "2025Q4"]:
            vol = np.random.uniform(200_000, 2_000_000)
            fixed_price = None
            if c["pricing_index"] == "62FE_CFR" and np.random.rand() < 0.3:
                fixed_price = np.random.uniform(115, 135)
            premium = np.random.uniform(-5, 8)
            rows.append(
                {
                    "position_id": f"POS-{c['contract_id']}-{quarter}-{pos_seq:03d}",
                    "contract_id": c["contract_id"],
                    "quarter": quarter,
                    "product_code": c["product_code"],
                    "total_volume_t": float(round(vol, 0)),
                    "fixed_price_usd_per_t": float(round(fixed_price, 2)) if fixed_price is not None else None,
                    "index_premium_discount_usd_per_t": float(round(premium, 2)),
                }
            )
            pos_seq += 1
    df = pd.DataFrame(rows)
    print(f"  rows: {len(df):,}")
    return df


# ============================================================
# raw_maintenance_logs
# ============================================================

def generate_raw_maintenance_logs() -> pd.DataFrame:
    print("Generating raw_maintenance_logs ...")
    rows = []
    wo_seq = 1

    # baseline planned maintenance and smaller unplanned events
    for asset in ASSETS:
        for d in ALL_DATES[::7]:  # about weekly maintenance checks
            if np.random.rand() < 0.4:
                start = d + pd.Timedelta(hours=int(np.random.randint(0, 24)))
                duration = np.random.uniform(4, 12)
                end = start + pd.Timedelta(hours=float(duration))
                rows.append(
                    {
                        "work_order_id": f"WO-{wo_seq:05d}",
                        "asset_id": asset,
                        "asset_type": ASSET_TYPES[asset],
                        "site": ASSET_SITE[asset],
                        "work_order_type": "planned",
                        "start_time": start,
                        "end_time": end,
                        "downtime_hours": float(round(duration, 2)),
                    }
                )
                wo_seq += 1

        for d in ALL_DATES[::15]:  # occasional unplanned
            if np.random.rand() < 0.3:
                start = d + pd.Timedelta(hours=int(np.random.randint(0, 24)))
                duration = np.random.uniform(12, 36)
                end = start + pd.Timedelta(hours=float(duration))
                rows.append(
                    {
                        "work_order_id": f"WO-{wo_seq:05d}",
                        "asset_id": asset,
                        "asset_type": ASSET_TYPES[asset],
                        "site": ASSET_SITE[asset],
                        "work_order_type": "unplanned",
                        "start_time": start,
                        "end_time": end,
                        "downtime_hours": float(round(duration, 2)),
                    }
                )
                wo_seq += 1

    # Inject key SL-2 outage on 2025-10-18 ~72h
    outage_start = OUTAGE_START + pd.Timedelta(hours=6)
    outage_end = outage_start + pd.Timedelta(hours=72)
    rows.append(
        {
            "work_order_id": f"WO-SL2-OUTAGE",
            "asset_id": "SL-2",
            "asset_type": ASSET_TYPES["SL-2"],
            "site": ASSET_SITE["SL-2"],
            "work_order_type": "unplanned",
            "start_time": outage_start,
            "end_time": outage_end,
            "downtime_hours": 72.0,
        }
    )

    df = pd.DataFrame(rows)
    print(f"  rows: {len(df):,}")
    return df


# ============================================================
# raw_asset_telemetry
# ============================================================

def generate_raw_asset_telemetry() -> pd.DataFrame:
    print("Generating raw_asset_telemetry ...")
    rows = []
    for asset in ASSETS:
        for d in ALL_DATES:
            util_base = np.random.uniform(70, 90)
            vib_base = np.random.uniform(4, 7)
            temp_base = np.random.uniform(4, 7)

            if asset in ["SL-2", "CV-01"] and OUTAGE_START - pd.Timedelta(days=7) <= d <= OUTAGE_END + pd.Timedelta(days=7):
                util_base += np.random.uniform(5, 10)
                vib_base += np.random.uniform(1, 3)
                temp_base += np.random.uniform(1, 2)

            rows.append(
                {
                    "asset_id": asset,
                    "date": d.date(),
                    "utilization_pct": float(round(min(util_base, 100.0), 2)),
                    "vibration_index": float(round(vib_base, 2)),
                    "temperature_index": float(round(temp_base, 2)),
                }
            )
    df = pd.DataFrame(rows)
    print(f"  rows: {len(df):,}")
    return df


# ============================================================
# raw_shipment_revenue
# ============================================================

def generate_raw_shipment_revenue(contracts: pd.DataFrame, vessels: pd.DataFrame, market_prices: pd.DataFrame) -> pd.DataFrame:
    print("Generating raw_shipment_revenue ...")
    rows = []

    # simple contract lookup by customer+product
    def pick_contract(cust, prod, date_):
        subset = contracts[
            (contracts["customer_name"] == cust)
            & (contracts["product_code"] == prod)
            & (contracts["contract_start_date"] <= date_)
            & (contracts["contract_end_date"] >= date_)
        ]
        if subset.empty:
            subset = contracts[
                (contracts["customer_name"] == cust)
                & (contracts["product_code"] == prod)
            ]
        if subset.empty:
            return None
        return subset.sample(1, random_state=np.random.randint(0, 10_000)).iloc[0]

    # simple price lookup 62FE_CFR by nearest date
    price_lookup = market_prices[market_prices["index_name"] == "62FE_CFR"].set_index("price_date")["price_usd_per_t"]

    for _, v in vessels.iterrows():
        ship_id = f"SHIP-{v['vessel_name']}"
        cust = v["customer_name"]
        prod = v["product_code"]
        laycan_start = pd.Timestamp(v["laycan_start_date"])
        contract = pick_contract(cust, prod, laycan_start.date())
        if contract is None:
            continue

        price_date = laycan_start.date()
        if price_date not in price_lookup.index:
            # fallback to closest date
            series = price_lookup.reset_index()
            series["diff"] = (series["price_date"] - price_date).abs()
            row = series.sort_values("diff").iloc[0]
            index_price = row["price_usd_per_t"]
        else:
            index_price = price_lookup.loc[price_date]

        if pd.isna(index_price):
            index_price = 120.0

        base_price = float(index_price)
        fixed_price = None
        if contract["pricing_index"] == "62FE_CFR" and np.random.rand() < 0.3:
            fixed_price = base_price + np.random.uniform(-5, 5)

        realized_price = fixed_price if fixed_price is not None else base_price + np.random.uniform(-3, 3)

        planned_tonnes = float(v["planned_tonnes"])
        revenue = planned_tonnes * realized_price

        nomination_date = laycan_start - pd.Timedelta(days=np.random.randint(20, 60))
        planned_load_date = laycan_start + pd.Timedelta(days=np.random.randint(0, 5))

        rows.append(
            {
                "shipment_id": ship_id,
                "contract_id": contract["contract_id"],
                "vessel_id": v["vessel_id"],
                "product_code": prod,
                "nomination_date": nomination_date.date(),
                "planned_load_date": planned_load_date.date(),
                "planned_tonnes": planned_tonnes,
                "realized_price_usd_per_t": float(round(realized_price, 2)),
                "realized_revenue_usd": float(round(revenue, 2)),
            }
        )

    df = pd.DataFrame(rows)
    print(f"  rows: {len(df):,}")
    return df


# ============================================================
# VALIDATION HELPERS
# ============================================================

def validate_inventory_story(port_events: pd.DataFrame):
    print("\nValidation: MM62 Pilbara Port inventory pattern ...")
    df = port_events.copy()
    # Ensure event_time is tz-naive datetime before using .dt accessor
    df["event_time"] = pd.to_datetime(df["event_time"]).dt.tz_localize(None)
    df["event_date"] = df["event_time"].dt.date
    filt = (df["site"] == "Pilbara Port") & (df["product_code"] == "MM62")
    daily = df[filt].groupby("event_date")["tonnes_delta"].sum().reset_index()
    daily["event_date"] = pd.to_datetime(daily["event_date"])
    daily = daily.sort_values("event_date")
    # Starting inventory is now included as initial_inventory events, so cumsum starts from 0
    daily["cum_tonnes"] = daily["tonnes_delta"].cumsum()

    pre = daily[daily["event_date"] < OUTAGE_START]
    dip = daily[(daily["event_date"] >= INVENTORY_DIP_START) & (daily["event_date"] <= INVENTORY_DIP_END)]
    post = daily[daily["event_date"] > INVENTORY_DIP_END]

    print(
        "  Pre-outage MM62 Pilbara avg inventory (Mt):",
        round(pre["cum_tonnes"].mean() / 1e6, 3) if not pre.empty else "n/a",
    )
    print(
        "  Dip window MM62 Pilbara min inventory (Mt):",
        round(dip["cum_tonnes"].min() / 1e6, 3) if not dip.empty else "n/a",
    )
    print(
        "  Post-dip MM62 Pilbara avg inventory (Mt):",
        round(post["cum_tonnes"].mean() / 1e6, 3) if not post.empty else "n/a",
    )
    
    # Validate that inventory never goes negative for any site/product
    print("\nValidation: Checking inventory never goes negative ...")
    
    all_ok = True
    for site in PORT_SITES:
        for prod in PRODUCTS:
            filt = (df["site"] == site) & (df["product_code"] == prod)
            site_daily = df[filt].groupby("event_date")["tonnes_delta"].sum().reset_index()
            site_daily["event_date"] = pd.to_datetime(site_daily["event_date"])
            site_daily = site_daily.sort_values("event_date")
            # Starting inventory is now included as initial_inventory events
            site_daily["cum_tonnes"] = site_daily["tonnes_delta"].cumsum()
            
            min_inv = site_daily["cum_tonnes"].min() if not site_daily.empty else 0
            if min_inv < 0:
                print(f"  ❌ {site} {prod}: minimum inventory = {min_inv:,.0f} (NEGATIVE!)")
                all_ok = False
            else:
                print(f"  ✓ {site} {prod}: minimum inventory = {min_inv:,.0f}")
    
    if all_ok:
        print("  ✅ All site/product combinations have non-negative inventory")


def validate_vessel_story(vessels: pd.DataFrame):
    print("\nValidation: October MM62 vessels at Pilbara Port ...")
    df = vessels.copy()
    df["laycan_start_date"] = pd.to_datetime(df["laycan_start_date"])
    oct_mm62 = df[
        (df["laycan_start_date"].dt.month == 10)
        & (df["product_code"] == "MM62")
        & (df["site"] == "Pilbara Port")
    ]
    print("  October MM62 Pilbara vessels:")
    print(
        oct_mm62[["vessel_name", "customer_name", "laycan_start_date", "laycan_end_date", "planned_tonnes", "actual_loaded_tonnes"]]
        .head(10)
        .to_string(index=False)
    )


def validate_quality_story(assays: pd.DataFrame):
    print("\nValidation: MM62 Fe% during outage vs baseline ...")
    df = assays[assays["product_code"] == "MM62"].copy()
    # Ensure sample_time is tz-naive datetime before using .dt accessor
    df["sample_time"] = pd.to_datetime(df["sample_time"]).dt.tz_localize(None)
    df["sample_date"] = df["sample_time"].dt.date
    df["sample_date"] = pd.to_datetime(df["sample_date"])
    pre = df[df["sample_date"] < OUTAGE_START]
    dip = df[(df["sample_date"] >= INVENTORY_DIP_START) & (df["sample_date"] <= INVENTORY_DIP_END)]
    print("  Pre-outage avg Fe%:", round(pre["fe_pct"].mean(), 3) if not pre.empty else "n/a")
    print("  Dip window avg Fe%:", round(dip["fe_pct"].mean(), 3) if not dip.empty else "n/a")


def validate_maintenance_story(maint: pd.DataFrame):
    print("\nValidation: Asset outage events ...")
    sl2 = maint[maint["asset_id"] == "SL-2"].copy()
    # Ensure start_time is tz-naive datetime before using .dt accessor
    sl2["start_time"] = pd.to_datetime(sl2["start_time"]).dt.tz_localize(None)
    sl2["start_date"] = sl2["start_time"].dt.date
    print("  SL-2 maintenance events around outage:")
    print(sl2.sort_values("start_time").tail(5).to_string(index=False))


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("Starting data generation for Mega Minerals (story-driven)...")
    print("-" * 60)

    # 1) Generate core raw tables
    mine_production = generate_raw_mine_production()
    save_to_parquet(mine_production, "raw_mine_production", num_files=4)

    rail_movements = generate_raw_rail_movements(mine_production)
    save_to_parquet(rail_movements, "raw_rail_movements", num_files=4)

    port_stockpile_events = generate_raw_port_stockpile_events(rail_movements)
    save_to_parquet(port_stockpile_events, "raw_port_stockpile_events", num_files=6)

    contracts = generate_raw_commercial_contracts()
    save_to_parquet(contracts, "raw_commercial_contracts", num_files=1)

    vessel_schedule = generate_raw_vessel_schedule(contracts)
    save_to_parquet(vessel_schedule, "raw_vessel_schedule", num_files=1)

    ore_quality_assays = generate_raw_ore_quality_assays(port_stockpile_events)
    save_to_parquet(ore_quality_assays, "raw_ore_quality_assays", num_files=4)

    market_prices, fx_rates = generate_raw_market_prices_and_fx()
    save_to_parquet(market_prices, "raw_market_prices", num_files=1)
    save_to_parquet(fx_rates, "raw_fx_rates", num_files=1)

    cost_curves = generate_raw_cost_curves()
    save_to_parquet(cost_curves, "raw_cost_curves", num_files=1)

    contract_positions = generate_raw_contract_positions(contracts)
    save_to_parquet(contract_positions, "raw_contract_positions", num_files=1)

    maintenance_logs = generate_raw_maintenance_logs()
    save_to_parquet(maintenance_logs, "raw_maintenance_logs", num_files=2)

    asset_telemetry = generate_raw_asset_telemetry()
    save_to_parquet(asset_telemetry, "raw_asset_telemetry", num_files=2)

    shipment_revenue = generate_raw_shipment_revenue(contracts, vessel_schedule, market_prices)
    save_to_parquet(shipment_revenue, "raw_shipment_revenue", num_files=1)

    # 2) Story validation summaries
    validate_inventory_story(port_stockpile_events)
    validate_vessel_story(vessel_schedule)
    validate_quality_story(ore_quality_assays)
    validate_maintenance_story(maintenance_logs)

    # 3) Load PDF files to volume
    for pdf_path in glob.glob("documents/pdf/*.pdf"):
        load_pdf_to_volume(pdf_path)

    print("\n" + "=" * 60)
    print("Data generation COMPLETE - raw tables ready for SQL transformations.")
    print("=" * 60)
