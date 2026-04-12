"""
Supply Acceptor V2 — Fetch and Run
====================================
Fetches demand (furniture + removals) and reservations from Snowflake,
then runs supply_acceptor_v2.py.

Key difference from fetch_and_run.py:
  - fetch_demand_v2() includes removal jobs (ca.IDENT = 'removals') tagged
    with number_of_men = 12, so supply_acceptor_v2 can separate furniture
    and removal demand when computing per-zone targets.

Data-fetching helpers (get_conn, fetch_tp_quality, fetch_reservations,
load_cluster_map, etc.) are shared from fetch_and_run.py — no duplication.

Usage:
    python3 fetch_and_run_v2.py 2026-04-02
    python3 fetch_and_run_v2.py 2026-04-02 2026-04-03
    python3 fetch_and_run_v2.py                         # defaults to tomorrow

Forecast mode (reservations only, demand from forecast files):
    python3 fetch_and_run_v2.py --forecast 2026-04-04
"""

import sys
import os
import subprocess
import pandas as pd
from datetime import date, timedelta, datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
sys.path.insert(0, os.path.join(SCRIPT_DIR, 'legacy'))

# ── Shared data-fetching helpers from V1 (not duplicated) ─────────────────────
from fetch_and_run import (
    get_conn,
    sf_query,
    fetch_tp_quality,
    fetch_reservations,
    load_cluster_map,
    fmt,
    ZONE_MAP,
    UK_LAT_MIN, UK_LAT_MAX,
    UK_LNG_MIN, UK_LNG_MAX,
    NUTS_FILTER,
)


# ── V2 demand fetch: furniture + removals ──────────────────────────────────────

def fetch_demand_v2(dates: list, conn) -> pd.DataFrame:
    """
    Fetch demand from Snowflake including both furniture and removal jobs.

    Output columns: sourcezone, pickup_day, number_of_men, realized_lane_level_jobs

    Encoding:
      number_of_men = 1  → furniture 1-man jobs (category_id=1, NUMBER_OF_MEN_REQUIRED=1)
      number_of_men = 2  → furniture 2-man jobs (category_id=1, NUMBER_OF_MEN_REQUIRED=2)
      number_of_men = 12 → removal jobs (category_id=2) — any crew size

    V1 fetch_demand() only includes general_goods_move / house_move and excludes
    removals. V2 adds removals so supply_acceptor_v2 can separate them in targeting.
    """
    cluster_map = load_cluster_map()
    date_strs   = ', '.join(f"'{d}'" for d in dates)

    sql = f"""
    SELECT DISTINCT
        t.LISTING_ID,
        t.CATEGORY_ID,
        TO_VARCHAR(DATE(t.PICKUP_DATE), 'YYYY-MM-DD') AS PICKUP_DATE,
        start_place.LAT  AS START_LAT,
        start_place.LNG  AS START_LNG,
        end_place.LAT    AS END_LAT,
        end_place.LNG    AS END_LNG,
        ls.NUMBER_OF_MEN_REQUIRED AS MANS
    FROM harmonised.production.LISTING t
    JOIN harmonised.production.ROUTE route           ON t.ROUTE_ID = route.ROUTE_ID
    JOIN harmonised.production.PLACE start_place     ON route.START_PLACE_ID = start_place.PLACE_ID
    JOIN harmonised.production.PLACE end_place       ON route.END_PLACE_ID   = end_place.PLACE_ID
    JOIN harmonised.production.LISTING_SCORE ls      ON t.LISTING_ID = ls.LISTING_ID
    JOIN harmonised.staging.CATEGORY ca              ON t.category_id = ca.category_id
    LEFT JOIN harmonised.production.NUTS sn          ON start_place.nuts_id = sn.nuts_id
    LEFT JOIN harmonised.production.NUTS en          ON end_place.nuts_id   = en.nuts_id
    WHERE TO_VARCHAR(DATE(t.PICKUP_DATE), 'YYYY-MM-DD') IN ({date_strs})
      AND t.status = 21
      AND t.locale = 'en-gb'
      AND DATE(t.delivery_date) = DATE(t.pickup_date)
      AND {NUTS_FILTER}
      AND t.routable = 1
      AND ca.IDENT IN ('general_goods_move', 'house_move', 'removals')
    """

    print("  Fetching demand (furniture + removals)...")
    raw = sf_query(conn, sql)
    print(f"  Raw jobs: {len(raw)}")

    for col in ['START_LAT', 'START_LNG', 'END_LAT', 'END_LNG']:
        raw[col] = pd.to_numeric(raw[col], errors='coerce')
    raw['MANS']        = pd.to_numeric(raw['MANS'],        errors='coerce').fillna(1).astype(int)
    raw['CATEGORY_ID'] = pd.to_numeric(raw['CATEGORY_ID'], errors='coerce').fillna(1).astype(int)

    # UK bounding box filter
    df = raw[
        (raw['START_LAT'].between(UK_LAT_MIN, UK_LAT_MAX)) &
        (raw['START_LNG'].between(UK_LNG_MIN, UK_LNG_MAX)) &
        (raw['END_LAT'].between(UK_LAT_MIN, UK_LAT_MAX)) &
        (raw['END_LNG'].between(UK_LNG_MIN, UK_LNG_MAX))
    ].copy()

    # Zone assignment via lat/long → cluster CSV
    df['start_key'] = df['START_LAT'].apply(fmt) + ':' + df['START_LNG'].apply(fmt)
    df['end_key']   = df['END_LAT'].apply(fmt)   + ':' + df['END_LNG'].apply(fmt)
    df['sourcezone']      = df['start_key'].map(cluster_map)
    df['destinationzone'] = df['end_key'].map(cluster_map)
    before = len(df)
    df = df.dropna(subset=['sourcezone', 'destinationzone'])
    print(f"  Zone mapping: {len(df)}/{before} matched")

    df['sourcezone'] = df['sourcezone'].map(ZONE_MAP).fillna(df['sourcezone'])

    # number_of_men is the actual crew size (1 or 2) for all jobs including removals.
    # A separate 'category' column distinguishes furniture from removals.
    df['number_of_men'] = df['MANS'].astype(int).clip(1, 2)
    df['category']      = df['CATEGORY_ID'].apply(
        lambda c: 'removals' if int(c) == 2 else 'furniture'
    )

    demand = (
        df.groupby(['sourcezone', 'number_of_men', 'category', 'PICKUP_DATE'])
        .size()
        .reset_index(name='realized_lane_level_jobs')
        .rename(columns={'PICKUP_DATE': 'pickup_day'})
    )

    furn_jobs = int(demand[demand['category'] == 'furniture']['realized_lane_level_jobs'].sum())
    rem_jobs  = int(demand[demand['category'] == 'removals']['realized_lane_level_jobs'].sum())
    totals    = demand.groupby('pickup_day')['realized_lane_level_jobs'].sum().to_dict()
    print(f"  Demand totals: {totals}  (furniture: {furn_jobs}, removals: {rem_jobs})")
    return demand


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    forecast_mode = '--forecast' in args
    if forecast_mode:
        args = [a for a in args if a != '--forecast']

    dates = args if args else [(date.today() + timedelta(days=1)).strftime('%Y-%m-%d')]
    print(f"Mode  : {'FORECAST (reservations only)' if forecast_mode else 'CONFIRMED JOBS — V2'}")
    print(f"Dates : {dates}")

    stamp    = datetime.now().strftime('%Y-%m-%d_%H%M')
    res_file = os.path.join(SCRIPT_DIR, f'recommended_reservations_{stamp}.csv')

    print("\nConnecting to Snowflake (browser auth)...")
    conn = get_conn()

    print("\nFetching TP quality...")
    tp_quality = fetch_tp_quality(conn)

    if not forecast_mode:
        demand_file = os.path.join(SCRIPT_DIR, f'demand_v2_{stamp}.csv')
        print("\nFetching demand (v2 — furniture + removals)...")
        demand = fetch_demand_v2(dates, conn)
        demand.to_csv(demand_file, index=False)
        print(f"  → {demand_file}")

    print("\nFetching reservations...")
    res = fetch_reservations(dates, conn, tp_quality)
    res.to_csv(res_file, index=False)
    print(f"  → {res_file}")

    conn.close()

    if forecast_mode:
        print("\nRunning supply acceptor v2 (forecast mode)...")
        result = subprocess.run(
            [sys.executable,
             os.path.join(SCRIPT_DIR, 'supply_acceptor_forecast_v2.py'),
             res_file],
            capture_output=False,
        )
    else:
        print("\nRunning supply acceptor v2 (confirmed jobs mode)...")
        result = subprocess.run(
            [sys.executable,
             os.path.join(SCRIPT_DIR, 'supply_acceptor_v2.py'),
             demand_file,
             res_file],
            capture_output=False,
        )

    sys.exit(result.returncode)


if __name__ == '__main__':
    main()
