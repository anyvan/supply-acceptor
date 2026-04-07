"""
Supply Acceptor Forecast V2
============================
Uses the cluster forecast CSVs (v5/v3/v2 furniture + removals) to build a
V2-format demand signal, then runs supply_acceptor_v2 targeting and vetting.

Key differences from supply_acceptor_forecast.py (V1):
  - Demand is split into furniture-1M, furniture-2M, and removals separately
    (not merged into a single realized_jobs total).
  - Per-zone targets use empirical parameters from jpj_parameters.csv:
      retention_1m, retention_2m, pct_1m_1j, pct_1m_2j, jpj_1m, jpj_2m
  - EI JPJ vetting uses the fitted linear model instead of the fixed 5.5.
  - No JPJ multiplier applied — conservatism is encoded in zone coverage ratios.

Forecast demand encoding for V2:
    number_of_men = 1  → pred_d1_routable_furn (man_type=1)  furniture 1-man
    number_of_men = 2  → pred_d1_routable_furn (man_type=2)  furniture 2-man
    number_of_men = 12 → pred_d1_routable_rem  (all types)   removal jobs

Usage:
    python3 supply_acceptor_forecast_v2.py <reservations_file> [output_file]
    python3 supply_acceptor_forecast_v2.py                     # uses defaults
    python3 supply_acceptor_forecast_v2.py /path/to/forecast_dir <res.csv>
"""

import sys
import os
import glob
from datetime import datetime
import pandas as pd

SCRIPT_DIR           = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR           = os.path.dirname(SCRIPT_DIR)
DEFAULT_FORECAST_DIR = os.path.join(PARENT_DIR, 'updated_forecast', 'production_v5')
OUTPUT_DIR           = os.path.join(SCRIPT_DIR, 'outputs')
os.makedirs(OUTPUT_DIR, exist_ok=True)

sys.path.insert(0, SCRIPT_DIR)

# ── Shared forecast-file helpers from V1 ──────────────────────────────────────
# find_forecast_files() and load_forecast_demand() are unchanged — reuse directly.
from supply_acceptor_forecast import find_forecast_files, load_forecast_demand


# ── Convert forecast data to V2 demand format ─────────────────────────────────

def convert_to_v2_demand(furn_path: str, rem_path: str) -> pd.DataFrame:
    """
    Reads the furniture + removals cluster forecast files and returns a V2
    demand DataFrame with columns:
        sourcezone, pickup_day, number_of_men, category,
        realized_lane_level_jobs,   ← forecast (pred_d1_routable)
        confirmed_routable_jobs      ← actuals already in the forecast sheet

    Furniture 1M  (number_of_men=1)  : pred_d1_routable_furn / confirmed_routable_furn  where man_type=1
    Furniture 2M  (number_of_men=2)  : pred_d1_routable_furn / confirmed_routable_furn  where man_type=2
    Removals      (number_of_men=12) : pred_d1_routable_rem  / confirmed_routable_rem   (all man_types summed)

    Uses load_forecast_demand() from V1 for file reading and cluster→zone mapping.
    """
    forecast = load_forecast_demand(furn_path, rem_path)

    # Furniture 1-man
    furn_1m = (
        forecast[forecast['man_type'] == 1]
        [['pickup_date', 'sourcezone', 'pred_d1_routable_furn', 'confirmed_routable_furn']]
        .rename(columns={
            'pickup_date':              'pickup_day',
            'pred_d1_routable_furn':    'realized_lane_level_jobs',
            'confirmed_routable_furn':  'confirmed_routable_jobs',
        })
        .assign(number_of_men=1, category='furniture')
    )

    # Furniture 2-man
    furn_2m = (
        forecast[forecast['man_type'] == 2]
        [['pickup_date', 'sourcezone', 'pred_d1_routable_furn', 'confirmed_routable_furn']]
        .rename(columns={
            'pickup_date':              'pickup_day',
            'pred_d1_routable_furn':    'realized_lane_level_jobs',
            'confirmed_routable_furn':  'confirmed_routable_jobs',
        })
        .assign(number_of_men=2, category='furniture')
    )

    # Removals — aggregate across man_types
    rem_all = (
        forecast
        .groupby(['pickup_date', 'sourcezone'])[['pred_d1_routable_rem', 'confirmed_routable_rem']]
        .sum()
        .reset_index()
        .rename(columns={
            'pickup_date':             'pickup_day',
            'pred_d1_routable_rem':    'realized_lane_level_jobs',
            'confirmed_routable_rem':  'confirmed_routable_jobs',
        })
        .assign(number_of_men=2, category='removals')
    )

    # Load full (all-distance) removals file for confirmed_total — it sits alongside
    # the lt200km file with the same date stamp but without '_lt200km' in the name.
    rem_full_path = rem_path.replace('_lt200km', '')
    if os.path.exists(rem_full_path):
        rem_full = pd.read_csv(rem_full_path)
        rem_full['pickup_date'] = pd.to_datetime(rem_full['pickup_date'])
        rem_full['cluster']     = rem_full['cluster'].str.strip().str.lower()
        rem_full['sourcezone']  = rem_full['cluster'].map(
            __import__('supply_acceptor_forecast', fromlist=['CLUSTER_TO_ZONE']).CLUSTER_TO_ZONE
        )
        conf_total = (
            rem_full.dropna(subset=['sourcezone'])
            .groupby(['pickup_date', 'sourcezone'])['confirmed_total']
            .sum()
            .reset_index()
            .rename(columns={'pickup_date': 'pickup_day', 'confirmed_total': 'confirmed_total_rem_jobs'})
        )
        rem_all = rem_all.merge(conf_total, on=['pickup_day', 'sourcezone'], how='left')
    else:
        print(f"  [forecast-v2] Full removals file not found ({os.path.basename(rem_full_path)}) — CTRem will show lt200km total")
        rem_all['confirmed_total_rem_jobs'] = forecast.groupby(
            ['pickup_date', 'sourcezone'])['confirmed_total_rem'].transform('sum')

    demand = pd.concat([furn_1m, furn_2m, rem_all], ignore_index=True)
    demand = demand[demand['realized_lane_level_jobs'] > 0].copy()
    demand['realized_lane_level_jobs']   = demand['realized_lane_level_jobs'].round().astype(int)
    demand['confirmed_routable_jobs']    = demand['confirmed_routable_jobs'].fillna(0).round().astype(int)
    demand['confirmed_total_rem_jobs']   = demand['confirmed_total_rem_jobs'].fillna(0).round().astype(int)

    return demand[['sourcezone', 'pickup_day', 'number_of_men', 'category',
                   'realized_lane_level_jobs', 'confirmed_routable_jobs', 'confirmed_total_rem_jobs']]


# ── Main forecast-V2 runner ───────────────────────────────────────────────────

def run(forecast_dir: str, res_path: str,
        output_path: str = None, run_date: str = None,
        pickup_dates: list = None) -> str:
    """
    End-to-end forecast V2 run.

    1. Finds the latest (or run_date-specific) forecast CSVs in forecast_dir.
    2. Converts them to V2 demand format.
    3. Filters to pickup_dates if provided (avoids processing all future dates).
    4. Saves demand to a temp CSV.
    5. Calls supply_acceptor_v2.run(demand_csv, res_csv, output_path).

    Returns the output CSV path.
    """
    import supply_acceptor_v2 as sav2

    furn_path, rem_path, run_date_str = find_forecast_files(forecast_dir, run_date)
    print(f"\n[forecast-v2] Forecast run : {run_date_str}")
    print(f"  Furniture : {os.path.basename(furn_path)}")
    print(f"  Removals  : {os.path.basename(rem_path)}")

    demand = convert_to_v2_demand(furn_path, rem_path)

    if pickup_dates:
        date_filter = pd.to_datetime(pickup_dates)
        demand = demand[demand['pickup_day'].isin(date_filter)].copy()

    furn_total = int(demand[demand['category'] == 'furniture']['realized_lane_level_jobs'].sum())
    rem_total  = int(demand[demand['category'] == 'removals']['realized_lane_level_jobs'].sum())
    dates_seen = sorted(demand['pickup_day'].unique())
    print(f"  Demand    : {furn_total} furniture + {rem_total} removals | dates: {[str(d)[:10] for d in dates_seen]}")

    stamp       = datetime.now().strftime('%Y-%m-%d_%H%M')
    demand_tmp  = os.path.join(OUTPUT_DIR, f'demand_v2_forecast_{stamp}.csv')
    demand.to_csv(demand_tmp, index=False)

    if output_path is None:
        output_path = os.path.join(OUTPUT_DIR, f'supply_acceptor_v2_forecast_output_{stamp}.csv')

    sav2.run(demand_tmp, res_path, output_path, use_effective_targets=True)
    return output_path


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    res_path     = None
    output_path  = None
    forecast_dir = DEFAULT_FORECAST_DIR

    for arg in args:
        if os.path.isdir(arg):
            forecast_dir = arg
        elif arg.endswith('.csv') and res_path is None:
            res_path = arg
        elif arg.endswith('.csv') and output_path is None:
            output_path = arg

    if res_path is None:
        candidates = sorted(
            glob.glob(os.path.join(SCRIPT_DIR, 'recommended_reservations_*.csv'))
        )
        if not candidates:
            print("ERROR: No reservations file found. Pass path as argument.")
            sys.exit(1)
        res_path = candidates[-1]
        print(f"[forecast-v2] Using reservations: {os.path.basename(res_path)}")

    run(forecast_dir, res_path, output_path)


if __name__ == '__main__':
    main()
