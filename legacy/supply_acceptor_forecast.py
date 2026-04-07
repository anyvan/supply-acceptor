"""
Supply Acceptor — Forecast Mode
================================
Identical acceptance logic to supply_acceptor.py, but demand is sourced from
the V2 cluster forecast files (generated nightly by the forecast pipeline)
instead of confirmed jobs from Snowflake.

Demand signal used for acceptance decisions:
    pred_d1_routable_furn + pred_d1_routable_rem   (per zone × man_type × date)
    — i.e. predicted routable jobs expected to be confirmed by D-1 1am.

Reference columns shown in output (for visibility only, not used in logic):
    confirmed_total_furn, confirmed_routable_furn
    confirmed_total_rem,  confirmed_routable_rem

Forecast input files (from production/ folder, keyed by run date):
    v2_furniture_cluster_YYYY-MM-DD.csv   — furniture cluster forecast
    v2_removals_cluster_YYYY-MM-DD.csv    — removals cluster forecast
    Columns: pickup_date, cluster, man_type,
             confirmed_total, confirmed_routable,
             pred_total, pred_routable, pred_d1_total, pred_d1_routable

Reservations input file:
    Same format as supply_acceptor.py — recommended_reservations_YYYY-MM-DD.csv

Usage:
    python3 supply_acceptor_forecast.py <forecast_dir> <reservations_file> [output_file]
    python3 supply_acceptor_forecast.py                     # uses defaults
    python3 supply_acceptor_forecast.py 2026-03-20          # use run-date files

    <forecast_dir>      Path to folder containing v2_*_cluster_YYYY-MM-DD.csv files.
                        Defaults to: <script_dir>/../updated_forecast/production/
    <reservations_file> Path to reservations CSV.  Defaults to latest in script dir.
    <output_file>       Optional output path.

Old supply_acceptor.py is unchanged and continues to work as before.
"""

import sys
import os
import math
import re
import random
import glob
import pandas as pd
import numpy as np
from datetime import datetime

# ── Cluster → Supply Zone Mapping ─────────────────────────────────────────────
# Source: supply-management-services/supplyacceptor/centersforcluster.csv
# Clusters not listed in any supply zone (cardiff, aberdeen) are excluded.
# dumfries is rolled into edinburgh-glasgow (geographically close, same supply area).

CLUSTER_TO_ZONE = {
    'london':       'london',
    'okehampton':   'cornwall',
    'salisbury':    'salisbury',
    'cardiff':      'cardiff',
    'birmingham':   'birmingham',
    'corwen':       'north wales',
    'oxford':       'oxford',
    'peterborough': 'peterborough',
    'brighton':     'brighton',
    'canterbury':   'kent',
    'norwich':      'norwich',
    'warrington':   'manchester',
    'sheffield':    'sheffield',
    'kendal':       'lake district',
    'scarborough':  'east yorkshire',
    'newcastle':    'newcastle',
    'dumfries':     'north lake district',
    'edinburgh':    'edinburgh-glasgow',
    'fort william': 'northwest scotland',
    'aberdeen':     'northeast scotland',
}

# ── Configuration (identical to supply_acceptor.py) ───────────────────────────

DENSE_ZONES = {'london', 'manchester', 'oxford', 'birmingham'}
DENSE_JPJ   = 5.5
LIGHT_JPJ   = 5.3

ZONE_JPJ_OVERRIDES = {
    'london': 5.8,
}

DEFAULT_COVERAGE = 1.00

ZONE_COVERAGE_OVERRIDES = {
    'birmingham': {
        'default': 0.50,
        'period_overrides': [
            (set(range(1, 4)),   0.75),
            (set(range(24, 32)), 0.75),
        ],
    },
    'manchester': {
        'default': 0.75,
        'period_overrides': [],
    },
    'peterborough': {
        'default': 0.30,
        'period_overrides': [],
    },
    'edinburgh-glasgow': {
        'default': 0.75,
        'period_overrides': [],
    },
    'oxford': {
        'default': 0.50,
        'period_overrides': [
            (set(range(1, 4)),   1.00),
            (set(range(24, 32)), 1.00),
        ],
    },
    'salisbury': {
        'default': 0.75,
        'period_overrides': [
            (set(range(1, 4)),   1.00),
            (set(range(24, 32)), 1.00),
        ],
    },
    'sheffield': {
        'default': 0.75,
        'period_overrides': [],
    },
}

ZONE_OVERFLOW_TARGETS = {
    'birmingham':        ['oxford', 'peterborough'],
    'brighton':          ['salisbury', 'kent'],
    'cardiff':           ['cornwall'],
    'edinburgh-glasgow': ['north lake district', 'newcastle'],
    'london':            ['oxford', 'peterborough', 'kent', 'brighton'],
    'peterborough':      ['oxford', 'norwich'],
    'manchester':        ['sheffield', 'north wales', 'lake district'],
}

VERSATILITY_1MAN_THRESHOLD   = 0.30
MIN_1MAN_JOBS_FOR_1MAN_TP    = 9
MIN_RATING                   = 4.4
MIN_CAPACITY                 = 8

DIVERSITY_T1_PCT = 0.50
DIVERSITY_T2_PCT = 0.30

SOUTH_LONDON_PREFIXES    = {'SE', 'SW', 'KT', 'CR', 'TW', 'BR', 'SM'}
LONDON_SOUTH_QUOTA_PCT   = 0.16
LONDON_1MAN_FACTOR       = 0.66
LONDON_1MAN_JPJ          = 5.5
LONDON_OVERSUPPLY_RATIO  = 1.2
LONDON_OVERSUPPLY_JPJ    = 6.0
LONDON_OVERSUPPLY_BUFFER = 5

ZONE_MIN_JOBS = {
    'east yorkshire':     10,
    'kent':               10,
    'north wales':        10,
    'north lake district':10,
    'norwich':            10,
    'northwest scotland': 10,
    'northeast scotland': 10,
}

W_RATING = 0.45
W_DEALLO = 0.34
W_VAT    = 0.06
W_CAP    = 0.10
W_TYPE   = 0.05

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_FORECAST_DIR = os.path.join(BASE_DIR, '..', 'updated_forecast', 'production')


# ── Forecast file discovery ────────────────────────────────────────────────────

def find_forecast_files(forecast_dir: str, run_date: str = None):
    """
    Find the furniture and removals cluster forecast files in forecast_dir.

    Priority order (highest first):
      1. v3_furniture_cluster_lt200km_*.csv  /  v3_removals_cluster_lt200km_*.csv
      2. v3_furniture_cluster_*.csv          /  v3_removals_cluster_*.csv
      3. v2_furniture_cluster_*.csv          /  v2_removals_cluster_*.csv

    If run_date (YYYY-MM-DD) is given, look for that exact date.
    Otherwise, picks the most recent file by date in the filename.

    Returns (furn_path, rem_path, run_date_str).
    """
    def extract_date(path):
        m = re.search(r'(\d{4}-\d{2}-\d{2})\.csv$', path)
        return m.group(1) if m else ''

    def find_best(category):
        """Return (files, version_label) for the highest available version."""
        for pattern, label in [
            (f'v5_{category}_cluster_lt200km_*.csv', 'v5_lt200km'),
            (f'v5_{category}_cluster_*.csv',          'v5'),
            (f'v3_{category}_cluster_lt200km_*.csv', 'v3_lt200km'),
            (f'v3_{category}_cluster_*.csv',          'v3'),
            (f'v2_{category}_cluster_*.csv',          'v2'),
        ]:
            files = sorted(glob.glob(os.path.join(forecast_dir, pattern)))
            if files:
                return files, label
        return [], None

    furn_files, furn_ver = find_best('furniture')
    rem_files,  rem_ver  = find_best('removals')

    if not furn_files:
        raise FileNotFoundError(f"No furniture cluster forecast found in {forecast_dir}")
    if not rem_files:
        raise FileNotFoundError(f"No removals cluster forecast found in {forecast_dir}")

    if run_date:
        furn = [f for f in furn_files if run_date in f]
        rem  = [f for f in rem_files  if run_date in f]
        if not furn:
            raise FileNotFoundError(f"{furn_ver} furniture cluster file for {run_date} not found in {forecast_dir}")
        if not rem:
            raise FileNotFoundError(f"{rem_ver} removals cluster file for {run_date} not found in {forecast_dir}")
        furn_path, rem_path = furn[-1], rem[-1]
    else:
        furn_path = max(furn_files, key=extract_date)
        rem_path  = max(rem_files,  key=extract_date)

    run_date_str = extract_date(furn_path)
    print(f"  [forecast] Furniture: {os.path.basename(furn_path)}  ({furn_ver})")
    print(f"  [forecast] Removals:  {os.path.basename(rem_path)}  ({rem_ver})")
    return furn_path, rem_path, run_date_str


def load_forecast_demand(furn_path: str, rem_path: str) -> pd.DataFrame:
    """
    Load and merge furniture + removals cluster forecasts.

    Returns a DataFrame with one row per (pickup_date, sourcezone, man_type):
        sourcezone              — supply zone name (after cluster mapping)
        pickup_date             — datetime
        man_type                — 1 or 2
        pred_d1_routable        — sum of furn + rem pred_d1_routable  ← acceptance demand
        confirmed_total_furn    — furniture confirmed_total            ← reference
        confirmed_routable_furn — furniture confirmed_routable         ← reference
        confirmed_total_rem     — removals  confirmed_total            ← reference
        confirmed_routable_rem  — removals  confirmed_routable         ← reference
        pred_d1_routable_furn   — furniture pred_d1_routable           ← reference
        pred_d1_routable_rem    — removals  pred_d1_routable           ← reference
    """
    furn = pd.read_csv(furn_path)
    rem  = pd.read_csv(rem_path)

    furn['pickup_date'] = pd.to_datetime(furn['pickup_date'])
    rem['pickup_date']  = pd.to_datetime(rem['pickup_date'])
    furn['cluster']     = furn['cluster'].str.strip().str.lower()
    rem['cluster']      = rem['cluster'].str.strip().str.lower()

    # Map cluster → supply zone; drop unmapped clusters
    furn['sourcezone'] = furn['cluster'].map(CLUSTER_TO_ZONE)
    rem['sourcezone']  = rem['cluster'].map(CLUSTER_TO_ZONE)

    unmapped_f = furn[furn['sourcezone'].isna()]['cluster'].unique()
    unmapped_r = rem[rem['sourcezone'].isna()]['cluster'].unique()
    if len(unmapped_f) > 0:
        print(f"  [forecast] Furniture clusters excluded (no supply zone): {list(unmapped_f)}")
    if len(unmapped_r) > 0:
        print(f"  [forecast] Removals clusters excluded (no supply zone):  {list(unmapped_r)}")

    furn = furn.dropna(subset=['sourcezone'])
    rem  = rem.dropna(subset=['sourcezone'])

    # Rename columns for clarity before merge
    furn_cols = furn.rename(columns={
        'confirmed_total':    'confirmed_total_furn',
        'confirmed_routable': 'confirmed_routable_furn',
        'pred_d1_routable':   'pred_d1_routable_furn',
    })[['pickup_date', 'sourcezone', 'man_type',
        'confirmed_total_furn', 'confirmed_routable_furn', 'pred_d1_routable_furn']]

    rem_cols = rem.rename(columns={
        'confirmed_total':    'confirmed_total_rem',
        'confirmed_routable': 'confirmed_routable_rem',
        'pred_d1_routable':   'pred_d1_routable_rem',
    })[['pickup_date', 'sourcezone', 'man_type',
        'confirmed_total_rem', 'confirmed_routable_rem', 'pred_d1_routable_rem']]

    # Multiple clusters may map to the same zone — aggregate before merge
    furn_agg = furn_cols.groupby(['pickup_date', 'sourcezone', 'man_type']).sum().reset_index()
    rem_agg  = rem_cols.groupby(['pickup_date', 'sourcezone', 'man_type']).sum().reset_index()

    merged = furn_agg.merge(rem_agg, on=['pickup_date', 'sourcezone', 'man_type'], how='outer')
    merged = merged.fillna(0)

    # Combined demand signal used for acceptance decisions
    merged['pred_d1_routable'] = merged['pred_d1_routable_furn'] + merged['pred_d1_routable_rem']

    return merged


# ── Helper functions (identical to supply_acceptor.py) ───────────────────────

def get_jpj(zone: str) -> float:
    zone = zone.strip().lower()
    if zone in ZONE_JPJ_OVERRIDES:
        return ZONE_JPJ_OVERRIDES[zone]
    return DENSE_JPJ if zone in DENSE_ZONES else LIGHT_JPJ


def dynamic_coverage(zone: str = '', pickup_date=None) -> float:
    zone = zone.strip().lower()
    if zone in ZONE_COVERAGE_OVERRIDES and pickup_date is not None:
        override = ZONE_COVERAGE_OVERRIDES[zone]
        day = pickup_date.day
        for day_range, ratio in override.get('period_overrides', []):
            if day in day_range:
                return ratio
        return override['default']
    return DEFAULT_COVERAGE


def score_tp(row: pd.Series) -> float:
    rating = float(row.get('rating') or 0)
    if rating == 6:
        rating_score = 0.90
    elif rating >= 4.4:
        rating_score = 0.60 + (rating - 4.5) * 0.80
    else:
        rating_score = max(0.0, rating / 5.0)

    try:
        deallo = max(0.0, float(row.get('Deallo Rate Overall') or 0))
    except (ValueError, TypeError):
        deallo = 0.0

    if deallo == 0:
        deallo_score = 1.00
    elif deallo <= 0.05:
        deallo_score = 0.90
    elif deallo <= 0.10:
        deallo_score = 0.70
    elif deallo <= 0.20:
        deallo_score = 0.40
    else:
        deallo_score = max(0.0, 1.0 - deallo * 4)

    try:
        vat = int(row.get('VAT_STATUS') or 0)
    except (ValueError, TypeError):
        vat = 0
    vat_score = 1.0 if vat == 0 else 0.0

    try:
        cap = float(row.get('RESERVATION_CAPACITY') or 15)
    except (ValueError, TypeError):
        cap = 15.0
    if cap < 8:
        cap_score = 0.0
    elif cap < 10:
        cap_score = 0.3
    elif cap < 15:
        cap_score = 0.5
    else:
        cap_score = 0.65

    res_type = str(row.get('RES_TYPE') or '').strip().lower()
    consider  = str(row.get('consider_res_type') or '').strip().lower()
    men       = int(row.get('NUMBER_OF_MEN') or 1)

    if men == 12:
        type_score = 1.00
    elif res_type == 'custom' and consider == 'local':
        type_score = 0.80
    elif res_type in ('local', 'national'):
        type_score = 0.70
    elif res_type == 'custom' and consider == 'national':
        type_score = 0.20
    else:
        type_score = 0.50

    score = (rating_score * W_RATING +
             deallo_score * W_DEALLO +
             vat_score    * W_VAT    +
             cap_score    * W_CAP    +
             type_score   * W_TYPE)
    return round(score, 4)


def versatility_rank(row: pd.Series) -> int:
    men      = int(row.get('NUMBER_OF_MEN') or 1)
    res_type = str(row.get('RES_TYPE') or '').strip().lower()
    consider = str(row.get('consider_res_type') or '').strip().lower()
    if men == 12 and res_type == 'national':
        return 4
    if men == 12:
        return 3
    if res_type == 'national':
        return 2
    return 1


def diversified_sample(candidates: pd.DataFrame, n: int) -> pd.DataFrame:
    if candidates.empty or n <= 0:
        return candidates.iloc[0:0]
    n = min(n, len(candidates))
    if n == len(candidates):
        return candidates

    unique_count = candidates['USERNAME'].nunique()

    if n <= unique_count:
        # Enough unique TPs to fill — enforce diversity by deduplicating first
        pool = candidates.sort_values('_score', ascending=False).drop_duplicates(subset='USERNAME', keep='first')
    else:
        # Not enough unique TPs — allow multiple reservations from the same TP (extra vehicles)
        pool = candidates

    n = min(n, len(pool))
    sorted_cands = pool.sort_values('_score', ascending=False)
    total  = len(sorted_cands)
    t1_end = math.ceil(total / 3)
    t2_end = t1_end + math.ceil((total - t1_end) / 2)

    tier_pools = [
        list(sorted_cands.iloc[:t1_end].index),
        list(sorted_cands.iloc[t1_end:t2_end].index),
        list(sorted_cands.iloc[t2_end:].index),
    ]

    selected_idx = []
    for _ in range(n):
        r = random.random()
        if r < 0.50:
            tier_order = [0, 1, 2]
        elif r < 0.80:
            tier_order = [1, 0, 2]
        else:
            tier_order = [2, 1, 0]
        for t in tier_order:
            if tier_pools[t]:
                idx = random.choice(tier_pools[t])
                tier_pools[t].remove(idx)
                selected_idx.append(idx)
                break

    return candidates.loc[selected_idx]


def select_candidates(candidates: pd.DataFrame, gap: int,
                      already_accepted_usernames: set,
                      one_man_ratio: float = 0.0) -> pd.DataFrame:
    def _fill(pool, n):
        if pool.empty or n <= 0:
            return pool.iloc[0:0]
        if one_man_ratio >= VERSATILITY_1MAN_THRESHOLD:
            twelve_man = pool[pool['NUMBER_OF_MEN'].apply(
                lambda m: int(m) if pd.notna(m) else 1) == 12]
            others = pool[pool['NUMBER_OF_MEN'].apply(
                lambda m: int(m) if pd.notna(m) else 1) != 12]
            sel_12 = diversified_sample(twelve_man, n)
            rem    = n - len(sel_12)
            sel_ot = diversified_sample(others, rem) if rem > 0 else others.iloc[0:0]
            parts  = [s for s in [sel_12, sel_ot] if not s.empty]
            return pd.concat(parts) if parts else pool.iloc[0:0]
        else:
            return diversified_sample(pool, n)

    primary  = candidates[~candidates['USERNAME'].isin(already_accepted_usernames)]
    fallback = candidates[ candidates['USERNAME'].isin(already_accepted_usernames)]

    selected  = []
    remaining = gap

    if not primary.empty:
        sel = _fill(primary, remaining)
        selected.append(sel)
        remaining -= len(sel)

    if remaining > 0 and not fallback.empty:
        sel = _fill(fallback, remaining)
        selected.append(sel)

    return pd.concat(selected) if selected else candidates.iloc[0:0]


def is_south_london(postcode) -> bool:
    if not postcode or pd.isna(postcode):
        return False
    pc = str(postcode).strip().upper()
    return any(pc.startswith(p) for p in SOUTH_LONDON_PREFIXES)


def london_calc_target(realized_jobs: float, total_available: int, apply_buffer: bool = True):
    base_journeys = round(realized_jobs / ZONE_JPJ_OVERRIDES.get('london', DENSE_JPJ))
    is_tight = total_available < LONDON_OVERSUPPLY_RATIO * base_journeys
    if is_tight:
        target = base_journeys
    else:
        buffer = LONDON_OVERSUPPLY_BUFFER if apply_buffer else 0
        target = max(0, round(realized_jobs / LONDON_OVERSUPPLY_JPJ) - buffer)
    return target, is_tight


def london_select_candidates(candidates, zone_data, one_man_jobs, gap, already_accepted=None):
    if candidates.empty or gap <= 0:
        return candidates.iloc[0:0]

    is_tight     = bool(zone_data.get('is_tight', False))
    final_target = int(zone_data['target_reservations'])

    cands = candidates.copy()
    cands['_cap_val']    = cands['RESERVATION_CAPACITY'].apply(lambda c: float(c or 0))
    cands['_rating_val'] = cands['rating'].apply(lambda r: float(r or 0))
    cands['_men_val']    = cands['NUMBER_OF_MEN'].apply(lambda m: int(m) if pd.notna(m) else 1)

    cands = cands[cands['_rating_val'] >= MIN_RATING]
    cands = cands[cands['_cap_val']    >= MIN_CAPACITY]

    if cands.empty:
        return cands

    if not is_tight:
        cands = cands.sort_values('_score', ascending=False)

    cands['_south'] = cands['START_POSTCODE'].apply(is_south_london)

    # ── Compute already-accepted composition to correctly size remaining sub-quotas
    acc_south = acc_1man = acc_2man_nat = acc_2man_loc = 0
    acc_usernames = set()
    if already_accepted is not None and not already_accepted.empty:
        acc = already_accepted.copy()
        acc['_south_a'] = acc['START_POSTCODE'].apply(is_south_london)
        acc['_men_a']   = acc['NUMBER_OF_MEN'].apply(lambda m: int(m) if pd.notna(m) else 1)
        acc_rt          = acc['RES_TYPE'].str.strip().str.lower()
        acc_south    = int(acc['_south_a'].sum())
        acc_1man     = int(acc[acc['_men_a'].isin([1, 12])].shape[0])
        acc_2man_nat = int(acc[(acc['_men_a'].isin([2, 12])) & (acc_rt == 'national')].shape[0])
        acc_2man_loc = int(acc[(acc['_men_a'].isin([2, 12])) & (acc_rt == 'local')].shape[0])
        acc_usernames = set(acc['USERNAME'].str.strip())

    # ── Sub-quota totals, reduced by already-accepted composition ─────────────
    south_quota_total = round(final_target * LONDON_SOUTH_QUOTA_PCT)
    one_man_total     = round(one_man_jobs * LONDON_1MAN_FACTOR / LONDON_1MAN_JPJ)
    two_man_total     = final_target - one_man_total
    national_total    = round(two_man_total / 2)
    local_total       = two_man_total - national_total

    south_quota_need  = max(0, south_quota_total - acc_south)
    one_man_need      = max(0, one_man_total      - acc_1man)
    national_need     = max(0, national_total     - acc_2man_nat)
    local_need        = max(0, local_total        - acc_2man_loc)

    selected       = []
    used_idx       = set()
    used_usernames = set(acc_usernames)

    def pool_excluding_used(base, extra_mask=None):
        """Return base pool excluding used indices, preferring unused usernames.
        Falls back to including used usernames only if needed to fill the pool."""
        idx_mask = ~base.index.isin(used_idx)
        if extra_mask is not None:
            idx_mask = idx_mask & extra_mask
        fresh = base[idx_mask & ~base['USERNAME'].isin(used_usernames)]
        stale = base[idx_mask &  base['USERNAME'].isin(used_usernames)]
        return pd.concat([fresh, stale]) if not stale.empty else fresh

    # ── Pass 1: South London quota ────────────────────────────────────────────
    south_pool = pool_excluding_used(cands, cands['_south'])
    south_sel  = diversified_sample(south_pool, south_quota_need)
    selected.append(south_sel)
    used_idx      |= set(south_sel.index)
    used_usernames |= set(south_sel['USERNAME'])

    sl_1man_count    = len(south_sel[south_sel['_men_val'].isin([1, 12])])
    sl_2man_nat_count = len(south_sel[(south_sel['_men_val'].isin([2, 12])) & (south_sel['RES_TYPE'].str.lower() == 'national')])
    sl_2man_loc_count = len(south_sel[(south_sel['_men_val'].isin([2, 12])) & (south_sel['RES_TYPE'].str.lower() == 'local')])

    if len(south_sel) < south_quota_need:
        print(f'  ⚠ London South quota: only {acc_south + len(south_sel)} of {south_quota_total} South London TPs filled')

    # ── Pass 2: 1-man bucket ─────────────────────────────────────────────────
    one_man_remaining = max(0, one_man_need - sl_1man_count)
    one_man_pool      = pool_excluding_used(cands, cands['_men_val'].isin([1, 12]))
    one_man_sel       = diversified_sample(one_man_pool, one_man_remaining)
    selected.append(one_man_sel)
    used_idx      |= set(one_man_sel.index)
    used_usernames |= set(one_man_sel['USERNAME'])

    one_man_shortfall = one_man_remaining - len(one_man_sel)
    if one_man_shortfall > 0:
        print(f'  ⚠ London 1-man shortfall: need {one_man_remaining}, only {len(one_man_sel)} available'
              f' — adding up to 4 extra 2-man TPs')

    # ── Pass 3: 2-man bucket — national/local split, cap >= 10 ───────────────
    extra_2man       = min(one_man_shortfall, 4)
    national_needed  = max(0, national_need - sl_2man_nat_count) + round(extra_2man / 2)
    local_needed     = max(0, local_need    - sl_2man_loc_count) + (extra_2man - round(extra_2man / 2))

    two_man_mask = cands['_men_val'].isin([2, 12]) & (cands['_cap_val'] >= 10)
    two_man_base = pool_excluding_used(cands, two_man_mask)
    nat_pool = two_man_base[two_man_base['RES_TYPE'] == 'national'].sort_values('_score', ascending=False)
    loc_pool = two_man_base[two_man_base['RES_TYPE'] == 'local'].sort_values('_score', ascending=False)

    nat_sel = diversified_sample(nat_pool, national_needed)
    loc_sel = diversified_sample(loc_pool, local_needed)
    selected.extend([nat_sel, loc_sel])

    if len(nat_sel) < national_needed:
        print(f'  ⚠ London 2-man national shortfall: need {national_needed}, only {len(nat_sel)} available')
    if len(loc_sel) < local_needed:
        print(f'  ⚠ London 2-man local shortfall: need {local_needed}, only {len(loc_sel)} available')

    non_empty = [df for df in selected if not df.empty]
    if not non_empty:
        return candidates.iloc[0:0]
    result = pd.concat(non_empty)
    return result.head(gap)


# ── Main algorithm ─────────────────────────────────────────────────────────────

def run(forecast_dir: str, res_path: str, output_path: str = None, run_date: str = None):

    # ── Load forecast demand ───────────────────────────────────────────────────
    furn_path, rem_path, run_date_str = find_forecast_files(forecast_dir, run_date)
    print(f"  [forecast] Run date:  {run_date_str}")

    forecast = load_forecast_demand(furn_path, rem_path)

    # ── Load reservations ──────────────────────────────────────────────────────
    res = pd.read_csv(res_path, low_memory=False)
    res = res.loc[:, ~res.columns.str.startswith('Unnamed')]
    res['sourcezone']  = res['sourcezone'].str.strip().str.lower()
    res['DATE']        = pd.to_datetime(res['DATE'], dayfirst=True, format='mixed')
    res['IRES_STATUS'] = res['IRES_STATUS'].str.strip().str.lower()
    res['RES_TYPE']    = res['RES_TYPE'].str.strip().str.lower()

    # ── Step 1: Zone + date demand targets (from forecast) ────────────────────
    # Total demand per (zone, date): sum pred_d1_routable across both man types
    zone_day = (
        forecast[forecast['man_type'].isin([1, 2])]
        .groupby(['sourcezone', 'pickup_date'])
        .agg(
            realized_jobs           = ('pred_d1_routable',        'sum'),
            confirmed_total_furn    = ('confirmed_total_furn',    'sum'),
            confirmed_routable_furn = ('confirmed_routable_furn', 'sum'),
            confirmed_total_rem     = ('confirmed_total_rem',     'sum'),
            confirmed_routable_rem  = ('confirmed_routable_rem',  'sum'),
            pred_d1_routable_furn   = ('pred_d1_routable_furn',  'sum'),
            pred_d1_routable_rem    = ('pred_d1_routable_rem',   'sum'),
        )
        .reset_index()
        .rename(columns={'pickup_date': 'pickup_day'})
    )

    # 1-man jobs = pred_d1_routable for man_type=1 only (furn + rem combined)
    one_man_forecast = (
        forecast[forecast['man_type'] == 1]
        .groupby(['sourcezone', 'pickup_date'])['pred_d1_routable']
        .sum()
        .reset_index()
        .rename(columns={'pickup_date': 'pickup_day', 'pred_d1_routable': 'one_man_jobs'})
    )
    zone_day = zone_day.merge(one_man_forecast, on=['sourcezone', 'pickup_day'], how='left')
    zone_day['one_man_jobs']  = zone_day['one_man_jobs'].fillna(0)
    zone_day['one_man_ratio'] = zone_day['one_man_jobs'] / zone_day['realized_jobs'].replace(0, 1)

    zone_day['jpj']            = zone_day['sourcezone'].apply(get_jpj)
    zone_day['total_journeys'] = zone_day['realized_jobs'] / zone_day['jpj']

    # ── Step 2: Count accepted and pending ────────────────────────────────────
    non_return = res[res['RES_TYPE'] != 'return']

    accepted_counts = (
        res[res['IRES_STATUS'] == 'accepted']
        .groupby(['sourcezone', 'DATE']).size()
        .reset_index(name='accepted_count')
    )
    pending_counts = (
        non_return[non_return['IRES_STATUS'] == 'pending']
        .groupby(['sourcezone', 'DATE']).size()
        .reset_index(name='pending_count')
    )

    zone_day = zone_day.merge(
        accepted_counts,
        left_on=['sourcezone', 'pickup_day'],
        right_on=['sourcezone', 'DATE'], how='left'
    ).drop(columns=['DATE'], errors='ignore')

    zone_day = zone_day.merge(
        pending_counts,
        left_on=['sourcezone', 'pickup_day'],
        right_on=['sourcezone', 'DATE'], how='left'
    ).drop(columns=['DATE'], errors='ignore')

    zone_day['accepted_count']  = zone_day['accepted_count'].fillna(0).astype(int)
    zone_day['pending_count']   = zone_day['pending_count'].fillna(0).astype(int)
    zone_day['total_available'] = zone_day['accepted_count'] + zone_day['pending_count']

    zone_day['coverage_ratio'] = zone_day.apply(
        lambda r: dynamic_coverage(zone=r['sourcezone'], pickup_date=r['pickup_day']),
        axis=1
    )
    zone_day['target_reservations'] = zone_day.apply(
        lambda r: int(r['total_journeys'] * r['coverage_ratio']), axis=1
    ).astype(int)
    zone_day['gap'] = (zone_day['target_reservations'] - zone_day['accepted_count']).clip(lower=0)

    # ── Tightness override ─────────────────────────────────────────────────────
    zone_day['is_tight']    = False
    zone_day['london_mode'] = ''

    for idx, row in zone_day[zone_day['sourcezone'].isin(ZONE_COVERAGE_OVERRIDES.keys())].iterrows():
        if row['sourcezone'] == 'london':
            continue
        if float(row['coverage_ratio']) >= 1.0:
            continue
        full_target = round(float(row['total_journeys']) * 1.0)
        if int(row['total_available']) < full_target:
            print(f"  [tight] {row['sourcezone']} {row['pickup_day'].date()}: "
                  f"supply tight (avail={int(row['total_available'])} < full_target={full_target}) "
                  f"— switching to 100% coverage (was {int(row['coverage_ratio']*100)}%)")
            zone_day.at[idx, 'coverage_ratio']      = 1.0
            zone_day.at[idx, 'target_reservations'] = full_target
            zone_day.at[idx, 'gap'] = max(0, full_target - int(row['accepted_count']))
            zone_day.at[idx, 'is_tight'] = True

    # ── London target override ─────────────────────────────────────────────────
    for idx, row in zone_day[zone_day['sourcezone'] == 'london'].iterrows():
        total_avail  = int(row['accepted_count']) + int(row['pending_count'])
        target, tight = london_calc_target(float(row['realized_jobs']), total_avail, apply_buffer=True)
        zone_day.at[idx, 'target_reservations'] = target
        zone_day.at[idx, 'is_tight']   = tight
        zone_day.at[idx, 'london_mode'] = 'TIGHT' if tight else 'OVERSUP'
        zone_day.at[idx, 'gap'] = max(0, target - int(row['accepted_count']))
        base = round(float(row['realized_jobs']) / DENSE_JPJ)
        zone_day.at[idx, 'coverage_ratio'] = target / base if base > 0 else 1.0

    # ── Overflow credits ───────────────────────────────────────────────────────
    OVERSUPPLIED_EXCESS_THRESHOLD = 0.85
    zone_day = zone_day.reset_index(drop=True)

    def calc_excess(row):
        if row['sourcezone'] in ZONE_COVERAGE_OVERRIDES:
            return max(0, math.floor(row['accepted_count'] - row['total_journeys'] * OVERSUPPLIED_EXCESS_THRESHOLD))
        else:
            return max(0, int(row['accepted_count']) - round(row['total_journeys']))

    zone_day['tp_excess']       = zone_day.apply(calc_excess, axis=1).astype(int)
    zone_day['overflow_credit'] = 0

    for src_zone, tgt_zones in ZONE_OVERFLOW_TARGETS.items():
        src_rows = zone_day[zone_day['sourcezone'] == src_zone]
        for _, src_row in src_rows.iterrows():
            overflow = int(src_row['tp_excess'])
            if overflow <= 0:
                continue
            date = src_row['pickup_day']
            for tgt_zone in tgt_zones:
                if overflow <= 0:
                    break
                mask = (zone_day['sourcezone'] == tgt_zone) & (zone_day['pickup_day'] == date)
                if not mask.any():
                    continue
                idx = zone_day[mask].index[0]
                current_gap = int(zone_day.at[idx, 'gap'])
                reduction   = min(overflow, current_gap)
                zone_day.at[idx, 'gap']             -= reduction
                zone_day.at[idx, 'overflow_credit'] += reduction
                overflow -= reduction

    # ── Step 3: Score pending reservations ────────────────────────────────────
    pending_mask = (res['IRES_STATUS'] == 'pending') & (res['RES_TYPE'] != 'return')
    res.loc[pending_mask, '_score']       = res[pending_mask].apply(score_tp, axis=1)
    res.loc[pending_mask, '_versatility'] = res[pending_mask].apply(versatility_rank, axis=1)

    res['new_recommendation']      = False
    res['new_recommendation_rank'] = pd.NA

    # ── Step 4: Select best pending TPs per zone+date ─────────────────────────
    accepted_summary = []

    for _, zd in zone_day.iterrows():
        zone = zd['sourcezone']
        date = zd['pickup_day']
        gap  = int(zd['gap'])

        newly_accepted = 0
        unfilled       = 0

        min_jobs = ZONE_MIN_JOBS.get(zone, 0)
        if min_jobs > 0 and float(zd['realized_jobs']) < min_jobs:
            print(f"  [skip] {zone} {date.date()}: only {float(zd['realized_jobs']):.1f} predicted jobs "
                  f"(min {min_jobs} required) — no reservations accepted")
            gap = 0

        if gap > 0:
            cand_mask = (
                (res['sourcezone'] == zone) &
                (res['DATE']       == date) &
                pending_mask
            )
            candidates = res.loc[cand_mask].copy()
            candidates = candidates[
                candidates['rating'].apply(lambda r: float(r or 0)) >= MIN_RATING
            ]

            if not candidates.empty:
                if zone == 'london':
                    mode = zd.get('london_mode', '')
                    print(f"  [london] {date.date()}: {mode} | target={int(zd['target_reservations'])} "
                          f"| avail={int(zd['accepted_count'])+int(zd['pending_count'])} "
                          f"| gap={gap}")
                    accepted_london = res[
                        (res['sourcezone'] == 'london') &
                        (res['DATE'] == date) &
                        (res['IRES_STATUS'] == 'accepted')
                    ]
                    to_accept = london_select_candidates(
                        candidates, zd, float(zd.get('one_man_jobs', 0)), gap,
                        already_accepted=accepted_london
                    )
                else:
                    one_man_jobs_z = float(zd.get('one_man_jobs', 0))
                    if one_man_jobs_z <= MIN_1MAN_JOBS_FOR_1MAN_TP:
                        before = len(candidates)
                        candidates = candidates[
                            candidates['NUMBER_OF_MEN'].apply(
                                lambda m: int(m) if pd.notna(m) else 1
                            ) != 1
                        ]
                        filtered = before - len(candidates)
                        if filtered > 0:
                            print(f"  [filter] {zone} {date.date()}: removed {filtered} pure 1-man TP(s) "
                                  f"(only {one_man_jobs_z:.1f} 1-man jobs ≤ {MIN_1MAN_JOBS_FOR_1MAN_TP} threshold)")

                    # Usernames already accepted OR already newly recommended this run
                    # (avoid selecting the same TP twice from two pending reservations)
                    accepted_usernames = set(
                        res[
                            (res['sourcezone'] == zone) &
                            (res['DATE'] == date) &
                            (
                                (res['IRES_STATUS'] == 'accepted') |
                                (res['new_recommendation'] == True)
                            )
                        ]['USERNAME'].str.strip()
                    )
                    to_accept = select_candidates(
                        candidates, gap, accepted_usernames,
                        float(zd.get('one_man_ratio', 0))
                    )

                for rank, idx in enumerate(to_accept.index, start=1):
                    res.at[idx, 'new_recommendation']      = True
                    res.at[idx, 'new_recommendation_rank'] = rank
                newly_accepted = min(gap, len(to_accept))
                unfilled       = max(0, gap - len(to_accept))

        # London 1-man composition check
        london_1man_warning = ''
        if zone == 'london' and int(zd['accepted_count']) > 0:
            one_man_target_ldn = round(float(zd.get('one_man_jobs', 0)) * LONDON_1MAN_FACTOR / LONDON_1MAN_JPJ)
            accepted_london = res[
                (res['sourcezone'] == 'london') &
                (res['DATE'] == date) &
                (res['IRES_STATUS'] == 'accepted')
            ]
            accepted_1man = len(accepted_london[
                accepted_london['NUMBER_OF_MEN'].apply(
                    lambda m: int(m) if pd.notna(m) else 1).isin([1, 12])
            ])
            if accepted_1man < one_man_target_ldn:
                shortfall = one_man_target_ldn - accepted_1man
                london_1man_warning = (
                    f'  ⚠ London {date.date()}: 1-man shortfall — need {one_man_target_ldn}, '
                    f'accepted {accepted_1man} (shortfall={shortfall})'
                )
                print(london_1man_warning)

        tp_excess   = int(zd['tp_excess'])
        ovfl_credit = int(zd['overflow_credit'])
        zone_label  = zone
        if zd.get('london_mode') == 'OVERSUP':
            zone_label = f"{zone} ▲"
        elif zd.get('london_mode') == 'TIGHT':
            zone_label = f"{zone} [TIGHT]"
        elif bool(zd.get('is_tight')):
            zone_label = f"{zone} [TIGHT]"

        accepted_summary.append({
            'Zone':                    zone_label,
            'Pickup Date':             date.date(),
            # Forecast demand (acceptance inputs)
            'Pred Jobs (D-1)':         round(float(zd['realized_jobs']), 1),
            'One Man Jobs':            float(zd.get('one_man_jobs', 0)),
            'Pred Furn (D-1)':         round(float(zd.get('pred_d1_routable_furn', 0)), 1),
            'Pred Rem (D-1)':          round(float(zd.get('pred_d1_routable_rem', 0)), 1),
            # Reference: confirmed demand today
            'Conf Furn (Total)':       int(round(zd.get('confirmed_total_furn', 0))),
            'Conf Furn (Routable)':    int(round(zd.get('confirmed_routable_furn', 0))),
            'Conf Rem (Total)':        int(round(zd.get('confirmed_total_rem', 0))),
            'Conf Rem (Routable)':     int(round(zd.get('confirmed_routable_rem', 0))),
            'Conf Routable':           int(round(zd.get('confirmed_routable_furn', 0))) + int(round(zd.get('confirmed_routable_rem', 0))),
            # Acceptance summary
            'Journeys':                round(float(zd['total_journeys']), 1),
            'Coverage %':              int(zd['coverage_ratio'] * 100),
            'Target Res':              int(zd['target_reservations']),
            'Already Accepted':        int(zd['accepted_count']),
            'Pending':                 int(zd['pending_count']),
            'TP Excess':               tp_excess,
            'Ovfl Credit':             ovfl_credit,
            'Gap':                     gap,
            'Newly Accepted':          newly_accepted,
            'Unfilled Gap':            unfilled,
        })

    # ── Output ────────────────────────────────────────────────────────────────
    res = res.drop(columns=['_score', '_versatility'], errors='ignore')

    if output_path is None:
        ts = datetime.now().strftime('%Y-%m-%d_%H%M')
        output_path = os.path.join(BASE_DIR, f'supply_acceptor_forecast_output_{ts}.csv')

    res.to_csv(output_path, index=False)

    # ── Print summary ─────────────────────────────────────────────────────────
    print("\n" + "=" * 110)
    print("  SUPPLY ACCEPTOR (FORECAST MODE) — RECOMMENDATIONS")
    print(f"  Run time      : {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Forecast date : {run_date_str}")
    print("=" * 110)

    if accepted_summary:
        summary_df = pd.DataFrame(accepted_summary)
        summary_df = summary_df.sort_values(['Pickup Date', 'Zone'])

        # Merge reference columns from forecast for display
        furn_ref = forecast.groupby(['sourcezone', 'pickup_date']).agg(
            pred_d1_routable_furn=('pred_d1_routable_furn', 'sum'),
            pred_d1_routable_rem =('pred_d1_routable_rem',  'sum'),
        ).reset_index().rename(columns={'pickup_date': 'pickup_day'})

        total_new = res['new_recommendation'].sum()

        for date, grp in summary_df.groupby('Pickup Date'):
            print(f"\n  Pickup: {date}")
            print(f"  {'Zone':<26} {'PredFurn':>9} {'PredRem':>8} {'PredTotal':>10} "
                  f"{'CRFurn':>7} {'CRRem':>6} {'CRout':>6} "
                  f"{'Jrnys':>6} {'Cov%':>5} {'Target':>7} {'Acc':>5} {'Pend':>5} "
                  f"{'Gap':>5} {'NewAcc':>7} {'Unfill':>7}")
            print(f"  {'-'*128}")
            for _, r in grp.iterrows():
                unfill_flag = ' ⚠' if r['Unfilled Gap'] > 0 else ''
                zone_base   = r['Zone'].replace('▲', '').replace('[TIGHT]', '').strip().lower()
                min_j       = ZONE_MIN_JOBS.get(zone_base, 0)
                is_skipped  = min_j > 0 and float(r['Pred Jobs (D-1)']) < min_j
                cov_str     = '   —' if is_skipped else f"{r['Coverage %']:>4}%"
                tgt_str     = '      —' if is_skipped else f"{r['Target Res']:>7}"
                gap_str     = '    —' if is_skipped else f"{r['Gap']:>5}"
                print(f"  {r['Zone']:<26} "
                      f"{r['Pred Furn (D-1)']:>9.1f} {r['Pred Rem (D-1)']:>8.1f} "
                      f"{r['Pred Jobs (D-1)']:>10.1f} "
                      f"{r['Conf Furn (Routable)']:>7} {r['Conf Rem (Routable)']:>6} "
                      f"{r['Conf Routable']:>6} "
                      f"{r['Journeys']:>6.1f} {cov_str} "
                      f"{tgt_str} {r['Already Accepted']:>5} {r['Pending']:>5} "
                      f"{gap_str} "
                      f"{r['Newly Accepted']:>7} {r['Unfilled Gap']:>7}{unfill_flag}")
            day_total_pred    = grp['Pred Jobs (D-1)'].sum()
            day_pred_furn     = grp['Pred Furn (D-1)'].sum()
            day_pred_rem      = grp['Pred Rem (D-1)'].sum()
            day_conf_furn     = grp['Conf Furn (Total)'].sum()
            day_conf_rfurn    = grp['Conf Furn (Routable)'].sum()
            day_conf_rem      = grp['Conf Rem (Total)'].sum()
            day_conf_rrem     = grp['Conf Rem (Routable)'].sum()
            day_conf_rout     = grp['Conf Routable'].sum()
            day_new           = grp['Newly Accepted'].sum()
            day_unfill        = grp['Unfilled Gap'].sum()
            print(f"  {'TOTAL':<26} {day_pred_furn:>9.1f} {day_pred_rem:>8.1f} {day_total_pred:>10.1f} "
                  f"{day_conf_rfurn:>7} {day_conf_rrem:>6} {day_conf_rout:>6} "
                  f"{'':>6} {'':>5} {'':>7} {'':>5} {'':>5} {'':>5} {day_new:>7} {day_unfill:>7}")

            # ── EI Vetting ────────────────────────────────────────────────────
            day_post_acc      = grp['Already Accepted'].sum() + grp['Newly Accepted'].sum()
            expected_journeys = day_total_pred / 5.5
            journeys_to_ei    = expected_journeys - day_post_acc
            dow               = pd.Timestamp(date).day_name()
            ei_lo, ei_hi      = (20, 30) if dow == 'Sunday' else (25, 40)
            if journeys_to_ei < ei_lo:
                ei_status = f'⚠ BELOW range ({ei_lo}–{ei_hi}) — over-reserved'
            elif journeys_to_ei > ei_hi:
                ei_status = f'⚠ ABOVE range ({ei_lo}–{ei_hi}) — under-reserved'
            else:
                ei_status = f'✓ within range ({ei_lo}–{ei_hi})'
            print(f"\n  EI Vetting ({dow}): {day_total_pred:.1f} pred jobs ÷ 5.5 = {expected_journeys:.1f} expected journeys  |  "
                  f"Post-acceptance TPs: {int(day_post_acc)}  |  "
                  f"→ Journeys to EI: {journeys_to_ei:.1f}  {ei_status}")

        print(f"\n{'─'*110}")
        print(f"  Total new recommendations : {int(total_new)}")
        print(f"  Output saved to           : {output_path}")

    else:
        print("\n  No gaps found — no new recommendations needed.")

    # ── Print recommended TPs ─────────────────────────────────────────────────
    recommended = res[res['new_recommendation'] == True].sort_values(
        ['DATE', 'sourcezone', 'new_recommendation_rank']
    )

    # Build unfilled lookup: (date, zone_base) → unfilled count
    unfill_lookup = {}
    if accepted_summary:
        for s in accepted_summary:
            zone_base = s['Zone'].replace('▲', '').replace('[TIGHT]', '').strip().lower()
            unfill_lookup[(s['Pickup Date'], zone_base)] = s.get('Unfilled Gap', 0)

    def _tp_row(r):
        vat       = 'Y' if int(r.get('VAT_STATUS') or 0) == 1 else 'N'
        deallo    = float(r.get('Deallo Rate Overall') or 0)
        hi_deallo = '  ⚠ high deallo' if deallo > 0.20 else ''
        type_abbr = {'local': 'Loc', 'national': 'Nat', 'custom': 'Cus'}.get(
                        str(r['RES_TYPE']).lower(), str(r['RES_TYPE']))
        return (f"  {str(r['USERNAME']):<18} {str(r['NUMBER_OF_MEN']):>3}  {type_abbr:<4}  "
                f"{float(r.get('rating') or 0):>6.2f}  {deallo:>5.0%}  {vat:>3}{hi_deallo}")

    def _tp_header():
        return f"  {'Username':<18} {'Men':>3}  {'Type':<4}  {'Rating':>6}  {'Deallo':>6}  {'VAT':>3}"

    if not recommended.empty:
        for date_val, date_recs in recommended.groupby(recommended['DATE'].dt.date):
            total_date = len(date_recs)
            dow = pd.Timestamp(date_val).day_name()
            print(f"\n{'=' * 80}")
            print(f"  RECOMMENDED TPs  —  {total_date} new acceptances  |  {date_val} ({dow})")
            print(f"{'=' * 80}")

            for zone_val, zone_recs in date_recs.groupby('sourcezone'):
                zone_recs = zone_recs.sort_values('new_recommendation_rank')
                n         = len(zone_recs)
                unfill    = unfill_lookup.get((date_val, zone_val), 0)
                unfill_str = f'  ({unfill} slot{"s" if unfill > 1 else ""} unfilled ⚠)' if unfill > 0 else ''
                print(f"\n  {zone_val.upper()} — accept {n}{unfill_str}")
                print(f"  {'─' * 55}")

                if zone_val == 'london':
                    men_series  = zone_recs['NUMBER_OF_MEN'].apply(lambda m: int(m) if pd.notna(m) else 1)
                    type_series = zone_recs['RES_TYPE'].str.lower()
                    one_12      = zone_recs[men_series.isin([1, 12])]
                    two_nat     = zone_recs[men_series.isin([2]) & (type_series == 'national')]
                    two_loc     = zone_recs[men_series.isin([2]) & (type_series == 'local')]
                    if not one_12.empty:
                        print(f"\n  1-man & 12-man  ({len(one_12)})")
                        print(_tp_header())
                        for _, r in one_12.iterrows():
                            print(_tp_row(r))
                    if not two_nat.empty:
                        print(f"\n  2-man National  ({len(two_nat)})")
                        print(_tp_header())
                        for _, r in two_nat.iterrows():
                            print(_tp_row(r))
                    if not two_loc.empty:
                        print(f"\n  2-man Local  ({len(two_loc)})")
                        print(_tp_header())
                        for _, r in two_loc.iterrows():
                            print(_tp_row(r))
                else:
                    print(_tp_header())
                    for _, r in zone_recs.iterrows():
                        print(_tp_row(r))

    # ── FLAGS ─────────────────────────────────────────────────────────────────
    if accepted_summary:
        print(f"\n{'=' * 100}")
        print("  FLAGS")
        print(f"{'=' * 100}")

        OVERFLOW_ROUTES = {
            'birmingham':        ['oxford', 'peterborough'],
            'manchester':        ['sheffield', 'north wales', 'lake district'],
            'london':            ['oxford', 'peterborough', 'kent', 'brighton'],
            'brighton':          ['salisbury', 'kent'],
            'peterborough':      ['oxford', 'norwich'],
            'edinburgh-glasgow': ['north lake district', 'newcastle'],
        }

        for date, grp in summary_df.groupby('Pickup Date'):
            date_flags = []
            date_rec   = recommended[recommended['DATE'].dt.date == date] if not recommended.empty else pd.DataFrame()

            # Flag 1 — Unfilled slots
            for _, r in grp[grp['Unfilled Gap'] > 0].iterrows():
                date_flags.append(f"⚠ {r['Zone']}: {int(r['Unfilled Gap'])} slot(s) unfilled — not enough qualifying pending TPs after scoring and hard filters")

            # Flag 2 — London 1-man shortfall (recomputed correctly after all acceptances)
            ldn_row = grp[grp['Zone'].str.startswith('london')]
            if not ldn_row.empty:
                ldn = ldn_row.iloc[0]
                one_man_target_flag = round(float(ldn.get('One Man Jobs', 0)) * LONDON_1MAN_FACTOR / LONDON_1MAN_JPJ)
                ldn_accepted = res[
                    (res['DATE'].dt.date == date) &
                    (res['sourcezone'] == 'london') &
                    (res['IRES_STATUS'] == 'accepted')
                ]
                ldn_new_rec = date_rec[date_rec['sourcezone'] == 'london'] if not date_rec.empty else pd.DataFrame()
                def is_1man_capable(m):
                    try: return int(m) in [1, 12]
                    except: return False
                acc_1man   = ldn_accepted['NUMBER_OF_MEN'].apply(is_1man_capable).sum()
                new_1man   = ldn_new_rec['NUMBER_OF_MEN'].apply(is_1man_capable).sum() if not ldn_new_rec.empty else 0
                total_1man = acc_1man + new_1man
                shortfall  = max(0, one_man_target_flag - total_1man)
                if shortfall > 0:
                    date_flags.append(
                        f"⚠ London 1-man shortfall: need {one_man_target_flag}, will have {int(total_1man)} after all acceptances (shortfall={shortfall})\n"
                        f"    → 1-man capable breakdown: {int(acc_1man)} already accepted + {int(new_1man)} newly recommended"
                    )

            # Flag 3 — High deallocation rate (> 20%)
            if not date_rec.empty:
                for _, r in date_rec.iterrows():
                    d = float(r.get('Deallo Rate Overall') or 0)
                    if d > 0.20:
                        date_flags.append(f"⚠ {r['USERNAME']} ({r['sourcezone']}): high deallo rate ({d:.0%}) — limited alternatives available")

            # Flag 4 — Rating below soft threshold (4.4–4.5)
            if not date_rec.empty:
                for _, r in date_rec.iterrows():
                    rating = float(r.get('rating') or 0)
                    if 4.4 <= rating < 4.5:
                        date_flags.append(f"⚠ {r['USERNAME']} ({r['sourcezone']}): rating {rating:.2f} — below 4.5 soft threshold")

            # Flag 5 — London South quota short
            if not date_rec.empty:
                ldn_target_row = grp[grp['Zone'].str.startswith('london')]
                if not ldn_target_row.empty:
                    ldn_target   = int(ldn_target_row.iloc[0]['Target Res'])
                    south_quota  = round(ldn_target * LONDON_SOUTH_QUOTA_PCT)
                    SOUTH_PREFIXES = ('SE', 'SW', 'KT', 'CR', 'TW', 'BR', 'SM')
                    ldn_new = date_rec[date_rec['sourcezone'] == 'london']
                    south_filled = ldn_new['START_POSTCODE'].apply(
                        lambda p: str(p).strip().upper()[:2] in SOUTH_PREFIXES if pd.notna(p) else False
                    ).sum() if not ldn_new.empty else 0
                    if south_filled < south_quota:
                        date_flags.append(f"⚠ London South quota short: {int(south_filled)} of {south_quota} South London slots filled")

            # Flag 6 — TIGHT zones
            REDUCED_COV_ZONES = {
                'birmingham': '50%', 'manchester': '75%', 'peterborough': '50%',
                'oxford': '50%', 'salisbury': '75%', 'sheffield': '75%',
            }
            for _, r in grp[grp['Zone'].str.contains(r'\[TIGHT\]')].iterrows():
                base_zone  = r['Zone'].replace(' [TIGHT]', '')
                normal_cov = REDUCED_COV_ZONES.get(base_zone)
                if normal_cov:
                    date_flags.append(f"⚠ {r['Zone']}: supply thin — switched to 100% coverage (normally {normal_cov})")
                else:
                    date_flags.append(f"⚠ {r['Zone']}: supply thin — total available TPs below full journey target")

            # Flag 7 — Unused overflow (only for zones with a configured overflow route)
            zones_with_credit = set(grp[grp['Ovfl Credit'] > 0]['Zone'].str.lower().str.replace(r'\s*\[.*?\]', '', regex=True).str.strip())
            for _, r in grp[grp['TP Excess'] > 0].iterrows():
                src_zone_clean = r['Zone'].lower().replace(' [tight]', '').replace(' [oversup]', '').strip()
                downstream     = OVERFLOW_ROUTES.get(src_zone_clean, [])
                if not downstream:
                    continue  # zone has no configured overflow route — excess is expected, not a flag
                absorbed = any(d in zones_with_credit for d in downstream)
                if not absorbed:
                    date_flags.append(f"⚠ {r['Zone']}: +{int(r['TP Excess'])} excess TPs but no downstream zone absorbed — credit unused")

            if date_flags:
                print(f"\n  {date}:")
                for f in date_flags:
                    for line in f.split('\n'):
                        print(f"    {line}")
            else:
                print(f"\n  {date}: no flags")

    print(f"\n{'=' * 100}\n")
    return res, summary_df if accepted_summary else pd.DataFrame()


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    args = sys.argv[1:]

    # Detect if first arg looks like a date (YYYY-MM-DD) rather than a path
    run_date_arg     = None
    forecast_dir_arg = None
    res_path_arg     = None
    output_path_arg  = None

    for arg in args:
        if re.match(r'^\d{4}-\d{2}-\d{2}$', arg):
            run_date_arg = arg
        elif res_path_arg is None and os.path.isfile(arg):
            res_path_arg = arg
        elif forecast_dir_arg is None and os.path.isdir(arg):
            forecast_dir_arg = arg
        elif res_path_arg is not None:
            output_path_arg = arg

    # Defaults
    if forecast_dir_arg is None:
        forecast_dir_arg = os.path.normpath(DEFAULT_FORECAST_DIR)

    if res_path_arg is None:
        # Pick most recent recommended_reservations_*.csv in script dir
        pattern = os.path.join(BASE_DIR, 'recommended_reservations_*.csv')
        matches = sorted(glob.glob(pattern))
        if not matches:
            print("ERROR: No reservations file found. Pass path as argument.")
            sys.exit(1)
        res_path_arg = matches[-1]
        print(f"  [auto] Using reservations: {os.path.basename(res_path_arg)}")

    run(forecast_dir_arg, res_path_arg, output_path_arg, run_date_arg)
