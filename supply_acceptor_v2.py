"""
Supply Acceptor v2 — Empirical JPJ Targeting
=============================================
Replaces the fixed JPJ + coverage approach with per-zone empirical parameters
derived from 30 days of anyroute S3 data (jpj_parameters.csv).

Key changes from v1:
  1. Separate 1-man and 2-man TP targets per zone using:
       retention_1m, retention_2m, pct_1m_1j, pct_1m_2j, jpj_1m, jpj_2m
  2. Dynamic EI JPJ from a fitted linear model on day-level job mix
  3. Two-pass TP selection — 1M pool (NUMBER_OF_MEN 1/12) then 2M pool (2/12)
  4. Zones missing from jpj_parameters.csv fall back to v1 single-JPJ logic

Zone target formula (before coverage):
  eff_1m     = furn_1m_jobs × retention_1m
  eff_2m     = furn_2m_jobs × retention_2m
  eff_rem    = rem_jobs     × retention          (overall retention used for removals)
  target_1m  = floor(eff_1m × pct_1m_1j / jpj_1m)
  target_2m  = round((eff_1m × pct_1m_2j + eff_2m + eff_rem) / jpj_2m)
  total_raw  = target_1m + target_2m
  target     = round(total_raw × coverage)

EI JPJ formula (fitted on 30-day all-vehicle-type data):
  Non-Sunday: JPJ = 5.2435 + 0.0023·x − 0.0007·y − 0.0171·z
  Sunday:     JPJ = 5.6584 + 0.0023·x − 0.0007·y − 0.0171·z
  x = total 1M-req jobs, y = total 2M-req jobs, z = total removal jobs

Usage:
    python3 supply_acceptor_v2.py               (reads same demand/res files as v1)
    python3 supply_acceptor_v2.py demand.csv reservations.csv
"""

import sys
import os
import math
import random
import pandas as pd
import numpy as np
from datetime import datetime
try:
    from tabulate import tabulate as _tabulate
    _HAS_TABULATE = True
except ImportError:
    _HAS_TABULATE = False

sys.stdout.reconfigure(line_buffering=True)

# ── Shared helpers — import from v1 ──────────────────────────────────────────
# score_tp, diversified_sample, select_candidates, london_* are unchanged.
# We import them rather than duplicating.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'legacy'))
from supply_acceptor import (
    score_tp, versatility_rank, diversified_sample, select_candidates,
    is_south_london, london_select_candidates, london_calc_target,
    dynamic_coverage,
    DENSE_ZONES, DENSE_JPJ, LIGHT_JPJ, ZONE_JPJ_OVERRIDES,
    ZONE_COVERAGE_OVERRIDES, ZONE_OVERFLOW_TARGETS,
    ZONE_MIN_JOBS, MIN_RATING, MIN_CAPACITY,
    LONDON_1MAN_FACTOR, LONDON_1MAN_JPJ,
    LONDON_OVERSUPPLY_RATIO, LONDON_OVERSUPPLY_JPJ, LONDON_OVERSUPPLY_BUFFER,
    LONDON_SOUTH_QUOTA_PCT,
)

# ── File paths ─────────────────────────────────────────────────────────────────
BASE_DIR          = os.path.dirname(os.path.abspath(__file__))
JPJ_PARAMS_FILE   = os.path.join(BASE_DIR, 'jpj_parameters.csv')
DEMAND_FILE       = os.path.join(BASE_DIR, 'FURN_Supply_Summary - Detailed view (2).csv')
RES_FILE          = os.path.join(BASE_DIR, 'FURN_Supply_Summary - reservations (10).csv')

# ── EI JPJ linear model coefficients (fitted on 30-day all-vehicle data) ──────
EI_JPJ_B0           = 5.2435
EI_JPJ_B1           = 0.0023    # per 1M-req job
EI_JPJ_B2           = -0.0007   # per 2M-req job
EI_JPJ_B3           = -0.0171   # per removal job
EI_JPJ_SUNDAY_DELTA = 0.4149    # Sunday intercept uplift

# ── Minimum 1-man jobs before accepting a 1-man TP (unchanged from v1) ────────
MIN_1MAN_JOBS_FOR_1MAN_TP = 9


# ── Load per-zone empirical parameters ────────────────────────────────────────

def load_jpj_params(path: str = JPJ_PARAMS_FILE) -> dict:
    """
    Returns a dict: zone → {retention, retention_1m, retention_2m,
                             pct_1m_1j, pct_1m_2j, jpj_1m, jpj_2m, jpj_ovrl}
    Zones with missing/NaN jpj_1m or jpj_2m are excluded (fall back to v1).
    """
    df = pd.read_csv(path)
    params = {}
    for _, row in df.iterrows():
        zone = str(row['zone']).strip().lower()
        # Skip zones with insufficient data
        if pd.isna(row.get('jpj_1m')) or pd.isna(row.get('jpj_2m')):
            continue
        params[zone] = {
            'retention':    float(row.get('retention', 1.0)  or 1.0),
            'retention_1m': float(row.get('retention_1m', 1.0) or 1.0),
            'retention_2m': float(row.get('retention_2m', 1.0) or 1.0),
            'pct_1m_1j':   float(row.get('pct_1m_1j', 0.7) or 0.7),
            'pct_1m_2j':   float(row.get('pct_1m_2j', 0.3) or 0.3),
            'jpj_1m':      float(row['jpj_1m']),
            'jpj_2m':      float(row['jpj_2m']),
            'jpj_ovrl':    float(row.get('jpj_ovrl', 5.5) or 5.5),
        }
    return params


# ── v2 zone target calculation ─────────────────────────────────────────────────

def calc_zone_targets_v2(zone: str, furn_1m: float, furn_2m: float,
                          rem: float, params: dict,
                          jpj_multiplier: float = 1.0) -> dict:
    """
    Compute raw (pre-coverage) 1M and 2M TP targets for a zone using
    empirical JPJ parameters.

    jpj_multiplier: scale factor applied to jpj_1m, jpj_2m and jpj_ovrl before
        computing targets.  Defaults to 1.0; only override for special scenarios.

    Returns a dict:
        target_1m, target_2m, total_raw,
        eff_1m, eff_2m, eff_rem,
        used_v2  (True if empirical params found, False if fell back to v1)
    """
    p = params.get(zone)
    total_furn = furn_1m + furn_2m
    total_jobs = total_furn + rem

    if p is None or total_jobs == 0:
        # Fallback: v1 single-JPJ logic
        jpj = ZONE_JPJ_OVERRIDES.get(zone, DENSE_JPJ if zone in DENSE_ZONES else LIGHT_JPJ)
        jpj *= jpj_multiplier
        raw_total = total_jobs / jpj if jpj > 0 else 0
        return {
            'target_1m': 0, 'target_2m': round(raw_total),
            'total_raw': raw_total,
            'eff_1m': 0, 'eff_2m': 0, 'eff_rem': 0,
            'used_v2': False,
        }

    eff_1m  = furn_1m * p['retention_1m']
    eff_2m  = furn_2m * p['retention_2m']
    eff_rem = rem     * p['retention']

    jpj_1m   = p['jpj_1m']   * jpj_multiplier
    jpj_2m   = p['jpj_2m']   * jpj_multiplier
    jpj_ovrl = p['jpj_ovrl'] * jpj_multiplier

    target_1m = math.floor(eff_1m * p['pct_1m_1j'] / jpj_1m) if jpj_1m > 0 else 0
    # Jobs from the 1M->1J pathway not covered by accepted 1M TPs spill onto 2M vehicles
    residual_1m = max(0.0, eff_1m * p['pct_1m_1j'] - target_1m * jpj_1m)
    load_2m   = residual_1m + eff_1m * p['pct_1m_2j'] + eff_2m + eff_rem
    target_2m = round(load_2m / jpj_2m) if jpj_2m > 0 else 0
    total_raw = (eff_1m + eff_2m + eff_rem) / jpj_ovrl if jpj_ovrl > 0 else 0

    return {
        'target_1m': max(0, target_1m),
        'target_2m': max(0, target_2m),
        'total_raw': total_raw,
        'eff_1m': eff_1m, 'eff_2m': eff_2m, 'eff_rem': eff_rem,
        'used_v2': True,
    }


# ── Dynamic EI JPJ ─────────────────────────────────────────────────────────────

def predicted_ei_jpj(furn_1m: float, furn_2m: float, rem: float,
                     pickup_date) -> float:
    """
    Predict JPJ for EI vetting using the fitted linear model.
    pickup_date can be a datetime.date or pd.Timestamp.
    """
    is_sunday = pd.Timestamp(pickup_date).day_name() == 'Sunday'
    jpj = (EI_JPJ_B0
           + EI_JPJ_B1 * furn_1m
           + EI_JPJ_B2 * furn_2m
           + EI_JPJ_B3 * rem
           + (EI_JPJ_SUNDAY_DELTA if is_sunday else 0.0))
    return max(4.0, jpj)   # floor at 4.0 to avoid unrealistic values


# ── Effective target for forecast-mode conservative acceptance ────────────────

def effective_tgt(tgt: int, pend: int) -> int:
    """
    Conservative acceptance target used in forecast mode.

    Tgt=1               → eTgt=1  (never reduce below 1)
    Tgt=2, pend >= tgt  → eTgt=1  (enough supply available, halve the target)
    Tgt=3, pend >= tgt  → eTgt=2  (reduce by 1 when supply is sufficient)
    Otherwise           → eTgt=max(1, round(Tgt × 0.75))
    """
    if tgt <= 1:
        return tgt
    if tgt == 2 and pend >= tgt:
        return 1
    if tgt == 3 and pend >= tgt:
        return 2
    return max(1, round(tgt * 0.75))


# ── v2 TP selection (two-pass: 1M then 2M) ────────────────────────────────────

def select_v2(candidates: pd.DataFrame, gap_1m: int, gap_2m: int,
              accepted_usernames: set, one_man_jobs: float,
              south_quota_pct: float = 0.0) -> pd.DataFrame:
    """
    Two-pass selection with optional per-bucket south London quota.

      Pass 1 — 1M pool {NUMBER_OF_MEN == 1 or 12}:
                 if south_quota_pct > 0, first pick round(gap_1m × pct) from
                 south London 1M TPs, then fill remainder from general 1M pool.
      Pass 2 — 2M pool {NUMBER_OF_MEN == 2 or 12}:
                 if south_quota_pct > 0, first pick round(gap_2m × pct) from
                 south London 2M TPs, then fill remainder from general 2M pool.

    The south quota is a soft target — if fewer south TPs are available, whatever
    is available is taken and the remainder is filled from the general pool.
    south_quota_pct is only passed for London (value: LONDON_SOUTH_QUOTA_PCT).
    """
    if candidates.empty or (gap_1m + gap_2m) <= 0:
        return candidates.iloc[0:0]

    selected   = []
    used_idx   = set()
    used_names = set(accepted_usernames)

    def _men(row):
        try: return int(row['NUMBER_OF_MEN'])
        except: return 1

    def _pick(pool, n):
        """Pick up to n from pool, excluding used indices and preferring unused usernames."""
        pool = pool[~pool.index.isin(used_idx)]
        fresh = pool[~pool['USERNAME'].isin(used_names)]
        stale = pool[ pool['USERNAME'].isin(used_names)]
        pool  = pd.concat([fresh, stale])
        sel   = diversified_sample(pool, n)
        return sel

    # Pass 1 — 1M pool
    if gap_1m > 0 and one_man_jobs > MIN_1MAN_JOBS_FOR_1MAN_TP:
        pool_1m = candidates[candidates.apply(lambda r: _men(r) in (1, 12), axis=1)]

        if south_quota_pct > 0:
            south_1m_need = round(gap_1m * south_quota_pct)
            south_1m_pool = pool_1m[pool_1m['START_POSTCODE'].apply(is_south_london)]
            sel_south_1m  = _pick(south_1m_pool, south_1m_need)
            selected.append(sel_south_1m)
            used_idx   |= set(sel_south_1m.index)
            used_names |= set(sel_south_1m['USERNAME'])

        remaining_1m = gap_1m - sum(len(s) for s in selected)
        if remaining_1m > 0:
            sel_1m = _pick(pool_1m, remaining_1m)
            selected.append(sel_1m)
            used_idx   |= set(sel_1m.index)
            used_names |= set(sel_1m['USERNAME'])

    # Pass 2 — 2M pool (includes 12-man if not already used in pass 1)
    # remaining_2m only deducts TPs selected within pass 2 (south quota pre-picks).
    # Pass 1 picks (even 12-man) filled the 1M gap and must not reduce the 2M count.
    if gap_2m > 0:
        pool_2m = candidates[candidates.apply(lambda r: _men(r) in (2, 12), axis=1)]
        pass2_selected = 0

        if south_quota_pct > 0:
            south_2m_need = round(gap_2m * south_quota_pct)
            south_2m_pool = pool_2m[pool_2m['START_POSTCODE'].apply(is_south_london)]
            sel_south_2m  = _pick(south_2m_pool, south_2m_need)
            selected.append(sel_south_2m)
            used_idx   |= set(sel_south_2m.index)
            used_names |= set(sel_south_2m['USERNAME'])
            pass2_selected += len(sel_south_2m)

        remaining_2m = max(0, gap_2m - pass2_selected)
        if remaining_2m > 0:
            sel_2m = _pick(pool_2m, remaining_2m)
            selected.append(sel_2m)

    non_empty = [s for s in selected if not s.empty]
    return pd.concat(non_empty) if non_empty else candidates.iloc[0:0]


# ── Main algorithm ─────────────────────────────────────────────────────────────

def run(demand_path: str, res_path: str, output_path: str = None,
        jpj_multiplier: float = 1.0, use_effective_targets: bool = False):
    # ── Load ──────────────────────────────────────────────────────────────────
    demand = pd.read_csv(demand_path)
    res    = pd.read_csv(res_path, low_memory=False)
    res    = res.loc[:, ~res.columns.str.startswith('Unnamed')]

    demand.columns = [c.strip().lower().replace(' ', '_') for c in demand.columns]
    demand['pickup_day'] = pd.to_datetime(demand['pickup_day'])
    demand['sourcezone'] = demand['sourcezone'].str.strip().str.lower()

    res['sourcezone']  = res['sourcezone'].str.strip().str.lower()
    res['DATE']        = pd.to_datetime(res['DATE'], dayfirst=True, format='mixed')
    res['IRES_STATUS'] = res['IRES_STATUS'].str.strip().str.lower()
    res['RES_TYPE']    = res['RES_TYPE'].str.strip().str.lower()

    # ── Load empirical JPJ parameters ─────────────────────────────────────────
    if not os.path.exists(JPJ_PARAMS_FILE):
        print(f"[v2] WARNING: {JPJ_PARAMS_FILE} not found — falling back to v1 JPJ for all zones")
        jpj_params = {}
    else:
        jpj_params = load_jpj_params()
        mult_note = f"  (×{jpj_multiplier:.2f} forecast multiplier)" if jpj_multiplier != 1.0 else ""
        print(f"[v2] Loaded empirical JPJ params for {len(jpj_params)} zones{mult_note}")

    # ── Step 1: Build per-zone demand breakdown ────────────────────────────────
    # Demand CSV uses a 'category' column ('furniture' / 'removals') to distinguish
    # job types. Both are 2-man capable; removals use overall retention in targeting.
    has_category   = 'category' in demand.columns
    has_confirmed  = 'confirmed_routable_jobs' in demand.columns
    has_conf_total_rem = 'confirmed_total_rem_jobs' in demand.columns

    def job_sum_furn(men_val):
        if has_category:
            mask = (demand['number_of_men'] == men_val) & (demand['category'] == 'furniture')
        else:
            mask = demand['number_of_men'] == men_val
        cols = ['realized_lane_level_jobs']
        if has_confirmed:
            cols.append('confirmed_routable_jobs')
        agg = demand[mask].groupby(['sourcezone', 'pickup_day'])[cols].sum().reset_index()
        agg = agg.rename(columns={'realized_lane_level_jobs': f'jobs_{men_val}m'})
        if has_confirmed:
            agg = agg.rename(columns={'confirmed_routable_jobs': f'conf_{men_val}m'})
        return agg

    def job_sum_rem():
        if has_category:
            mask = demand['category'] == 'removals'
        else:
            return pd.DataFrame(columns=['sourcezone', 'pickup_day', 'jobs_rem'])
        cols = ['realized_lane_level_jobs']
        if has_confirmed:
            cols.append('confirmed_routable_jobs')
        if has_conf_total_rem:
            cols.append('confirmed_total_rem_jobs')
        agg = demand[mask].groupby(['sourcezone', 'pickup_day'])[cols].sum().reset_index()
        agg = agg.rename(columns={'realized_lane_level_jobs': 'jobs_rem'})
        if has_confirmed:
            agg = agg.rename(columns={'confirmed_routable_jobs': 'conf_rem'})
        if has_conf_total_rem:
            agg = agg.rename(columns={'confirmed_total_rem_jobs': 'conf_rem_total'})
        return agg

    furn_1m_df = job_sum_furn(1)
    furn_2m_df = job_sum_furn(2)
    rem_df     = job_sum_rem()

    # Merge into one row per zone+date
    zone_day = furn_1m_df.merge(furn_2m_df, on=['sourcezone', 'pickup_day'], how='outer') \
                         .merge(rem_df,     on=['sourcezone', 'pickup_day'], how='outer')
    zone_day = zone_day.fillna(0)
    zone_day['realized_jobs'] = zone_day['jobs_1m'] + zone_day['jobs_2m'] + zone_day['jobs_rem']
    zone_day['one_man_ratio'] = zone_day['jobs_1m'] / zone_day['realized_jobs'].replace(0, 1)
    if has_confirmed:
        zone_day['conf_total'] = zone_day['conf_1m'] + zone_day['conf_2m'] + zone_day['conf_rem']
    if has_conf_total_rem:
        zone_day['conf_rem_total'] = zone_day['conf_rem_total'].fillna(0)

    # Add zones that have reservations but no demand
    pickup_dates = zone_day['pickup_day'].unique()
    res_zones    = set(res[res['DATE'].isin(pickup_dates)]['sourcezone'].unique())
    demand_pairs = set(zip(zone_day['sourcezone'], zone_day['pickup_day']))
    extra_base = {'jobs_1m': 0, 'jobs_2m': 0, 'jobs_rem': 0, 'realized_jobs': 0, 'one_man_ratio': 0}
    if has_confirmed:
        extra_base.update({'conf_1m': 0, 'conf_2m': 0, 'conf_rem': 0, 'conf_total': 0})
    if has_conf_total_rem:
        extra_base['conf_rem_total'] = 0
    extra = [
        dict(sourcezone=z, pickup_day=d, **extra_base)
        for d in pickup_dates for z in res_zones if (z, d) not in demand_pairs
    ]
    if extra:
        zone_day = pd.concat([zone_day, pd.DataFrame(extra)], ignore_index=True)

    # ── Step 2: Compute v2 targets ────────────────────────────────────────────
    # No coverage multiplier — empirical retention + JPJ parameters already
    # encode the fraction of demand that gets routed in this zone.
    def apply_v2_targets(row):
        zone = row['sourcezone']
        t    = calc_zone_targets_v2(zone, row['jobs_1m'], row['jobs_2m'], row['jobs_rem'], jpj_params, jpj_multiplier)
        return pd.Series({
            'target_1m':           t['target_1m'],
            'target_2m':           t['target_2m'],
            'target_reservations': t['target_1m'] + t['target_2m'],
            'total_raw_journeys':  t['total_raw'],
            'eff_1m': t['eff_1m'], 'eff_2m': t['eff_2m'], 'eff_rem': t['eff_rem'],
            'used_v2': t['used_v2'],
        })

    v2_cols = zone_day.apply(apply_v2_targets, axis=1)
    zone_day = pd.concat([zone_day, v2_cols], axis=1)

    # ── Actuals-based targets (for forecast mode: floor for eTgt) ─────────────
    # Compute the same targets using confirmed job counts.  In forecast mode
    # this acts as a floor: if confirmed demand already justifies a higher target
    # than the conservative eTgt, the eTgt is bumped up to match.
    # In actuals mode conf_* == jobs_* so atgt == tgt (no effect).
    def apply_actuals_targets(row):
        zone = row['sourcezone']
        c1m  = float(row['conf_1m'])  if 'conf_1m'  in row.index and pd.notna(row['conf_1m'])  else float(row['jobs_1m'])
        c2m  = float(row['conf_2m'])  if 'conf_2m'  in row.index and pd.notna(row['conf_2m'])  else float(row['jobs_2m'])
        crem = float(row['conf_rem']) if 'conf_rem' in row.index and pd.notna(row['conf_rem']) else float(row['jobs_rem'])
        t = calc_zone_targets_v2(zone, c1m, c2m, crem, jpj_params, jpj_multiplier)
        return pd.Series({'atgt_1m': t['target_1m'], 'atgt_2m': t['target_2m']})

    atgt_cols = zone_day.apply(apply_actuals_targets, axis=1)
    zone_day = pd.concat([zone_day, atgt_cols], axis=1)

    # ── Step 3: Accepted / pending counts (split by men type) ─────────────────
    non_return = res[res['RES_TYPE'] != 'return']

    def men_val(df):
        return df['NUMBER_OF_MEN'].apply(lambda m: int(m) if pd.notna(m) else 1)

    accepted_all = res[res['IRES_STATUS'] == 'accepted']
    acc_counts   = accepted_all.groupby(['sourcezone', 'DATE']).size().reset_index(name='accepted_count')
    acc_1m       = (accepted_all[men_val(accepted_all) == 1]
                    .groupby(['sourcezone', 'DATE']).size().reset_index(name='accepted_1m'))
    acc_2m       = (accepted_all[men_val(accepted_all) == 2]
                    .groupby(['sourcezone', 'DATE']).size().reset_index(name='accepted_2m'))
    acc_12m      = (accepted_all[men_val(accepted_all) == 12]
                    .groupby(['sourcezone', 'DATE']).size().reset_index(name='accepted_12m'))

    pend_counts  = (non_return[non_return['IRES_STATUS'] == 'pending']
                    .groupby(['sourcezone', 'DATE']).size().reset_index(name='pending_count'))

    for df, l_col, r_col in [
        (acc_counts, 'pickup_day', 'DATE'),
        (acc_1m,     'pickup_day', 'DATE'),
        (acc_2m,     'pickup_day', 'DATE'),
        (acc_12m,    'pickup_day', 'DATE'),
        (pend_counts,'pickup_day', 'DATE'),
    ]:
        zone_day = zone_day.merge(df, left_on=['sourcezone', 'pickup_day'],
                                  right_on=['sourcezone', r_col], how='left').drop(columns=[r_col], errors='ignore')

    for col in ['accepted_count', 'accepted_1m', 'accepted_2m', 'accepted_12m', 'pending_count']:
        zone_day[col] = zone_day[col].fillna(0).astype(int)

    # ── Redistribute accepted 12M TPs across 1M / 2M buckets ─────────────────
    # 12M TPs are flexible — they can serve both 1M and 2M jobs. Assign each
    # 12M TP to whichever bucket has the larger raw gap (prefer 2M on a tie,
    # since 12M vans are more naturally a 2M/removals resource).
    raw_g1m = (zone_day['target_1m'] - zone_day['accepted_1m']).clip(lower=0)
    raw_g2m = (zone_day['target_2m'] - zone_day['accepted_2m']).clip(lower=0)
    to_1m   = pd.Series(0, index=zone_day.index, dtype=int)
    to_2m   = pd.Series(0, index=zone_day.index, dtype=int)
    max_12m = int(zone_day['accepted_12m'].max()) if len(zone_day) > 0 else 0
    for _ in range(max_12m):
        has_remaining = zone_day['accepted_12m'] > (to_1m + to_2m)
        curr_g1m      = (raw_g1m - to_1m).clip(lower=0)
        curr_g2m      = (raw_g2m - to_2m).clip(lower=0)
        goes_to_2m    = has_remaining & (curr_g2m >= curr_g1m)
        goes_to_1m    = has_remaining & (curr_g2m < curr_g1m)
        to_2m         = to_2m + goes_to_2m.astype(int)
        to_1m         = to_1m + goes_to_1m.astype(int)
    zone_day['acc_12m_to_1m'] = to_1m
    zone_day['acc_12m_to_2m'] = to_2m

    zone_day['total_available'] = zone_day['accepted_count'] + zone_day['pending_count']

    # ── Step 4: Gap calculation ────────────────────────────────────────────────
    zone_day['gap_1m'] = (zone_day['target_1m'] - zone_day['accepted_1m'] - zone_day['acc_12m_to_1m']).clip(lower=0)
    zone_day['gap_2m'] = (zone_day['target_2m'] - zone_day['accepted_2m'] - zone_day['acc_12m_to_2m']).clip(lower=0)
    # Total gap is sum of per-bucket gaps so cross-bucket imbalance is captured correctly
    # (e.g. over-accepted on 1M but under on 2M still shows the real 2M shortfall)
    zone_day['gap']   = zone_day['gap_1m'] + zone_day['gap_2m']

    zone_day['is_tight']    = False
    zone_day['london_mode'] = ''
    zone_day = zone_day.reset_index(drop=True)

    # ── Step 5: Overflow credits ───────────────────────────────────────────────
    # Excess is per-bucket — no threshold multiplier.
    # A zone that over-accepted 1M TPs donates 1M credits; over-accepted 2M donates 2M credits.
    zone_day['excess_1m']     = (zone_day['accepted_1m'] + zone_day['acc_12m_to_1m'] - zone_day['target_1m']).clip(lower=0).astype(int)
    zone_day['excess_2m']     = (zone_day['accepted_2m'] + zone_day['acc_12m_to_2m'] - zone_day['target_2m']).clip(lower=0).astype(int)
    zone_day['tp_excess']     = zone_day['excess_1m'] + zone_day['excess_2m']
    zone_day['overflow_credit'] = 0

    for src_zone, tgt_zones in ZONE_OVERFLOW_TARGETS.items():
        for _, src_row in zone_day[zone_day['sourcezone'] == src_zone].iterrows():
            ovfl_1m = int(src_row['excess_1m'])
            ovfl_2m = int(src_row['excess_2m'])
            if ovfl_1m + ovfl_2m <= 0:
                continue
            d = src_row['pickup_day']
            for tgt_zone in tgt_zones:
                if ovfl_1m + ovfl_2m <= 0:
                    break
                mask = (zone_day['sourcezone'] == tgt_zone) & (zone_day['pickup_day'] == d)
                if not mask.any():
                    continue
                tidx = zone_day[mask].index[0]
                # Apply 1M excess to the receiving zone's 1M gap
                red_1m = min(ovfl_1m, int(zone_day.at[tidx, 'gap_1m']))
                zone_day.at[tidx, 'gap_1m'] -= red_1m
                ovfl_1m -= red_1m
                # Apply 2M excess to the receiving zone's 2M gap
                red_2m = min(ovfl_2m, int(zone_day.at[tidx, 'gap_2m']))
                zone_day.at[tidx, 'gap_2m'] -= red_2m
                ovfl_2m -= red_2m
                # Update total gap and credit tracker
                total_red = red_1m + red_2m
                zone_day.at[tidx, 'gap']             -= total_red
                zone_day.at[tidx, 'overflow_credit'] += total_red

    # ── Step 6: Score pending reservations ────────────────────────────────────
    pending_mask = (res['IRES_STATUS'] == 'pending') & (res['RES_TYPE'] != 'return')
    res.loc[pending_mask, '_score'] = res[pending_mask].apply(score_tp, axis=1)
    res['new_recommendation']      = False
    res['new_recommendation_rank'] = pd.NA
    res['rating_fallback']         = False

    # ── Step 7: Zone-by-zone selection ────────────────────────────────────────
    accepted_summary = []

    for _, zd in zone_day.iterrows():
        zone   = zd['sourcezone']
        date   = zd['pickup_day']
        gap    = int(zd['gap'])
        gap_1m = int(zd['gap_1m'])
        gap_2m = int(zd['gap_2m'])

        newly_accepted = 0
        unfilled       = 0
        to_accept      = pd.DataFrame()   # primary-pass selections (empty until filled)

        # Skip if below minimum jobs threshold
        min_jobs = ZONE_MIN_JOBS.get(zone, 0)
        if min_jobs > 0 and float(zd['realized_jobs']) < min_jobs:
            print(f"  [skip] {zone} {date.date()}: {int(zd['realized_jobs'])} jobs "
                  f"< min {min_jobs} — skipping")
            gap = gap_1m = gap_2m = 0

        # Effective targets (forecast mode only) — conservative acceptance cap
        # gap_1m/gap_2m remain unchanged for shortage/excess reporting
        if use_effective_targets:
            pend = int(zd['pending_count'])
            etgt_1m = effective_tgt(int(zd['target_1m']), pend)
            etgt_2m = effective_tgt(int(zd['target_2m']), pend)
            # Floor: actuals-based target — confirmed demand sets a minimum
            atgt_1m = int(zd['atgt_1m'])
            atgt_2m = int(zd['atgt_2m'])
            etgt_1m = max(etgt_1m, atgt_1m)
            etgt_2m = max(etgt_2m, atgt_2m)
            eff_acc_1m = int(zd['accepted_1m']) + int(zd['acc_12m_to_1m'])
            eff_acc_2m = int(zd['accepted_2m']) + int(zd['acc_12m_to_2m'])
            egap_1m = max(0, min(etgt_1m - eff_acc_1m, gap_1m))
            egap_2m = max(0, min(etgt_2m - eff_acc_2m, gap_2m))
        else:
            etgt_1m = int(zd['target_1m'])
            etgt_2m = int(zd['target_2m'])
            atgt_1m = int(zd['atgt_1m'])
            atgt_2m = int(zd['atgt_2m'])
            egap_1m, egap_2m = gap_1m, gap_2m
        egap = egap_1m + egap_2m

        if egap > 0:
            cand_mask = (
                (res['sourcezone'] == zone) &
                (res['DATE']       == date) &
                pending_mask
            )
            candidates = res.loc[cand_mask].copy()
            candidates = candidates[
                candidates['rating'].apply(lambda r: float(r or 0)) >= MIN_RATING
            ]
            candidates = candidates[
                candidates['RESERVATION_CAPACITY'].apply(lambda c: float(c or 0)) >= MIN_CAPACITY
            ]

            if not candidates.empty:
                accepted_usernames = set(
                    res[
                        (res['sourcezone'] == zone) &
                        (res['DATE'] == date) &
                        ((res['IRES_STATUS'] == 'accepted') | (res['new_recommendation'] == True))
                    ]['USERNAME'].str.strip()
                )
                south_quota_pct = LONDON_SOUTH_QUOTA_PCT if zone == 'london' else 0.0
                to_accept = select_v2(
                    candidates, egap_1m, egap_2m,
                    accepted_usernames,
                    float(zd.get('jobs_1m', 0)),
                    south_quota_pct=south_quota_pct,
                )

                for rank, idx in enumerate(to_accept.index, start=1):
                    res.at[idx, 'new_recommendation']      = True
                    res.at[idx, 'new_recommendation_rank'] = rank
                newly_accepted = min(egap, len(to_accept))
                unfilled       = max(0, egap - len(to_accept))

            # ── Fallback pass: use below-MIN_RATING TPs if slots remain unfilled ──
            if unfilled > 0:
                # Derive remaining 1M/2M gaps from what primary pass filled.
                # 12M TPs in select_v2 go to the larger gap first.
                men_vals = to_accept['NUMBER_OF_MEN'].apply(lambda x: float(x or 0)) if not to_accept.empty else pd.Series([], dtype=float)
                n_1m_sel  = int((men_vals == 1.0).sum())
                n_2m_sel  = int((men_vals == 2.0).sum())
                n_12m_sel = int((men_vals == 12.0).sum())
                if egap_1m >= egap_2m:
                    true_1m_filled = min(egap_1m, n_1m_sel + n_12m_sel)
                    true_2m_filled = min(egap_2m, n_2m_sel)
                else:
                    true_2m_filled = min(egap_2m, n_2m_sel + n_12m_sel)
                    true_1m_filled = min(egap_1m, n_1m_sel)
                rem_gap_1m = max(0, egap_1m - true_1m_filled)
                rem_gap_2m = max(0, egap_2m - true_2m_filled)

                fallback_mask = (
                    (res['sourcezone'] == zone) &
                    (res['DATE']       == date) &
                    pending_mask &
                    (res['new_recommendation'] == False) &
                    (res['rating'].apply(lambda r: float(r or 0)) < MIN_RATING) &
                    (res['RESERVATION_CAPACITY'].apply(lambda c: float(c or 0)) >= MIN_CAPACITY)
                )
                fallback_candidates = res.loc[fallback_mask].copy()

                if not fallback_candidates.empty:
                    accepted_usernames = set(
                        res[
                            (res['sourcezone'] == zone) &
                            (res['DATE'] == date) &
                            ((res['IRES_STATUS'] == 'accepted') | (res['new_recommendation'] == True))
                        ]['USERNAME'].str.strip()
                    )
                    fallback_selected = select_v2(
                        fallback_candidates, rem_gap_1m, rem_gap_2m,
                        accepted_usernames,
                        float(zd.get('jobs_1m', 0)),
                        south_quota_pct=0.0,  # no south quota in fallback pass
                    )
                    start_rank = len(to_accept) + 1
                    for rank, idx in enumerate(fallback_selected.index, start=start_rank):
                        res.at[idx, 'new_recommendation']      = True
                        res.at[idx, 'new_recommendation_rank'] = rank
                        res.at[idx, 'rating_fallback']         = True
                    newly_accepted += len(fallback_selected)
                    unfilled        = max(0, unfilled - len(fallback_selected))
                    if not fallback_selected.empty:
                        print(f"  [fallback] {zone} {date.date()}: {len(fallback_selected)} low-rating TP(s) accepted to fill shortfall")

        zone_label = zone
        if zd.get('london_mode'):
            zone_label = f"{zone} [{zd['london_mode']}]"
        elif bool(zd.get('is_tight')):
            zone_label = f"{zone} [TIGHT]"
        v2_flag = '' if zd.get('used_v2', False) else ' [v1]'

        accepted_summary.append({
            'Zone':             zone_label + v2_flag,
            'Pickup Date':      date.date(),
            'Furn1M Jobs':      int(zd['jobs_1m']),
            'Furn2M Jobs':      int(zd['jobs_2m']),
            'Rem Jobs':         int(zd['jobs_rem']),
            'Total Jobs':       int(zd['realized_jobs']),
            'Conf1M':           int(zd['conf_1m'])       if has_confirmed     else int(zd['jobs_1m']),
            'Conf2M':           int(zd['conf_2m'])       if has_confirmed     else int(zd['jobs_2m']),
            'ConfRem':          int(zd['conf_rem'])      if has_confirmed     else int(zd['jobs_rem']),
            'ConfRemTot':       int(zd['conf_rem_total'])if has_conf_total_rem else 0,
            'ConfTot':          int(zd['conf_total'])    if has_confirmed     else int(zd['realized_jobs']),
            'Tgt1M':            int(zd['target_1m']),
            'Tgt2M':            int(zd['target_2m']),
            'aTgt1M':           atgt_1m,
            'aTgt2M':           atgt_2m,
            'eTgt1M':           etgt_1m,
            'eTgt2M':           etgt_2m,
            'Target':           int(zd['target_reservations']),
            'Accepted':         int(zd['accepted_count']),
            'Acc1M':            int(zd['accepted_1m']),
            'Acc2M':            int(zd['accepted_2m']),
            'Acc12M':           int(zd['accepted_12m']),
            'Acc12M_to_1M':     int(zd['acc_12m_to_1m']),
            'Acc12M_to_2M':     int(zd['acc_12m_to_2m']),
            'Pending':          int(zd['pending_count']),
            'Excess1M':         int(zd['excess_1m']),
            'Excess2M':         int(zd['excess_2m']),
            'TP Excess':        int(zd['tp_excess']),
            'Ovfl Credit':      int(zd['overflow_credit']),
            'Gap':              gap,
            'Gap1M':            gap_1m,
            'Gap2M':            gap_2m,
            'Newly Accepted':   newly_accepted,
            'Unfilled Gap':     unfilled,
        })

    # ── Output ────────────────────────────────────────────────────────────────
    res = res.drop(columns=['_score'], errors='ignore')
    if output_path is None:
        ts = datetime.now().strftime('%Y-%m-%d_%H%M')
        output_path = os.path.join(BASE_DIR, f'supply_acceptor_v2_output_{ts}.csv')
    res.to_csv(output_path, index=False)

    # Save zone-level summary alongside main output (used by vetted recommendations writer)
    if accepted_summary:
        summary_path = output_path.replace('.csv', '_zone_summary.csv')
        pd.DataFrame(accepted_summary).to_csv(summary_path, index=False)

    # ── Print summary ─────────────────────────────────────────────────────────
    print("\n" + "=" * 110)
    print("  SUPPLY ACCEPTOR v2 — RECOMMENDATIONS")
    print(f"  Run time : {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 110)

    if accepted_summary:
        summary_df = pd.DataFrame(accepted_summary).sort_values(['Pickup Date', 'Zone'])
        total_new  = int(res['new_recommendation'].sum())

        for date, grp in summary_df.groupby('Pickup Date'):
            show_conf    = has_confirmed
            show_rem_tot = has_conf_total_rem
            print(f"\n  Pickup: {date}")

            # ── Build column list dynamically ──────────────────────────────
            cols   = ['Zone', '1MJobs', '2MJobs', 'Rem']
            if show_conf:
                cols += ['C1M', 'C2M', 'CRem']
                if show_rem_tot:
                    cols.append('CTRem')
                cols.append('CTot')
            cols += ['Tgt1M', 'Tgt2M']
            if use_effective_targets:
                cols += ['aT1M', 'aT2M', 'eT1M', 'eT2M']
            cols += ['Target', 'Acc', 'A1M', 'A2M', 'A12M', 'Pend',
                     'Excess', 'OvflCrd', 'Gap', 'G1M', 'G2M', 'NewAcc', 'Unfill']

            # ── Build rows ─────────────────────────────────────────────────
            rows = []
            for _, r in grp.iterrows():
                flag = ' ⚠' if r['Unfilled Gap'] > 0 else ''
                row = [r['Zone'] + flag, r['Furn1M Jobs'], r['Furn2M Jobs'], r['Rem Jobs']]
                if show_conf:
                    row += [r['Conf1M'], r['Conf2M'], r['ConfRem']]
                    if show_rem_tot:
                        row.append(r['ConfRemTot'])
                    row.append(r['ConfTot'])
                row += [r['Tgt1M'], r['Tgt2M']]
                if use_effective_targets:
                    row += [r['aTgt1M'], r['aTgt2M'], r['eTgt1M'], r['eTgt2M']]
                row += [r['Target'], r['Accepted'], r['Acc1M'], r['Acc2M'], r['Acc12M'],
                        r['Pending'], r['TP Excess'], r['Ovfl Credit'],
                        r['Gap'], r['Gap1M'], r['Gap2M'], r['Newly Accepted'], r['Unfilled Gap']]
                rows.append(row)

            # ── TOTAL row ──────────────────────────────────────────────────
            day_1m  = int(grp['Furn1M Jobs'].sum())
            day_2m  = int(grp['Furn2M Jobs'].sum())
            day_rem = int(grp['Rem Jobs'].sum())
            day_tot = int(grp['Total Jobs'].sum())
            day_new     = int(grp['Newly Accepted'].sum())
            day_ei_hold = int(grp['EI Hold'].sum()) if 'EI Hold' in grp.columns else 0
            day_acc     = int(grp['Accepted'].sum()) + day_new
            # For EI journey calc: exclude HOLD_EI TPs — they are supply for EI, not pre-accepted
            day_acc_accept = day_acc - day_ei_hold
            tot_row = ['TOTAL', day_1m, day_2m, day_rem]
            if show_conf:
                tot_row += [int(grp['Conf1M'].sum()), int(grp['Conf2M'].sum()),
                            int(grp['ConfRem'].sum())]
                if show_rem_tot:
                    tot_row.append(int(grp['ConfRemTot'].sum()))
                tot_row.append(int(grp['ConfTot'].sum()))
            tot_row += [int(grp['Tgt1M'].sum()), int(grp['Tgt2M'].sum())]
            if use_effective_targets:
                tot_row += [int(grp['aTgt1M'].sum()), int(grp['aTgt2M'].sum()),
                            int(grp['eTgt1M'].sum()), int(grp['eTgt2M'].sum())]
            tot_row += [int(grp['Target'].sum()), int(grp['Accepted'].sum()),
                        int(grp['Acc1M'].sum()), int(grp['Acc2M'].sum()), int(grp['Acc12M'].sum()),
                        int(grp['Pending'].sum()), int(grp['TP Excess'].sum()),
                        int(grp['Ovfl Credit'].sum()), int(grp['Gap'].sum()),
                        int(grp['Gap1M'].sum()), int(grp['Gap2M'].sum()),
                        day_new, int(grp['Unfilled Gap'].sum())]
            rows.append(tot_row)

            # ── Print table ────────────────────────────────────────────────
            if _HAS_TABULATE:
                print(_tabulate(rows, headers=cols, tablefmt='simple', intfmt=','))
            else:
                # Fallback: plain header + rows (no separator line)
                header = '  '.join(f'{c:>{max(len(c),5)}}' if i else f'{c:<26}'
                                   for i, c in enumerate(cols))
                print('  ' + header)
                for row in rows:
                    line = '  '.join(f'{str(v):>{max(len(cols[i]),5)}}' if i else f'{str(v):<26}'
                                     for i, v in enumerate(row))
                    print('  ' + line)

            # ── EI Vetting — dynamic JPJ ───────────────────────────────────
            ei_jpj    = predicted_ei_jpj(day_1m, day_2m, day_rem, date)
            ei_jrnys  = day_tot / ei_jpj - day_acc_accept
            dow       = pd.Timestamp(date).day_name()
            ei_lo, ei_hi = (20, 30) if dow == 'Sunday' else (25, 40)

            if ei_jrnys < ei_lo:
                ei_status = f'⚠ BELOW range ({ei_lo}–{ei_hi}) — over-reserved'
            elif ei_jrnys > ei_hi:
                ei_status = f'⚠ ABOVE range ({ei_lo}–{ei_hi}) — under-reserved'
            else:
                ei_status = f'✓ within range ({ei_lo}–{ei_hi})'

            print(f"\n  EI Vetting ({dow}): {day_tot} jobs ÷ {ei_jpj:.2f} (v2 predicted JPJ) "
                  f"= {day_tot/ei_jpj:.1f} expected journeys  |  "
                  f"Pre-accepted TPs: {day_acc_accept} (excl. {day_ei_hold} HOLD_EI)  |  "
                  f"→ Journeys to EI: {ei_jrnys:.1f}  {ei_status}")
            print(f"  [v1 comparison] {day_tot} ÷ 5.5 = {day_tot/5.5:.1f} journeys to EI using fixed JPJ")

            # ── Excess & Shortage Report ───────────────────────────────────────
            excess_zones    = grp[(grp['Excess1M'] > 0) | (grp['Excess2M'] > 0)]
            shortage_zones  = grp[(grp['Gap1M'] > 0)    | (grp['Gap2M'] > 0)]
            mismatch_zones  = grp[
                ((grp['Excess1M'] > 0) & (grp['Gap2M'] > 0)) |
                ((grp['Excess2M'] > 0) & (grp['Gap1M'] > 0))
            ]
            if not excess_zones.empty or not shortage_zones.empty:
                print(f"\n  {'─'*80}")
                print(f"  EXCESS & SHORTAGE REPORT")
                print(f"  {'─'*80}")
                if not excess_zones.empty:
                    print(f"  Excess zones (over-accepted by bucket):")
                    for _, r in excess_zones.iterrows():
                        parts = []
                        if r['Excess1M'] > 0:
                            eff_a1m = r['Acc1M'] + r['Acc12M_to_1M']
                            tgt1m_note = ' ← Tgt1M=0 (accepted unnecessarily)' if r['Tgt1M'] == 0 else ''
                            parts.append(f"+{r['Excess1M']} 1M (effA1M={eff_a1m} [ded1M={r['Acc1M']}+12M→1M={r['Acc12M_to_1M']}] > Tgt1M={r['Tgt1M']}){tgt1m_note}")
                        if r['Excess2M'] > 0:
                            parts.append(f"+{r['Excess2M']} 2M (A2M={r['Acc2M']} > Tgt2M={r['Tgt2M']})")
                        print(f"    {r['Zone']:<28}: {', '.join(parts)}")
                if not shortage_zones.empty:
                    print(f"  Shortage zones (unfilled by bucket):")
                    for _, r in shortage_zones.iterrows():
                        parts = []
                        if r['Gap1M'] > 0:
                            parts.append(f"G1M={r['Gap1M']}")
                        if r['Gap2M'] > 0:
                            parts.append(f"G2M={r['Gap2M']}")
                        print(f"    {r['Zone']:<28}: {', '.join(parts)}")
                if not mismatch_zones.empty:
                    print(f"  Bucket mismatches (excess in one bucket, shortage in the other — excess provides NO relief):")
                    for _, r in mismatch_zones.iterrows():
                        parts = []
                        if r['Excess1M'] > 0 and r['Gap2M'] > 0:
                            parts.append(f"excess_1m={r['Excess1M']} but gap_2m={r['Gap2M']}")
                        if r['Excess2M'] > 0 and r['Gap1M'] > 0:
                            parts.append(f"excess_2m={r['Excess2M']} but gap_1m={r['Gap1M']}")
                        print(f"    {r['Zone']:<28}: {', '.join(parts)}")

        print(f"\n{'─'*80}")
        print(f"  Total new recommendations : {total_new}")
        print(f"  Output saved to           : {output_path}")

    else:
        print("\n  No gaps found — no new recommendations needed.")

    # ── Print recommended TPs ─────────────────────────────────────────────────
    recommended = res[res['new_recommendation'] == True].sort_values(
        ['DATE', 'sourcezone', 'new_recommendation_rank']
    )
    if not recommended.empty:
        print(f"\n{'='*80}")
        print("  RECOMMENDED TPs TO ACCEPT")
        print(f"{'='*80}")
        print(f"  {'Date':<12} {'Zone':<22} {'Username':<14} {'Men':>4} {'Type':<10} "
              f"{'Consider':<10} {'Rating':>7} {'Deallo':>7} {'VAT':>4} {'Cap':>4} {'Rank':>5}")
        print(f"  {'-'*100}")
        for _, r in recommended.iterrows():
            vat_label = 'No' if int(r.get('VAT_STATUS') or 0) == 0 else 'Yes'
            is_new    = float(r.get('rating') or 0) == 6.0
            flags     = '  ★ new TP' if is_new else ''
            deallo    = float(r.get('Deallo Rate Overall') or 0)
            if deallo > 0.20:
                flags += '  ⚠ high deallo'
            if not is_new and float(r.get('rating') or 0) < 4.5:
                flags += f"  ⚠ rating {float(r.get('rating') or 0):.2f}"
            if r.get('rating_fallback'):
                flags += f"  ★ shortfall fallback (rating {float(r.get('rating') or 0):.1f})"
            print(f"  {str(r['DATE'].date()):<12} {r['sourcezone']:<22} {str(r['USERNAME']):<14} "
                  f"{str(r['NUMBER_OF_MEN']):>4} {str(r['RES_TYPE']):<10} "
                  f"{str(r.get('consider_res_type') or ''):<10} "
                  f"{float(r.get('rating') or 0):>7.2f} "
                  f"{deallo:>7.3f} "
                  f"{vat_label:>4} "
                  f"{str(r.get('RESERVATION_CAPACITY') or ''):>4} "
                  f"{str(int(r['new_recommendation_rank'])):>5}{flags}")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]
    demand_path = args[0] if len(args) > 0 else DEMAND_FILE
    res_path    = args[1] if len(args) > 1 else RES_FILE
    run(demand_path, res_path)


if __name__ == '__main__':
    main()
