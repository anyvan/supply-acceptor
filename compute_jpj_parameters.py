"""
compute_jpj_parameters.py
=========================
Pulls the last N days of anyroute main runs from S3 and outputs a per-zone
parameter table for use in Supply Acceptor v2.

Zone assignment uses lat/long -> cluster CSV (same as fetch_and_run.py).
Job man-type uses number_of_men_required from full_output.csv directly.
Snowflake is only needed for category_id (furniture vs removals).

    Zone | Days | Avg1MJobs | Avg2MJobs | AvgRem | Retent% | 1M->1Jrny% | 1M->2Jrny% | 1M_JPJ | 2M_JPJ | Ovrl_JPJ

Usage:
    python3 compute_jpj_parameters.py             # last 30 days
    python3 compute_jpj_parameters.py --days 14   # last 14 days

AWS credentials must be in environment.
"""

import argparse
import io
import os
import sys
from collections import defaultdict
from datetime import date, timedelta

import boto3
import pandas as pd
import snowflake.connector

# ── S3 config ──────────────────────────────────────────────────────────────────
S3_BUCKET  = 'anyroute-results-data-production'
S3_PREFIX  = 'journey_building/GB/'

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR  = os.path.join(SCRIPT_DIR, '..')
CLUSTERS_CSV = os.path.join(PARENT_DIR, 'centers for cluster info - Final_clusters.csv')

UK_LAT_MIN, UK_LAT_MAX = 49.809432, 58.700162
UK_LNG_MIN, UK_LNG_MAX = -7.092700,  2.133975

ZONE_MAP = {
    'london': 'london', 'okehampton': 'cornwall', 'salisbury': 'salisbury',
    'cardiff': 'cardiff', 'birmingham': 'birmingham', 'corwen': 'north wales',
    'oxford': 'oxford', 'peterborough': 'peterborough', 'brighton': 'brighton',
    'canterbury': 'kent', 'norwich': 'norwich', 'warrington': 'manchester',
    'sheffield': 'sheffield', 'newcastle': 'newcastle', 'scarborough': 'east yorkshire',
    'edinburgh': 'edinburgh-glasgow', 'dumfries': 'north lake district',
    'kendal': 'lake district', 'fort william': 'northwest scotland',
    'fort.william': 'northwest scotland', 'aberdeen': 'northeast scotland',
}


# ── Cluster map (lat/long -> zone) ─────────────────────────────────────────────

def normalize_zero(x):
    return 0.0 if x == -0.0 else x

def fmt(x):
    v = round(float(x), 1)
    return f'{normalize_zero(v):.1f}'

def load_cluster_map() -> dict:
    df = pd.read_csv(CLUSTERS_CSV)
    result = {}
    for _, row in df.iterrows():
        key = fmt(row['lat']) + ':' + fmt(row['lon'])
        city = row['allocatedcity'].lower().strip()
        result[key] = ZONE_MAP.get(city, city)
    return result

def latlong_to_zone(lat, lng, cluster_map: dict):
    try:
        key = fmt(lat) + ':' + fmt(lng)
        return cluster_map.get(key)
    except Exception:
        return None


# ── S3 helpers ─────────────────────────────────────────────────────────────────

def find_main_run(s3, target_date: date):
    """Return the S3 prefix for the main anyroute run (~12:30 BST) on target_date."""
    year_prefix = f"{S3_PREFIX}{target_date.year}/"
    date_str    = target_date.strftime('%Y%m%d')
    paginator   = s3.get_paginator('list_objects_v2')

    candidates = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=year_prefix, Delimiter='/'):
        for obj in page.get('CommonPrefixes', []):
            folder = obj['Prefix'].rstrip('/').split('/')[-1]
            if folder.startswith(date_str):
                time_part = folder[9:15]  # HHMMSS from e.g. 20260331T113814Z_...
                candidates.append((time_part, obj['Prefix']))

    if not candidates:
        return None
    window = [(t, p) for t, p in candidates if '100000' <= t <= '140000']
    if window:
        return sorted(window)[-1][1]
    return sorted(candidates)[-1][1]


def load_virtual_jobs(s3, run_prefix: str, cluster_map: dict):
    """
    Download full_output.csv and return virtual job rows with:
      _men      — vehicle crew size (number_of_men)
      _job_men  — job requirement (number_of_men_required) — from file directly
      _job_zone — zone of the job's own lat/long
      _zone     — zone of the journey (mode of _job_zone across all stops)
    Returns all rows where _job_zone is mappable.
    """
    key = run_prefix + 'full_output.csv'
    try:
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)['Body'].read()
    except Exception as e:
        print(f"  [warn] Cannot read {key}: {e}")
        return None

    df = pd.read_csv(io.BytesIO(body), low_memory=False)
    df.columns = [c.lower() for c in df.columns]

    # ── All-jobs day summary (all vehicle types, for EI JPJ regression) ───────
    all_job_rows = df[df['job_id'].notna()].copy()
    all_job_rows['_men_req'] = pd.to_numeric(
        all_job_rows['number_of_men_required'] if 'number_of_men_required' in all_job_rows.columns
        else pd.Series(dtype=float), errors='coerce')
    # Keep minimal columns needed for post-Snowflake aggregation
    keep_cols = ['job_id', '_men_req', 'journey_unique_id']
    all_summary = {
        'rows':         all_job_rows[keep_cols].copy(),
        'all_listing_ids': list(all_job_rows['job_id'].dropna().unique()),
    }

    virtual = df[df['vehicle_type'] == 'virtual'].copy()
    jobs    = virtual[virtual['job_id'].notna()].copy()
    if jobs.empty:
        return None, all_summary

    jobs['_men']     = pd.to_numeric(jobs['number_of_men'],          errors='coerce').fillna(0).astype(int)
    jobs['_job_men'] = pd.to_numeric(jobs['number_of_men_required'],  errors='coerce')

    # Zone from lat/long
    jobs['lat'] = pd.to_numeric(jobs['lat'],  errors='coerce')
    jobs['long'] = pd.to_numeric(jobs['long'], errors='coerce')

    jobs['_job_zone'] = jobs.apply(
        lambda r: latlong_to_zone(r['lat'], r['long'], cluster_map)
        if pd.notna(r['lat']) and pd.notna(r['long']) else None,
        axis=1
    )

    # Journey zone = mode of _job_zone across all stops in the journey
    journey_zone = {}
    for jid, grp in jobs.groupby('journey_unique_id'):
        zones = grp['_job_zone'].dropna()
        if zones.empty:
            continue
        z = zones.mode().iloc[0]
        journey_zone[jid] = z

    jobs['_zone'] = jobs['journey_unique_id'].map(journey_zone)

    return jobs[jobs['_job_zone'].notna()].copy(), all_summary


# ── Snowflake: category only ────────────────────────────────────────────────────

def snowflake_category_lookup(listing_ids: list) -> pd.Series:
    """Returns a Series mapping LISTING_ID -> CATEGORY_ID."""
    print(f"\nConnecting to Snowflake ({len(listing_ids):,} listings for category lookup)...")
    conn = snowflake.connector.connect(
        user='salmanmemon.external@anyvan.com',
        authenticator='externalbrowser',
        account='pu40889.eu-west-1',
        role='MART_ROUTE_OPT_GROUP',
        warehouse='MART_ROUTE_OPT_WH',
        database='HARMONISED',
        schema='PRODUCTION',
    )
    cur  = conn.cursor()
    results = []
    chunk_size = 10_000
    for i in range(0, len(listing_ids), chunk_size):
        chunk   = listing_ids[i:i + chunk_size]
        ids_str = ', '.join(str(x) for x in chunk)
        cur.execute(f"SELECT LISTING_ID, CATEGORY_ID FROM harmonised.production.LISTING WHERE LISTING_ID IN ({ids_str})")
        results.extend(cur.fetchall())
        print(f"  Fetched chunk {i//chunk_size + 1} / {(len(listing_ids)-1)//chunk_size + 1}")
    cur.close()
    conn.close()

    cat_df = pd.DataFrame(results, columns=['LISTING_ID', 'CATEGORY_ID']).drop_duplicates('LISTING_ID')
    print(f"  Category rows: {len(cat_df):,}")
    return cat_df.set_index('LISTING_ID')['CATEGORY_ID']


# ── Per-zone accumulator ───────────────────────────────────────────────────────

def accumulate(jobs_day: pd.DataFrame, cat_map: pd.Series, accum: dict, snapshots: list, run_date=None):
    """Update per-zone accumulator for one day's data."""
    jobs = jobs_day.copy()
    jobs['_cat'] = jobs['job_id'].map(cat_map)

    all_zones = set(jobs['_job_zone'].dropna().unique()) | \
                set(jobs['_zone'].dropna().unique())

    for zone in all_zones:
        if zone not in accum:
            accum[zone] = defaultdict(float)
        a = accum[zone]

        # Retention (keyed by _job_zone)
        orig = jobs[jobs['_job_zone'] == zone]
        if len(orig) > 0:
            a['days_seen']        += 1
            a['jobs_originating'] += len(orig)
            a['jobs_retained']    += len(orig[orig['_zone'] == zone])

            orig_1m = orig[orig['_job_men'] == 1]
            orig_2m = orig[orig['_job_men'] == 2]
            a['jobs_originating_1m'] += len(orig_1m)
            a['jobs_retained_1m']    += len(orig_1m[orig_1m['_zone'] == zone])
            a['jobs_originating_2m'] += len(orig_2m)
            a['jobs_retained_2m']    += len(orig_2m[orig_2m['_zone'] == zone])

    # JPJ / split stats (keyed by journey zone _zone)
    mapped = jobs[jobs['_zone'].notna()].copy()
    furn   = mapped[mapped['_cat'] == 1]
    rem    = mapped[mapped['_cat'] == 2]

    for zone in mapped['_zone'].unique():
        a = accum[zone]

        z_all  = mapped[mapped['_zone'] == zone]
        z_furn = furn[furn['_zone'] == zone]
        z_rem  = rem[rem['_zone'] == zone]

        a['total_jobs']     += len(z_all)
        a['total_journeys'] += z_all['journey_unique_id'].nunique()

        z_1M = z_all[z_all['_men'] == 1]
        z_2M = z_all[z_all['_men'] == 2]
        a['jobs_on_1M']  += len(z_1M)
        a['journeys_1M'] += z_1M['journey_unique_id'].nunique()
        a['jobs_on_2M']  += len(z_2M)
        a['journeys_2M'] += z_2M['journey_unique_id'].nunique()

        f1M = z_furn[z_furn['_job_men'] == 1]
        a['furn_1M_req']       += len(f1M)
        a['furn_1M_req_on_1M'] += len(f1M[f1M['_men'] == 1])
        a['furn_1M_req_on_2M'] += len(f1M[f1M['_men'] == 2])
        a['furn_2M_jobs']      += len(z_furn[z_furn['_job_men'] == 2])
        a['removals']          += len(z_rem)

        # Snapshot for JPJ regression
        n_jobs     = len(z_all)
        n_journeys = z_all['journey_unique_id'].nunique()
        if n_journeys > 0:
            x = len(z_furn[z_furn['_job_men'] == 1])   # 1M furniture jobs
            y = len(z_furn[z_furn['_job_men'] == 2])   # 2M furniture jobs
            z_ = len(z_rem)                              # removal jobs
            snapshots.append({
                'date': run_date, 'zone': zone, 'x_1m': x, 'y_2m': y, 'z_rem': z_,
                'total_jobs': n_jobs, 'total_journeys': n_journeys,
                'jpj': n_jobs / n_journeys,
            })


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=30)
    args = parser.parse_args()

    s3 = boto3.client(
        's3', region_name='eu-west-1',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        aws_session_token=os.environ.get('AWS_SESSION_TOKEN'),
    )

    cluster_map = load_cluster_map()
    print(f"Cluster map loaded: {len(cluster_map):,} lat/long keys\n")

    today = date.today()
    dates = [today - timedelta(days=i) for i in range(1, args.days + 1)]

    # ── Step 1: collect job rows from S3 ──────────────────────────────────────
    print(f"Scanning {args.days} days of anyroute runs...\n")
    all_frames       = []
    all_summary_by_date = {}   # date -> all-jobs summary dict
    for d in dates:
        prefix = find_main_run(s3, d)
        if not prefix:
            print(f"  {d}: no run found — skipping")
            continue
        run_id = prefix.rstrip('/').split('/')[-1]
        jobs, all_summary = load_virtual_jobs(s3, prefix, cluster_map)
        if jobs is None or jobs.empty:
            print(f"  {d}: {run_id} — no virtual jobs")
            continue
        jobs['_date'] = d
        all_summary_by_date[d] = all_summary
        mapped = jobs['_zone'].notna().sum()
        zones  = jobs['_zone'].nunique()
        n_all = len(all_summary['rows'])
        n_jrnys = all_summary['rows']['journey_unique_id'].nunique()
        print(f"  {d}: {run_id} — {n_all:,} total jobs "
              f"({len(jobs):,} virtual mapped), "
              f"{n_jrnys} journeys, {zones} zones")
        all_frames.append(jobs)

    if not all_frames:
        print("No data found.")
        sys.exit(1)

    all_jobs = pd.concat(all_frames, ignore_index=True)
    print(f"\nTotal job rows: {len(all_jobs):,}")

    # ── Step 2: Snowflake — category only (all listing IDs across all vehicle types) ──
    def to_int_safe(x):
        try: return int(float(x))
        except (ValueError, TypeError): return None

    virtual_ids = set(v for v in (to_int_safe(x) for x in all_jobs['job_id'].dropna().unique()) if v is not None)
    all_ids     = virtual_ids.copy()
    for s in all_summary_by_date.values():
        all_ids.update(v for v in (to_int_safe(x) for x in s['all_listing_ids']) if v is not None)
    listing_ids = sorted(all_ids)
    cat_map     = snowflake_category_lookup(listing_ids)

    # ── Step 3: Accumulate ────────────────────────────────────────────────────
    accum     = {}
    snapshots = []
    for d in sorted(all_jobs['_date'].unique()):
        accumulate(all_jobs[all_jobs['_date'] == d], cat_map, accum, snapshots, run_date=d)

    # ── Step 4: Print table ───────────────────────────────────────────────────
    print()
    w = 178
    print("═" * w)
    print(f"JPJ Parameter Table — last {args.days} days  "
          f"(lat/long cluster assignment + anyroute number_of_men_required)")
    print(f"{'Zone':<22} {'Days':>5} {'Avg1MJobs':>10} {'Avg2MJobs':>10} {'AvgRem':>7} "
          f"{'Retent%':>8} {'Ret1M%':>7} {'Ret2M%':>7} "
          f"{'1M->1Jrny':>10} {'1M->2Jrny':>10} {'1M_JPJ':>7} {'2M_JPJ':>7} {'Ovrl_JPJ':>9}")
    print("─" * w)

    rows = []
    for zone in sorted(accum.keys()):
        a    = accum[zone]
        days = int(a['days_seen'])

        avg_1m  = a['furn_1M_req']  / days if days > 0 else 0
        avg_2m  = a['furn_2M_jobs'] / days if days > 0 else 0
        avg_rem = a['removals']     / days if days > 0 else 0

        retention  = (a['jobs_retained'] / a['jobs_originating']
                      if a['jobs_originating'] > 0 else None)
        retention_1m = (a['jobs_retained_1m'] / a['jobs_originating_1m']
                        if a['jobs_originating_1m'] > 0 else None)
        retention_2m = (a['jobs_retained_2m'] / a['jobs_originating_2m']
                        if a['jobs_originating_2m'] > 0 else None)

        tot_1M = a['furn_1M_req']
        pct_1m_1j = a['furn_1M_req_on_1M'] / tot_1M if tot_1M > 0 else 0
        pct_1m_2j = a['furn_1M_req_on_2M'] / tot_1M if tot_1M > 0 else 0

        jpj_1m   = a['jobs_on_1M'] / a['journeys_1M'] if a['journeys_1M'] > 0 else None
        jpj_2m   = a['jobs_on_2M'] / a['journeys_2M'] if a['journeys_2M'] > 0 else None
        jpj_ovrl = a['total_jobs'] / a['total_journeys'] if a['total_journeys'] > 0 else None

        rows.append({
            'zone': zone, 'days': days,
            'avg_1m': avg_1m, 'avg_2m': avg_2m, 'avg_rem': avg_rem,
            'retention': retention,
            'retention_1m': retention_1m, 'retention_2m': retention_2m,
            'pct_1m_1j': pct_1m_1j, 'pct_1m_2j': pct_1m_2j,
            'jpj_1m': jpj_1m, 'jpj_2m': jpj_2m, 'jpj_ovrl': jpj_ovrl,
        })

        ret_s  = f"{retention:.0%}"    if retention    is not None else "—"
        ret1m  = f"{retention_1m:.0%}" if retention_1m is not None else "—"
        ret2m  = f"{retention_2m:.0%}" if retention_2m is not None else "—"
        j1m    = f"{jpj_1m:.2f}"       if jpj_1m   else "—"
        j2m    = f"{jpj_2m:.2f}"       if jpj_2m   else "—"
        jovrl  = f"{jpj_ovrl:.2f}"     if jpj_ovrl else "—"

        print(f"  {zone:<20} {days:>5} {avg_1m:>10.1f} {avg_2m:>10.1f} {avg_rem:>7.1f} "
              f"{ret_s:>8} {ret1m:>7} {ret2m:>7} "
              f"{pct_1m_1j:>10.0%} {pct_1m_2j:>10.0%} {j1m:>7} {j2m:>7} {jovrl:>9}")

    print("═" * w)

    # ── Step 5: Edinburgh example ──────────────────────────────────────────────
    eg_zone = 'edinburgh-glasgow'
    if eg_zone in accum:
        r   = next(r for r in rows if r['zone'] == eg_zone)
        ret = r['retention'] or 1.0
        e1  = 20 * ret
        e2  = 20 * ret
        print(f"\nExample — {eg_zone} with 20 1M jobs + 20 2M jobs:")
        print(f"  Step 1 — Retention ({ret:.0%}):    eff 1M = {e1:.1f},  eff 2M = {e2:.1f}")
        print(f"  Step 2 — 1M TPs  = floor({e1:.1f} × {r['pct_1m_1j']:.0%} / {r['jpj_1m']:.2f}) = {int(e1 * r['pct_1m_1j'] / r['jpj_1m'])}")
        j2m_load = e1 * r['pct_1m_2j'] + e2
        print(f"  Step 3 — 2M TPs  = round(({e1:.1f}×{r['pct_1m_2j']:.0%} + {e2:.1f}) / {r['jpj_2m']:.2f}) = {round(j2m_load / r['jpj_2m'])}")
        print(f"  Step 4 — Total journeys = round({e1+e2:.1f} / {r['jpj_ovrl']:.2f}) = {round((e1+e2) / r['jpj_ovrl'])}")

    # ── Step 6: JPJ linear regression (day level) ─────────────────────────────
    import numpy as np

    snap_df = pd.DataFrame(snapshots)

    # ── All-jobs day dataframe (for EI JPJ regression) ────────────────────────
    def to_int_id(x):
        try: return int(float(x))
        except (ValueError, TypeError): return None

    day_rows = []
    for d, s in sorted(all_summary_by_date.items()):
        day_jobs = s['rows'].copy()
        day_jobs['_int_id'] = day_jobs['job_id'].apply(to_int_id)
        day_jobs['_cat']    = day_jobs['_int_id'].map(cat_map)

        furn = day_jobs[day_jobs['_cat'] == 1]
        rem  = day_jobs[day_jobs['_cat'] == 2]

        n_jobs     = len(day_jobs)
        n_journeys = day_jobs['journey_unique_id'].nunique()
        if n_journeys == 0:
            continue

        day_rows.append({
            'date':            d,
            'x_1m':           int((furn['_men_req'] == 1).sum()),
            'y_2m':           int((furn['_men_req'] == 2).sum()),
            'z_rem':          len(rem),
            'total_jobs':     n_jobs,
            'total_journeys': n_journeys,
            'actual_jpj':     n_jobs / n_journeys,
        })
    day_df = pd.DataFrame(day_rows).sort_values('date')

    print(f"\n{'─'*65}")
    print(f"JPJ Linear Regression — day level ({len(day_df)} days)")
    print(f"  Model:  JPJ = β0 + β1·(1M jobs) + β2·(2M jobs) + β3·(removal jobs)")
    print(f"{'─'*65}")

    is_sunday = (pd.to_datetime(day_df['date']).dt.dayofweek == 6).astype(float).values
    X = np.column_stack([
        np.ones(len(day_df)),
        day_df['x_1m'].values,
        day_df['y_2m'].values,
        day_df['z_rem'].values,
        is_sunday,
    ])
    y_jpj = day_df['actual_jpj'].values

    # Weight by total journeys — busier days are more reliable observations
    W = day_df['total_journeys'].values
    Xw = X * W[:, None]
    yw = y_jpj * W
    coeffs, _, _, _ = np.linalg.lstsq(Xw, yw, rcond=None)

    b0, b1, b2, b3, b4 = coeffs
    y_pred = X @ coeffs
    ss_res = np.sum(W * (y_jpj - y_pred) ** 2)
    ss_tot = np.sum(W * (y_jpj - np.average(y_jpj, weights=W)) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else float('nan')

    print(f"  β0 (intercept)    = {b0:+.4f}")
    print(f"  β1 (1M jobs)      = {b1:+.4f}")
    print(f"  β2 (2M jobs)      = {b2:+.4f}")
    print(f"  β3 (removal jobs) = {b3:+.4f}")
    print(f"  β4 (Sunday dummy) = {b4:+.4f}")
    print(f"  R²                = {r2:.4f}")
    print(f"\n  Formula (non-Sun): JPJ = {b0:.4f} + {b1:+.4f}·x + {b2:+.4f}·y + {b3:+.4f}·z")
    print(f"  Formula (Sunday):  JPJ = {b0+b4:.4f} + {b1:+.4f}·x + {b2:+.4f}·y + {b3:+.4f}·z")

    # ── Day-level actual vs predicted table ───────────────────────────────────
    day_df['is_sunday'] = is_sunday
    day_df['pred_jpj']  = y_pred
    day_df['diff']      = day_df['pred_jpj'] - day_df['actual_jpj']
    day_df['weekday']   = pd.to_datetime(day_df['date']).dt.strftime('%a')

    print(f"\n{'─'*85}")
    print(f"{'Date':<12} {'Day':<5} {'AllJobs':>8} {'1M Req':>7} {'2M Req':>7} {'Rem':>5} "
          f"{'Actual JPJ':>11} {'Pred JPJ':>9} {'Diff':>7}")
    print(f"{'─'*85}")
    for _, row in day_df.iterrows():
        diff_s = f"{row['diff']:+.2f}"
        print(f"  {str(row['date']):<10} {row['weekday']:<5} {int(row['total_jobs']):>8} "
              f"{int(row['x_1m']):>7} {int(row['y_2m']):>7} {int(row['z_rem']):>5} "
              f"{row['actual_jpj']:>11.2f} {row['pred_jpj']:>9.2f} {diff_s:>7}")
    print(f"{'─'*85}")

    # ── Step 8: Save CSV ───────────────────────────────────────────────────────
    out_df   = pd.DataFrame(rows)
    out_path = os.path.join(SCRIPT_DIR, 'jpj_parameters.csv')
    out_df.to_csv(out_path, index=False)
    print(f"\nSaved to: {out_path}")


if __name__ == '__main__':
    main()
