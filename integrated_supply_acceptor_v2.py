"""
Integrated Supply Acceptor V2
──────────────────────────────
V2 of the integrated runner. Identical mode-selection logic to V1 but routes
everything through the V2 algorithm stack:

    Actuals mode  → fetch_and_run_v2.py  → supply_acceptor_v2.py
    Forecast mode → supply_acceptor_forecast_v2.py  (no JPJ multiplier patching)

Mode decision (per pickup date P, D = days until pickup):
    D-1  (any time)       → actuals,  V2 targeting + empirical EI JPJ
    D-2  before 09:00 UK  → forecast, V2 targeting + empirical EI JPJ
    D-2  from   09:00 UK  → actuals,  V2 targeting + empirical EI JPJ
    D-3  or earlier       → forecast, V2 targeting + empirical EI JPJ

Conservatism in forecast mode (V2):
    V1 applied a JPJ multiplier (×1.2) to target fewer journeys further out.
    V2 encodes conservatism entirely through zone coverage ratios in
    dynamic_coverage() — no additional multiplier is applied.

Data fetching (shared with V1, no duplication):
    Actuals : fetch_and_run_v2.fetch_demand_v2()  +  fetch_and_run.fetch_reservations()
    Forecast: supply_acceptor_forecast.find_forecast_files/load_forecast_demand()
              + fetch_and_run.fetch_reservations()

Post-run steps (same as V1):
    - TP deduplication report  (report_tp_duplicates)
    - Recommendations CSV      (write_recommendations_csv)

Usage:
    python3 integrated_supply_acceptor_v2.py 2026-04-02
    python3 integrated_supply_acceptor_v2.py 2026-04-02 2026-04-03
    python3 integrated_supply_acceptor_v2.py --dry-run 2026-04-02
    python3 integrated_supply_acceptor_v2.py            # defaults to tomorrow
"""

import sys
import os
import glob
import importlib
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

sys.stdout.reconfigure(line_buffering=True)

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR   = os.path.dirname(SCRIPT_DIR)
FORECAST_DIR = os.path.join(PARENT_DIR, 'updated_forecast', 'production_v5')
OUTPUT_DIR   = os.path.join(SCRIPT_DIR, 'outputs')
UK_TZ        = ZoneInfo('Europe/London')

os.makedirs(OUTPUT_DIR, exist_ok=True)

sys.path.insert(0, SCRIPT_DIR)

# ── S3 forecast config (same bucket as V1) ─────────────────────────────────────
FORECAST_S3_BUCKET = 'supply-acceptor-data-production'
FORECAST_S3_PREFIX = 'demand-forecast/'

# ── Mode / time helpers ────────────────────────────────────────────────────────

def get_uk_now() -> datetime:
    return datetime.now(UK_TZ)


def decide(pickup_date: date, uk_now: datetime) -> tuple:
    """
    Returns (mode, days_out).
      mode    : 'actuals' or 'forecast'
      days_out: integer days until pickup
    """
    days_out = (pickup_date - uk_now.date()).days
    uk_hour  = uk_now.hour + uk_now.minute / 60.0

    if days_out <= 1:
        return 'actuals', days_out
    elif days_out == 2:
        return ('forecast' if uk_hour < 9.0 else 'actuals'), days_out
    else:
        return 'forecast', days_out


# ── S3 forecast sync ──────────────────────────────────────────────────────────

class ForecastSyncError(Exception):
    """Non-credential S3 failure — abort, no fallback."""

class CredentialsError(ForecastSyncError):
    """AWS credentials missing, expired, or invalid."""


def sync_forecast_from_s3(dest_dir: str) -> str:
    """
    Downloads the latest V5 cluster lt200km forecast files from S3 into dest_dir.
    Returns the run_date string (e.g. '2026-04-03') on success.
    Raises CredentialsError if credentials are missing/expired/invalid.
    Raises ForecastSyncError for any other S3 failure.
    Never falls back to local files — callers must handle errors explicitly.
    """
    try:
        import boto3
        from botocore.exceptions import NoCredentialsError, ClientError
    except ImportError:
        raise ForecastSyncError("boto3 is not installed — run: pip install boto3")

    CREDENTIAL_ERROR_CODES = {
        'NoCredentialProviders',
        'InvalidClientTokenId',
        'ExpiredTokenException',
        'AuthFailure',
        'AccessDenied',
        'InvalidAccessKeyId',
    }

    try:
        s3 = boto3.client('s3', region_name='eu-west-1')

        resp = s3.list_objects_v2(
            Bucket=FORECAST_S3_BUCKET,
            Prefix=FORECAST_S3_PREFIX,
            Delimiter='/'
        )
        folders = [p['Prefix'] for p in resp.get('CommonPrefixes', [])]
        if not folders:
            raise ForecastSyncError("No forecast folders found in S3.")

        latest   = sorted(folders)[-1]
        run_date = latest.rstrip('/').split('/')[-1]

        target_files = [
            f'v5_furniture_cluster_lt200km_{run_date}.csv',
            f'v5_removals_cluster_lt200km_{run_date}.csv',
            f'v5_removals_cluster_{run_date}.csv',
        ]

        os.makedirs(dest_dir, exist_ok=True)
        for fname in target_files:
            s3_key     = latest + fname
            local_path = os.path.join(dest_dir, fname)
            if os.path.exists(local_path):
                print(f"[forecast-s3] Already cached: {fname}")
                continue
            s3.download_file(FORECAST_S3_BUCKET, s3_key, local_path)
            print(f"[forecast-s3] Downloaded: {fname}")

        print(f"[forecast-s3] Forecast run: {run_date}")
        return run_date

    except NoCredentialsError:
        raise CredentialsError("No AWS credentials found.")
    except ClientError as e:
        code = e.response['Error']['Code']
        if code in CREDENTIAL_ERROR_CODES:
            raise CredentialsError(f"AWS credentials invalid or expired ({code}).")
        raise ForecastSyncError(f"S3 ClientError: {code} — {e}")
    except (ForecastSyncError, CredentialsError):
        raise
    except Exception as e:
        raise ForecastSyncError(f"Unexpected S3 error: {e}")


def _prompt_aws_credentials():
    """Interactively prompt for AWS credentials and set them as env vars."""
    import sys
    if not sys.stdin.isatty():
        print(
            "\n[forecast-s3] Cannot prompt for credentials — stdin is not a terminal.\n"
            "  Export credentials before running:\n"
            "    export AWS_ACCESS_KEY_ID=...\n"
            "    export AWS_SECRET_ACCESS_KEY=...\n"
            "    export AWS_SESSION_TOKEN=...\n"
        )
        sys.exit(1)
    print("\n" + "=" * 70)
    print("  AWS credentials required to download the forecast from S3.")
    print("  Paste each value and press Enter (input is not echoed for the secret).")
    print("=" * 70)
    import getpass
    key_id = input("  AWS_ACCESS_KEY_ID     : ").strip()
    secret = getpass.getpass("  AWS_SECRET_ACCESS_KEY : ").strip()
    token  = getpass.getpass("  AWS_SESSION_TOKEN     : ").strip()
    os.environ['AWS_ACCESS_KEY_ID']     = key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = secret
    if token:
        os.environ['AWS_SESSION_TOKEN']  = token
    elif 'AWS_SESSION_TOKEN' in os.environ:
        del os.environ['AWS_SESSION_TOKEN']
    print("[forecast-s3] Credentials set. Retrying S3 sync...\n")


# ── Actuals mode ──────────────────────────────────────────────────────────────

def run_actuals_v2(dates: list) -> str | None:
    """
    Fetches demand (furniture + removals) and reservations from Snowflake,
    then calls supply_acceptor_v2.run() directly — all in-process so output
    flows in the correct sequential order.
    Returns the path of the output CSV.
    """
    import fetch_and_run_v2 as fv2
    import supply_acceptor_v2 as sav2

    stamp       = datetime.now().strftime('%Y-%m-%d_%H%M')
    demand_file = os.path.join(OUTPUT_DIR, f'demand_v2_{stamp}.csv')
    res_file    = os.path.join(OUTPUT_DIR, f'recommended_reservations_{stamp}.csv')
    output_path = os.path.join(OUTPUT_DIR, f'supply_acceptor_v2_output_{stamp}.csv')

    print("\nConnecting to Snowflake (browser auth)...")
    conn = fv2.get_conn()

    print("\nFetching TP quality...")
    tp_quality = fv2.fetch_tp_quality(conn)

    print("\nFetching demand (v2 — furniture + removals)...")
    demand = fv2.fetch_demand_v2(dates, conn)
    demand.to_csv(demand_file, index=False)
    print(f"  → {demand_file}")

    print("\nFetching reservations...")
    from fetch_and_run import fetch_reservations
    res = fetch_reservations(dates, conn, tp_quality)
    res.to_csv(res_file, index=False)
    print(f"  → {res_file}")

    conn.close()

    print(f"\nRunning supply acceptor v2 (confirmed jobs mode)...")
    sav2.run(demand_file, res_file, output_path)
    return output_path


# ── Forecast mode ─────────────────────────────────────────────────────────────

def run_forecast_v2(dates: list) -> str:
    """
    1. Syncs latest V5 forecast from S3 — aborts if unavailable (no local fallback).
       Prompts for AWS credentials interactively if they are missing or expired.
    2. Fetches reservations from Snowflake via fetch_and_run helpers.
    3. Calls supply_acceptor_forecast_v2.run() pinned to the S3 run_date so stale
       local files from a different date are never used.

    Returns the path of the output CSV.
    """
    import fetch_and_run as far
    import supply_acceptor_forecast_v2 as sfv2

    # Pull latest forecast from S3 — prompt for credentials if needed, abort on error
    run_date = None
    for attempt in range(3):
        try:
            run_date = sync_forecast_from_s3(FORECAST_DIR)
            break
        except CredentialsError as e:
            print(f"\n[forecast-s3] {e}")
            if attempt < 2:
                _prompt_aws_credentials()
            else:
                print("[forecast-s3] Credentials failed after 3 attempts. Cannot proceed.")
                sys.exit(1)
        except ForecastSyncError as e:
            print(f"\n[forecast-s3] Fatal: {e}")
            sys.exit(1)

    stamp       = datetime.now().strftime('%Y-%m-%d_%H%M')
    res_file    = os.path.join(OUTPUT_DIR, f'recommended_reservations_{stamp}.csv')
    output_path = os.path.join(OUTPUT_DIR, f'supply_acceptor_v2_forecast_output_{stamp}.csv')

    # Fetch reservations
    print("\nConnecting to Snowflake (browser auth)...")
    conn = far.get_conn()

    print("\nFetching TP quality...")
    tp_quality = far.fetch_tp_quality(conn)

    print("\nFetching reservations...")
    res = far.fetch_reservations(dates, conn, tp_quality)
    res.to_csv(res_file, index=False)
    print(f"  → {res_file}")

    conn.close()

    print(f"\n[integrated-v2] Running supply acceptor V2 (forecast mode, run_date={run_date})...")
    sfv2.run(FORECAST_DIR, res_file, output_path, run_date=run_date, pickup_dates=dates)

    return output_path


# ── Post-processing: TP deduplication report ──────────────────────────────────

def report_tp_duplicates(output_path: str):
    """
    Analyses the V2 output CSV and highlights any TP that appears more than
    once on the same day (accepted + newly recommended).

    Logic identical to V1: cap = 1 per TP per day.
    For each excess slot:
      1. Search for a same-type (NUMBER_OF_MEN) replacement from the pending pool.
         New TPs (rating == 6.00) are prioritised first.
      2. If no replacement → recommend leaving for EI (or do not accept duplicate).

    Read-only vetting — output CSV is NOT modified.
    """
    import supply_acceptor_v2 as sav2

    df = _load_output_csv(output_path)
    df['_score']    = df.apply(sav2.score_tp, axis=1)
    df['_is_new_tp'] = df['rating'].apply(lambda r: float(r or 0) == 6.0)

    active_mask = (df['IRES_STATUS'] == 'accepted') | df['new_recommendation']

    EI_ELIGIBLE_ZONES = {'london', 'birmingham', 'manchester'}
    any_duplicates = False

    for (date_val, zone), group_idx in df.groupby(['DATE', 'sourcezone']).groups.items():
        group  = df.loc[group_idx]
        active = group[active_mask.loc[group_idx]]

        if active.empty:
            continue

        counts = active.groupby('USERNAME').size()
        dupes  = counts[counts > 1]
        if dupes.empty:
            continue

        if not any_duplicates:
            print("\n" + "=" * 90)
            print("  DEDUP VETTING REPORT — TPs appearing more than once (cap = 1 per TP per day)")
            print("=" * 90)
            any_duplicates = True

        for username, count in dupes.items():
            tp_rows = active[active['USERNAME'] == username].sort_values('_score', ascending=False)

            accepted_count  = int((tp_rows['IRES_STATUS'] == 'accepted').sum())
            recommend_count = int(tp_rows['new_recommendation'].sum())
            men_val  = int(tp_rows.iloc[0]['NUMBER_OF_MEN'])
            res_type = tp_rows.iloc[0]['RES_TYPE']
            score    = round(tp_rows.iloc[0]['_score'], 4)

            print(f"\n  ⚠  {zone.upper()}  {date_val.date()}  |  {username}  "
                  f"({men_val}-man, {res_type})  score={score}  "
                  f"[{accepted_count} accepted + {recommend_count} recommended = {count} total]")

            keep_count  = max(0, 1 - accepted_count)
            excess_rows = tp_rows[tp_rows['new_recommendation']].iloc[keep_count:]

            if excess_rows.empty:
                print(f"    Both slots already accepted — cannot replace either.")
                continue

            active_usernames = set(active['USERNAME'])

            for _, excess_row in excess_rows.iterrows():
                excess_men  = int(excess_row['NUMBER_OF_MEN'])
                excess_type = excess_row['RES_TYPE']

                pending = df[
                    (df['DATE']             == date_val) &
                    (df['sourcezone']       == zone) &
                    (df['IRES_STATUS']      == 'pending') &
                    (~df['new_recommendation']) &
                    (~df['USERNAME'].isin(active_usernames)) &
                    (df['NUMBER_OF_MEN']    == excess_men)
                ].sort_values(['_is_new_tp', '_score'], ascending=[False, False])

                if pending.empty:
                    if zone in EI_ELIGIBLE_ZONES:
                        print(f"    Slot ({excess_men}-man, {excess_type})  →  ✗ No replacement — leave for EI")
                    else:
                        print(f"    Slot ({excess_men}-man, {excess_type})  →  ✗ No replacement — do not accept duplicate")
                else:
                    best      = pending.iloc[0]
                    repl_rat  = round(float(best.get('rating', 0)), 2)
                    repl_dal  = f"{round(float(best.get('Deallo Rate Overall', 0)) * 100, 1)}%"
                    repl_cap  = float(best.get('RESERVATION_CAPACITY', 0) or 0)
                    new_flag  = '  ★ new TP' if bool(best['_is_new_tp']) else ''

                    quality_warnings = []
                    if repl_rat < 4.2 and not bool(best['_is_new_tp']):
                        quality_warnings.append(f'rating {repl_rat} < 4.2')
                    if repl_cap < 8:
                        quality_warnings.append(f'capacity {int(repl_cap)} < 8 cubes')
                    quality_flag = (f'  ⚠ LOW QUALITY ({", ".join(quality_warnings)})'
                                    if quality_warnings else '')

                    print(f"    Slot ({excess_men}-man, {excess_type})  →  ✓ Replace with: "
                          f"{best['USERNAME']}  ({excess_men}-man, {best['RES_TYPE']}, "
                          f"rating={repl_rat}, deallo={repl_dal}, cap={int(repl_cap)}){new_flag}{quality_flag}")
                    active_usernames.add(best['USERNAME'])

    if not any_duplicates:
        print("\n[dedup-v2] No TP duplicates found — all TPs appear at most once per day.")

    df.drop(columns=['_score', '_is_new_tp'], inplace=True, errors='ignore')


# ── Post-processing: write recommendations CSV ────────────────────────────────

def write_recommendations_csv(output_path: str):
    """
    Reads the V2 output CSV and writes the newly recommended rows to
    recommended_tps_YYYY-MM-DD.csv (one file per pickup date).
    """
    RES_COLS = [
        'DATE', 'ID', 'USERNAME', 'IRES_STATUS', 'RES_TYPE', 'NUMBER_OF_MEN',
        'START_POSTCODE', 'RESERVATION_CAPACITY', 'HOURS_AVAILABLE', 'sourcezone',
        'consider_res_type', 'rating', 'Deallo Rate', 'Deallo Rate Overall', 'VAT_STATUS',
    ]

    df = _load_output_csv(output_path)
    recs = df[df['new_recommendation']].copy()

    if recs.empty:
        print("[recommendations-v2] No new recommendations to write.")
        return

    recs = recs.sort_values(['DATE', 'sourcezone', 'new_recommendation_rank'])
    out_cols = [c for c in RES_COLS if c in recs.columns]
    out = recs[out_cols]

    for pickup_date, group in out.groupby(out['DATE'].dt.date):
        csv_path = os.path.join(OUTPUT_DIR, f'recommended_tps_{pickup_date}.csv')
        group.to_csv(csv_path, index=False)
        print(f"[recommendations-v2] Written {len(group)} row(s)  →  {csv_path}")


# ── Post-processing: write vetted recommendations CSV ─────────────────────────

# Zones considered "deep-pool" — safe to hold back TPs for EI balancing
DEEP_POOL_ZONES = {'london', 'birmingham', 'manchester', 'peterborough'}

# Per-zone EI reservation quotas: always hold back this many TPs for EI,
# regardless of the current EI level.  'prefer_men' controls which men-type
# is removed first (e.g. prefer removing 2M from London where 2M is in excess).
EI_RESERVATION_QUOTA: dict = {
    'london':       {'count': 5, 'prefer_men': [2]},
    'birmingham':   {'count': 3, 'prefer_men': []},
    'manchester':   {'count': 3, 'prefer_men': []},
    'sheffield':    {'count': 2, 'prefer_men': []},
    'peterborough': {'count': 2, 'prefer_men': []},
}


def write_vetted_recommendations_csv(output_path: str):
    """
    Produces a second per-date CSV — vetted_tps_YYYY-MM-DD.csv — that applies
    two layers of corrections on top of the raw recommendations:

    Layer 0.5 — Zone EI reservation quotas:
        Unconditionally hold back a fixed number of TPs from high-supply zones
        (configured in EI_RESERVATION_QUOTA) so EI always receives a baseline
        share of journeys.  London: 5 TPs (prefer 2M); Birmingham/Manchester: 3;
        Sheffield/Peterborough: 2.  Selection is random within the preferred
        men-type pool first, then falls back to other types.
        Held-back rows get vetting_status = 'HOLD_EI'.

    Layer 1 — EI balancing:
        If EI journeys (post-raw-acceptance) would fall below the target floor
        (25 on weekdays, 20 on Sundays), hold back the lowest-ranked TPs from
        deep-pool zones (london, birmingham, manchester, peterborough, plus any
        zone whose pending pool >= 2× gap) until EI reaches the floor.
        Held-back rows get vetting_status = 'HOLD_EI'.

    Layer 2 — Dedup corrections:
        Any TP that appears more than once (accepted + newly recommended) on the
        same day gets one slot removed.  Where a same-men replacement exists in
        pending it gets vetting_status = 'REPLACE_DEDUP'; where there is no
        replacement the excess slot gets vetting_status = 'REMOVE_DEDUP'.

    Output columns:
        ID, DATE, sourcezone, USERNAME, NUMBER_OF_MEN, RES_TYPE, consider_res_type,
        rating, deallo_pct, VAT_STATUS, RESERVATION_CAPACITY,
        new_recommendation_rank, vetting_status, vetting_reason
    """
    import pandas as pd
    import supply_acceptor_v2 as sav2

    df = _load_output_csv(output_path)
    df['_score']     = df.apply(sav2.score_tp, axis=1)
    df['_is_new_tp'] = df['rating'].apply(lambda r: float(r or 0) == 6.0)
    df['deallo_pct'] = (
        pd.to_numeric(df.get('Deallo Rate Overall', 0), errors='coerce').fillna(0) * 100
    ).round(1)

    # Load zone summary companion
    summary_path = output_path.replace('.csv', '_zone_summary.csv')
    if not __import__('os').path.exists(summary_path):
        print(f"[vetted] Zone summary not found ({summary_path}) — skipping vetted output.")
        return
    zone_summary = pd.read_csv(summary_path)
    zone_summary['_zone_key'] = zone_summary['Zone'].str.extract(r'^([a-z\- ]+)', expand=False).str.strip().str.lower()

    recs = df[df['new_recommendation']].copy()
    if recs.empty:
        print("[vetted] No new recommendations to vet.")
        return

    # Tag each recommendation with a mutable vetting_status
    recs = recs.sort_values(['DATE', 'sourcezone', 'new_recommendation_rank'])
    recs['vetting_status'] = 'ACCEPT'
    recs['vetting_reason'] = ''

    for pickup_date_ts, day_recs_idx in recs.groupby(recs['DATE'].dt.date).groups.items():
        pickup_date = pd.Timestamp(pickup_date_ts)

        # ── Compute EI journeys including already-accepted + all new recs ──────
        day_all = df[df['DATE'].dt.date == pickup_date_ts]
        already_acc = int((day_all['IRES_STATUS'] == 'accepted').sum())

        # Demand totals from zone summary (for this date)
        day_summary = zone_summary[
            zone_summary['Pickup Date'].apply(lambda d: str(d)[:10]) == str(pickup_date_ts)
        ]
        day_1m  = int(day_summary['Furn1M Jobs'].sum())
        day_2m  = int(day_summary['Furn2M Jobs'].sum())
        day_rem = int(day_summary['Rem Jobs'].sum())

        ei_jpj   = sav2.predicted_ei_jpj(day_1m, day_2m, day_rem, pickup_date)
        total_jobs = day_1m + day_2m + day_rem
        total_new_recs = len(day_recs_idx)

        ei_lo = 20 if pickup_date.day_name() == 'Sunday' else 25

        # Current EI (with ALL new recs accepted)
        total_acc_with_all = already_acc + total_new_recs
        ei_current = total_jobs / ei_jpj - total_acc_with_all

        # ── Layer 0.5: Zone EI reservation quotas ─────────────────────────────
        # Unconditionally hold back a fixed number of TPs per zone so that EI
        # always receives a baseline share of journeys from high-supply zones.
        # prefer_men controls which bucket is removed first (e.g. excess 2M in
        # London).  Selection is random within the preferred / fallback pools.
        import random as _random
        for zone, quota_cfg in EI_RESERVATION_QUOTA.items():
            quota   = quota_cfg['count']
            prefer  = quota_cfg.get('prefer_men', [])

            # Eligible = ACCEPT-status new recs in this zone on this date
            eligible_idx = [
                i for i in day_recs_idx
                if recs.at[i, 'sourcezone'] == zone
                and recs.at[i, 'vetting_status'] == 'ACCEPT'
            ]
            if not eligible_idx:
                continue

            # Split into preferred bucket and remainder, shuffle each
            preferred = [i for i in eligible_idx if int(recs.at[i, 'NUMBER_OF_MEN']) in prefer]
            remainder = [i for i in eligible_idx if i not in preferred]
            _random.shuffle(preferred)
            _random.shuffle(remainder)
            pool = preferred + remainder

            to_hold = min(quota, len(pool))
            for idx in pool[:to_hold]:
                men = int(recs.at[idx, 'NUMBER_OF_MEN'])
                recs.at[idx, 'vetting_status'] = 'HOLD_EI'
                recs.at[idx, 'vetting_reason'] = (
                    f"EI reservation: {zone} zone quota={quota} "
                    f"— held {men}M TP for EI"
                )

        # ── Layer 1: EI balancing — proportional hold across deep-pool zones ─
        if ei_current < ei_lo:
            import math

            # Build per-zone pending/gap sizes for dynamic deep-pool check
            zone_gap = {}
            zone_pending = {}
            for _, zrow in day_summary.iterrows():
                zk = str(zrow['_zone_key'])
                zone_gap[zk]     = max(0, int(zrow.get('Gap', 0)))
                zone_pending[zk] = int(zrow.get('Pending', 0))

            # Deep-pool = configured list OR pending >= 2×gap
            def is_deep_pool(zone: str) -> bool:
                if zone in DEEP_POOL_ZONES:
                    return True
                gap = zone_gap.get(zone, 0)
                pend = zone_pending.get(zone, 0)
                return gap > 0 and pend >= 2 * gap

            needed = int(math.ceil(ei_lo - ei_current))

            import random

            # Per-zone bucket status from zone summary
            # bucket_excess[zone] = set of men values that are in excess (safe to cut)
            # bucket_short[zone]  = set of men values that are in shortage (do NOT cut)
            bucket_excess = {}
            bucket_short  = {}
            for _, zrow in day_summary.iterrows():
                zk = str(zrow['_zone_key'])
                exc = set()
                sht = set()
                # 1M bucket: men=1 and men=12 both count toward accepted_1m
                if int(zrow.get('Excess1M', 0)) > 0:
                    exc.update([1, 12])
                if int(zrow.get('Gap1M', 0)) > 0:
                    sht.update([1, 12])
                # 2M bucket
                if int(zrow.get('Excess2M', 0)) > 0:
                    exc.add(2)
                if int(zrow.get('Gap2M', 0)) > 0:
                    sht.add(2)
                bucket_excess[zk] = exc
                bucket_short[zk]  = sht

            # Build per-zone candidate lists split by bucket priority:
            #   tier 0 — TP's bucket is in excess for that zone   (preferred cut)
            #   tier 1 — TP's bucket is neither excess nor short  (neutral)
            #   tier 2 — TP's bucket is in shortage               (avoid cutting)
            # Within each tier candidates are shuffled randomly.
            zone_candidates = {}   # zone -> [index, ...]  (ordered by tier then random)
            for idx in day_recs_idx:
                zone = recs.at[idx, 'sourcezone']
                if not is_deep_pool(zone):
                    continue
                men  = int(recs.at[idx, 'NUMBER_OF_MEN'])
                exc  = bucket_excess.get(zone, set())
                sht  = bucket_short.get(zone, set())
                tier = 0 if men in exc else (2 if men in sht else 1)
                zone_candidates.setdefault(zone, {0: [], 1: [], 2: []})
                zone_candidates[zone][tier].append(idx)

            # Shuffle within each tier, then flatten to ordered list per zone
            zone_ordered = {}
            for zone, tiers in zone_candidates.items():
                flat = []
                for t in (0, 1, 2):
                    random.shuffle(tiers[t])
                    flat.extend(tiers[t])
                zone_ordered[zone] = flat

            # Per-zone cap: ~13% of that zone's new recs, minimum 1
            CUT_FRACTION = 0.13
            zone_allowance = {
                z: max(1, round(len(v) * CUT_FRACTION))
                for z, v in zone_ordered.items()
            }
            zone_held = {z: 0 for z in zone_ordered}
            zone_pos  = {z: 0 for z in zone_ordered}

            # Stable zone order: London first (largest pool), then others alphabetically
            ordered_zones = sorted(
                zone_ordered.keys(),
                key=lambda z: (0 if z == 'london' else 1, z)
            )

            held = 0
            while held < needed:
                made_progress = False
                for zone in ordered_zones:
                    if held >= needed:
                        break
                    if zone_held[zone] >= zone_allowance[zone]:
                        continue
                    pos = zone_pos[zone]
                    if pos >= len(zone_ordered[zone]):
                        continue
                    idx  = zone_ordered[zone][pos]
                    men  = int(recs.at[idx, 'NUMBER_OF_MEN'])
                    exc  = bucket_excess.get(zone, set())
                    sht  = bucket_short.get(zone, set())
                    tier = 0 if men in exc else (2 if men in sht else 1)
                    tier_label = (
                        'excess bucket' if tier == 0 else
                        ('shortage bucket — last resort' if tier == 2 else 'neutral bucket')
                    )
                    recs.at[idx, 'vetting_status'] = 'HOLD_EI'
                    recs.at[idx, 'vetting_reason'] = (
                        f"EI balancing: {zone} hold "
                        f"{zone_held[zone]+1}/{zone_allowance[zone]} "
                        f"(~{round(CUT_FRACTION*100)}% cap, "
                        f"{len(zone_ordered[zone])} recs in zone, "
                        f"{men}M {tier_label}); "
                        f"total needed={needed}"
                    )
                    zone_held[zone] += 1
                    zone_pos[zone]  += 1
                    held += 1
                    made_progress = True
                if not made_progress:
                    break   # all zone allowances exhausted — cannot hold more

        # ── Layer 2: Dedup corrections ─────────────────────────────────────────
        # Work on accepted (including all new_recs not yet HOLD_EI/REMOVE) + already_accepted
        day_all_for_dedup = day_all.copy()
        day_all_for_dedup['_score']     = day_all_for_dedup.apply(sav2.score_tp, axis=1)
        day_all_for_dedup['_is_new_tp'] = day_all_for_dedup['rating'].apply(lambda r: float(r or 0) == 6.0)

        for zone, zone_idx in day_all_for_dedup.groupby('sourcezone').groups.items():
            zone_df = day_all_for_dedup.loc[zone_idx]

            # Active = accepted + new_recommendation rows not yet held
            active_mask_z = (
                (zone_df['IRES_STATUS'] == 'accepted') |
                (
                    zone_df['new_recommendation'] &
                    zone_df['ID'].apply(
                        lambda rid: recs.loc[
                            recs['ID'] == rid, 'vetting_status'
                        ].eq('ACCEPT').any() if rid in recs['ID'].values else False
                    )
                )
            )
            active = zone_df[active_mask_z]

            counts = active.groupby('USERNAME').size()
            dupes = counts[counts > 1]
            if dupes.empty:
                continue

            active_usernames = set(active['USERNAME'])

            for username, count in dupes.items():
                tp_rows = active[active['USERNAME'] == username].sort_values('_score', ascending=False)
                accepted_count  = int((tp_rows['IRES_STATUS'] == 'accepted').sum())
                keep_count      = max(0, 1 - accepted_count)
                excess_tp_rows  = tp_rows[tp_rows['new_recommendation']].iloc[keep_count:]

                for _, excess_row in excess_tp_rows.iterrows():
                    excess_id   = excess_row['ID']
                    excess_men  = int(excess_row['NUMBER_OF_MEN'])
                    excess_type = excess_row['RES_TYPE']

                    # Check if this ID is in recs (it should be)
                    if excess_id not in recs['ID'].values:
                        continue

                    # Find a replacement from pending pool
                    pending_repl = day_all_for_dedup[
                        (day_all_for_dedup['sourcezone']  == zone) &
                        (day_all_for_dedup['IRES_STATUS'] == 'pending') &
                        (~day_all_for_dedup['new_recommendation']) &
                        (~day_all_for_dedup['USERNAME'].isin(active_usernames)) &
                        (day_all_for_dedup['NUMBER_OF_MEN'] == excess_men)
                    ].sort_values(['_is_new_tp', '_score'], ascending=[False, False])

                    rec_idx = recs.index[recs['ID'] == excess_id][0]
                    if not pending_repl.empty:
                        best = pending_repl.iloc[0]
                        recs.at[rec_idx, 'vetting_status'] = 'REPLACE_DEDUP'
                        recs.at[rec_idx, 'vetting_reason']  = (
                            f"Dedup: {username} already accepted/recommended; "
                            f"replace with {best['USERNAME']} "
                            f"(rating={round(float(best.get('rating', 0)), 2)}, "
                            f"deallo={round(float(best.get('Deallo Rate Overall', 0))*100, 1)}%)"
                        )
                        active_usernames.add(best['USERNAME'])
                    else:
                        ei_eligible = zone in {'london', 'birmingham', 'manchester'}
                        recs.at[rec_idx, 'vetting_status'] = 'REMOVE_DEDUP'
                        recs.at[rec_idx, 'vetting_reason']  = (
                            f"Dedup: {username} already accepted/recommended; "
                            f"no {excess_men}-man replacement in pending pool. "
                            + ("Leave for EI." if ei_eligible else "Do not accept duplicate.")
                        )

    # ── Layer 0.6: Post-EI-quota supply gap fallback (second pass) ───────────────
    # EI reservation quotas (Layer 0.5) may hold back all new recs for a zone,
    # leaving its supply gap unfilled from a pre-acceptance perspective.
    # Run a second pass: for any EI-quota zone whose ACCEPT count is below its gap,
    # pull additional TPs from the pending pool and mark them ACCEPT.
    _pending_mask = (df['IRES_STATUS'] == 'pending') & (df['RES_TYPE'] != 'return')
    _new_rows = []

    for _pdate, _day_idx in recs.groupby(recs['DATE'].dt.date).groups.items():
        _day_recs   = recs.loc[_day_idx]
        _day_summary = zone_summary[
            zone_summary['Pickup Date'].apply(lambda d: str(d)[:10]) == str(_pdate)
        ]

        for _zone, _qcfg in EI_RESERVATION_QUOTA.items():
            _zone_recs = _day_recs[_day_recs['sourcezone'] == _zone]
            _n_accept  = int((_zone_recs['vetting_status'] == 'ACCEPT').sum())

            _zsum = _day_summary[_day_summary['_zone_key'] == _zone]
            if _zsum.empty:
                continue
            _zrow = _zsum.iloc[0]
            _gap  = int(_zrow.get('Gap', 0))
            _g1m  = int(_zrow.get('Gap1M', 0))
            _g2m  = int(_zrow.get('Gap2M', 0))

            if _n_accept >= _gap or _gap == 0:
                continue  # gap already filled by ACCEPT recs

            # Compute remaining 1M/2M gaps after existing ACCEPT recs.
            # 12M TPs: assign to the bucket with more room (same logic as main algorithm).
            _acc    = _zone_recs[_zone_recs['vetting_status'] == 'ACCEPT']
            _a1m    = int((_acc['NUMBER_OF_MEN'].apply(lambda m: int(m or 1)) == 1).sum())
            _a2m    = int((_acc['NUMBER_OF_MEN'].apply(lambda m: int(m or 1)) == 2).sum())
            _a12m_n = int((_acc['NUMBER_OF_MEN'].apply(lambda m: int(m or 1)) == 12).sum())
            _r1m = _g1m - _a1m
            _r2m = _g2m - _a2m
            for _ in range(_a12m_n):
                if _r1m > _r2m:
                    _r1m -= 1
                else:
                    _r2m -= 1
            _rem_g1m = max(0, _r1m)
            _rem_g2m = max(0, _r2m)
            if _rem_g1m + _rem_g2m == 0:
                continue

            # IDs already in recs for this zone-date (exclude from new candidates)
            _rec_ids = set(recs[
                (recs['sourcezone'] == _zone) &
                (recs['DATE'].dt.date == _pdate)
            ]['ID'])
            # Usernames already accepted or recommended (for select_v2 dedup)
            _acc_users = set(df[
                (df['sourcezone'] == _zone) &
                (df['DATE'].dt.date == _pdate) &
                ((df['IRES_STATUS'] == 'accepted') | df['new_recommendation'])
            ]['USERNAME'].str.strip())

            _cand = df[
                (df['sourcezone'] == _zone) &
                (df['DATE'].dt.date == _pdate) &
                _pending_mask &
                (~df['ID'].isin(_rec_ids)) &
                (~df['USERNAME'].str.strip().isin(_acc_users))  # hard dedup
            ].copy()
            if _cand.empty:
                continue
            _cand['_score'] = _cand.apply(sav2.score_tp, axis=1)

            # Primary: rated + capacity; fallback: capacity only (low-rating)
            _rated = _cand[
                (_cand['rating'].apply(lambda r: float(r or 0)) >= sav2.MIN_RATING) &
                (_cand['RESERVATION_CAPACITY'].apply(lambda c: float(c or 0)) >= sav2.MIN_CAPACITY)
            ]
            _is_fb = _rated.empty
            _pool  = _cand[
                _cand['RESERVATION_CAPACITY'].apply(lambda c: float(c or 0)) >= sav2.MIN_CAPACITY
            ] if _is_fb else _rated
            if _pool.empty:
                continue

            # No south quota for the fallback pass — just fill the remaining gap
            _sel   = sav2.select_v2(
                _pool, _rem_g1m, _rem_g2m, _acc_users,
                float(_zrow.get('Furn1M Jobs', 0)),
                south_quota_pct=0.0,
            )
            if _sel.empty:
                continue

            _next_rank = int(
                recs[(recs['sourcezone'] == _zone) &
                     (recs['DATE'].dt.date == _pdate)]['new_recommendation_rank'].max() or 0
            ) + 1
            for _off, _idx in enumerate(_sel.index):
                _r = df.loc[_idx].to_dict()
                _r['new_recommendation']      = True
                _r['new_recommendation_rank'] = _next_rank + _off
                _r['rating_fallback']         = _is_fb
                _r['vetting_status']          = 'ACCEPT'
                _r['vetting_reason']          = (
                    'Post-EI-quota fallback: EI reservation quota held earlier recs; '
                    'additional TP to fill supply gap'
                    + (' (low-rating fallback)' if _is_fb else '')
                )
                _r['_score']     = float(_sel.at[_idx, '_score']) if '_score' in _sel.columns else 0.0
                _r['_is_new_tp'] = float(_r.get('rating', 0) or 0) == 6.0
                _r['deallo_pct'] = round(float(_r.get('Deallo Rate Overall', 0) or 0) * 100, 1)
                _new_rows.append(_r)
                df.at[_idx, 'new_recommendation']      = True
                df.at[_idx, 'new_recommendation_rank'] = _next_rank + _off

            print(f"  [post-EI fallback] {_zone} {_pdate}: "
                  f"{len(_sel)} additional TP(s) ACCEPT after EI quota hold"
                  + (' (low-rating fallback)' if _is_fb else ''))

    if _new_rows:
        recs = pd.concat([recs, pd.DataFrame(_new_rows)], ignore_index=True)

    # ── Update zone summary: Unfilled Gap = gap − ACCEPT_count (post-quota) ──────
    # The zone summary written by supply_acceptor_v2 counted all new_recs (including
    # those later held for EI) as filling the gap. Recompute true unfilled now that
    # we know which recs are ACCEPT vs HOLD_EI, and rewrite the summary CSV.
    try:
        _zs = pd.read_csv(summary_path)
        _zs['_zk'] = (_zs['Zone']
                      .str.extract(r'^([a-z\- ]+)', expand=False)
                      .str.strip().str.lower())
        for _si, _sr in _zs.iterrows():
            _zk   = str(_sr['_zk'])
            _pd   = str(_sr['Pickup Date'])[:10]
            _gap_ = int(_sr.get('Gap', 0))
            _na   = int(recs[
                (recs['sourcezone'] == _zk) &
                (recs['DATE'].apply(lambda d: str(d)[:10]) == _pd) &
                (recs['vetting_status'] == 'ACCEPT')
            ].shape[0])
            _zs.at[_si, 'Unfilled Gap'] = max(0, _gap_ - _na)
        _zs.drop(columns=['_zk'], errors='ignore').to_csv(summary_path, index=False)
    except Exception as _e:
        print(f"[vetted] Warning: could not update zone summary unfilled gap ({_e})")

    # ── Write per-date output ──────────────────────────────────────────────────
    OUT_COLS = [
        'ID', 'DATE', 'sourcezone', 'USERNAME', 'NUMBER_OF_MEN', 'RES_TYPE',
        'consider_res_type', 'rating', 'deallo_pct', 'VAT_STATUS',
        'RESERVATION_CAPACITY', 'new_recommendation_rank',
        'vetting_status', 'vetting_reason', 'rating_fallback',
    ]

    recs = recs.sort_values(['DATE', 'sourcezone', 'new_recommendation_rank'])
    out_cols = [c for c in OUT_COLS if c in recs.columns]

    for pickup_date, group in recs.groupby(recs['DATE'].dt.date):
        csv_path = os.path.join(OUTPUT_DIR, f'vetted_tps_{pickup_date}.csv')
        group[out_cols].to_csv(csv_path, index=False)

        n_accept  = int((group['vetting_status'] == 'ACCEPT').sum())
        n_hold    = int((group['vetting_status'] == 'HOLD_EI').sum())
        n_replace = int((group['vetting_status'] == 'REPLACE_DEDUP').sum())
        n_remove  = int((group['vetting_status'] == 'REMOVE_DEDUP').sum())
        print(
            f"[vetted] {pickup_date}: "
            f"{n_accept} ACCEPT  {n_hold} HOLD_EI  "
            f"{n_replace} REPLACE_DEDUP  {n_remove} REMOVE_DEDUP  "
            f"→ {csv_path}"
        )

    recs.drop(columns=['_score', '_is_new_tp', 'deallo_pct'], inplace=True, errors='ignore')


# ── Vetted report printer ──────────────────────────────────────────────────────

def print_vetted_report(output_path: str, is_forecast: bool = False):
    """
    Prints the full vetted recommendations report after write_vetted_recommendations_csv().
    Sections:
      1. Vetted TP list grouped by zone
      2. Flags (rule-based)
      3. Vetting Summary table
    """
    import os
    import pandas as pd

    try:
        from tabulate import tabulate as _tab
        _HAS_TAB = True
    except ImportError:
        _HAS_TAB = False

    def _tabulate(rows, headers):
        if _HAS_TAB:
            return _tab(rows, headers=headers, tablefmt='simple')
        col_w = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0))
                 for i, h in enumerate(headers)]
        sep = '  '.join('-' * w for w in col_w)
        hdr = '  '.join(str(h).ljust(w) for h, w in zip(headers, col_w))
        body = '\n'.join('  '.join(str(r[i]).ljust(w) for i, w in enumerate(col_w)) for r in rows)
        return f"{hdr}\n{sep}\n{body}"

    script_dir = os.path.dirname(os.path.abspath(output_path))
    summary_path = output_path.replace('.csv', '_zone_summary.csv')
    if not os.path.exists(summary_path):
        return

    zone_summary = pd.read_csv(summary_path)
    zone_summary['_zk'] = (zone_summary['Zone']
                           .str.replace(r'\s*\[.*?\]', '', regex=True)
                           .str.replace('⚠', '', regex=False)
                           .str.strip().str.lower())

    def _zval(zrow, col, default=0):
        return zrow[col].iloc[0] if col in zrow.columns and len(zrow) else default

    for pickup_date_str in sorted(zone_summary['Pickup Date'].astype(str).str[:10].unique()):
        vetted_path = os.path.join(script_dir, f'vetted_tps_{pickup_date_str}.csv')
        if not os.path.exists(vetted_path):
            continue

        vetted = pd.read_csv(vetted_path)
        vetted['sourcezone'] = vetted['sourcezone'].str.strip().str.lower()
        vetted['USERNAME']   = vetted['USERNAME'].str.strip()
        vetted['deallo_pct'] = pd.to_numeric(vetted['deallo_pct'], errors='coerce').fillna(0.0)
        vetted['rating']     = pd.to_numeric(vetted['rating'],     errors='coerce').fillna(0.0)
        day_summary = zone_summary[zone_summary['Pickup Date'].astype(str).str[:10] == pickup_date_str]

        W = 94
        print('\n' + '=' * W)
        print(f"  VETTED RECOMMENDATIONS — {pickup_date_str}")
        print('=' * W)

        # ── Section 1: Per-zone TP tables ──────────────────────────────────────
        all_zones = sorted(vetted['sourcezone'].unique())
        for zone in all_zones:
            zv = vetted[vetted['sourcezone'] == zone].sort_values('new_recommendation_rank')
            n_acc  = int((zv['vetting_status'] == 'ACCEPT').sum())
            n_hold = int((zv['vetting_status'] == 'HOLD_EI').sum())
            n_dup  = int(zv['vetting_status'].isin(['REPLACE_DEDUP', 'REMOVE_DEDUP']).sum())
            parts = []
            if n_acc:  parts.append(f"{n_acc} ACCEPT")
            if n_hold: parts.append(f"{n_hold} HOLD_EI")
            if n_dup:  parts.append(f"{n_dup} DEDUP")
            zs = day_summary[day_summary['_zk'] == zone]
            gap   = int(_zval(zs, 'Gap'))
            unf   = int(_zval(zs, 'Unfilled Gap'))
            hdr_extra = f"  Gap={gap}" if gap else ""
            if unf: hdr_extra += f"  ⚠ Unfill={unf}"
            print(f"\n  {zone.upper()}  ({', '.join(parts)}){hdr_extra}")

            rows = []
            for _, r in zv.iterrows():
                men  = int(r['NUMBER_OF_MEN'])
                rt   = str(r.get('RES_TYPE', '')).strip().lower()
                cons = str(r.get('consider_res_type', '')).strip().lower()
                if men == 12:    tp_type = '12M'
                elif rt == 'custom': tp_type = 'Cus'
                elif rt == 'national': tp_type = 'Nat'
                else:            tp_type = 'Loc'
                vat  = 'Yes' if int(r.get('VAT_STATUS', 0) or 0) else 'No'
                stat = r['vetting_status']
                flags = ''
                if stat == 'ACCEPT':
                    if r['deallo_pct'] > 20: flags += ' ⚠ high deallo'
                    if 4.4 <= r['rating'] <= 4.5: flags += ' ⚠ low rating'
                reason = str(r.get('vetting_reason', '') or '').strip()
                # Shorten reason for display
                if 'EI reservation' in reason:
                    reason_short = f"quota hold"
                elif 'EI balancing' in reason:
                    reason_short = f"EI balancing hold"
                elif 'Dedup' in reason or 'DEDUP' in stat:
                    reason_short = reason[:60] if reason else stat
                else:
                    reason_short = ''
                rows.append([
                    int(r['ID']),
                    r['USERNAME'],
                    f"{men}M",
                    tp_type,
                    f"{r['rating']:.2f}",
                    f"{r['deallo_pct']:.1f}%",
                    vat,
                    stat + flags,
                    reason_short,
                ])
            print(_tabulate(rows, ['ID', 'Username', 'Men', 'Type', 'Rating', 'Deallo', 'VAT', 'Status', 'Note']))

        # ── Section 2: Flags ───────────────────────────────────────────────────
        flags_list = []
        flag_n = 1

        # F1: Unfilled slots
        unf_zones = day_summary[day_summary['Unfilled Gap'] > 0]
        for _, row in unf_zones.iterrows():
            zname = row['_zk']
            unf   = int(row['Unfilled Gap'])
            pend  = int(row['Pending'])
            g1m   = int(row['Gap1M'])
            g2m   = int(row['Gap2M'])
            bucket_str = []
            if g1m: bucket_str.append(f"G1M={g1m}")
            if g2m: bucket_str.append(f"G2M={g2m}")
            bkt = f" ({', '.join(bucket_str)})" if bucket_str else ''
            pool_note = f"Pend={pend} TP{'s' if pend != 1 else ''}" if pend > 0 else "pool empty"
            flags_list.append(
                (flag_n, f"Unfilled — {zname}: Unfill={unf}{bkt}, {pool_note}")
            )
            flag_n += 1

        # F2: London 1M shortfall
        lon_s = day_summary[day_summary['_zk'] == 'london']
        if not lon_s.empty:
            lon = lon_s.iloc[0]
            tgt1m  = int(lon['Tgt1M'])
            a1m    = int(lon['Acc1M'])
            a12to1 = int(lon['Acc12M_to_1M'])
            lon_v  = vetted[(vetted['sourcezone'] == 'london') & (vetted['vetting_status'] == 'ACCEPT')]
            new_1m = int(lon_v[lon_v['NUMBER_OF_MEN'].isin([1, 12])].shape[0])
            eff_1m = a1m + a12to1 + new_1m
            if eff_1m < tgt1m:
                flags_list.append((flag_n, f"London 1M shortfall — Tgt1M={tgt1m}, effective coverage={eff_1m} (A1M={a1m} + Acc12M→1M={a12to1} + new accepted={new_1m})"))
                flag_n += 1

        # F3: High deallo (>20%) in ACCEPT recs
        hi_d = vetted[(vetted['vetting_status'] == 'ACCEPT') & (vetted['deallo_pct'] > 20)]
        if not hi_d.empty:
            lines = [f"{r['USERNAME']} ({r['sourcezone']}, {r['deallo_pct']:.1f}%)" for _, r in hi_d.iterrows()]
            flags_list.append((flag_n, f"High deallo in ACCEPT recs (>20%): {', '.join(lines)}"))
            flag_n += 1

        # F4: Low rating (4.4-4.5) in ACCEPT recs
        lo_r = vetted[(vetted['vetting_status'] == 'ACCEPT') & (vetted['rating'].between(4.4, 4.51))]
        if not lo_r.empty:
            lines = [f"{r['USERNAME']} ({r['sourcezone']}, {r['rating']:.2f})" for _, r in lo_r.iterrows()]
            flags_list.append((flag_n, f"Low rating in ACCEPT recs (4.4–4.5): {', '.join(lines)}"))
            flag_n += 1

        # F5: Zones where all new recs were held (0 net accepted)
        quota_zones = set(EI_RESERVATION_QUOTA.keys())
        for zone in all_zones:
            zv = vetted[vetted['sourcezone'] == zone]
            zs = day_summary[day_summary['_zk'] == zone]
            new_acc  = int(_zval(zs, 'Newly Accepted'))
            n_accept = int((zv['vetting_status'] == 'ACCEPT').sum())
            n_hold   = int((zv['vetting_status'] == 'HOLD_EI').sum())
            gap      = int(_zval(zs, 'Gap'))
            pend     = int(_zval(zs, 'Pending'))
            if new_acc > 0 and n_accept == 0 and n_hold > 0 and gap > 0:
                reason = "EI quota" if zone in quota_zones else "EI balancing"
                flags_list.append((flag_n, f"{zone}: 0 net accepted — all {n_hold} new rec{'s' if n_hold != 1 else ''} held ({reason}); Gap={gap}, Pend={pend}"))
                flag_n += 1

        # F6: Dry pool zones with gap (0 pending, Gap > 0) — no new recs flagged above
        dry = day_summary[(day_summary['Pending'] == 0) & (day_summary['Gap'] > 0)]
        for _, row in dry.iterrows():
            zname = row['_zk']
            gap   = int(row['Gap'])
            g1m   = int(row['Gap1M'])
            g2m   = int(row['Gap2M'])
            # Only flag if not already covered by unfill flag
            if int(row['Unfilled Gap']) == 0:
                bkt = f" (G1M={g1m}, G2M={g2m})" if g1m or g2m else ''
                flags_list.append((flag_n, f"{zname}: Gap={gap}{bkt} but 0 TPs pending — EI must cover entirely"))
                flag_n += 1

        # F7: EI quota slip-through (ACCEPT TP has worse deallo than a HOLD_EI TP in same zone)
        for zone in all_zones:
            zv = vetted[vetted['sourcezone'] == zone]
            acc_tps  = zv[zv['vetting_status'] == 'ACCEPT']
            hold_tps = zv[zv['vetting_status'] == 'HOLD_EI']
            if acc_tps.empty or hold_tps.empty:
                continue
            worst_acc  = acc_tps.loc[acc_tps['deallo_pct'].idxmax()]
            best_hold  = hold_tps.loc[hold_tps['deallo_pct'].idxmin()]
            if worst_acc['deallo_pct'] > best_hold['deallo_pct'] and worst_acc['deallo_pct'] > 15:
                flags_list.append((flag_n,
                    f"{zone} EI slip-through: {worst_acc['USERNAME']} accepted ({worst_acc['deallo_pct']:.1f}% deallo) "
                    f"while {best_hold['USERNAME']} held ({best_hold['deallo_pct']:.1f}% deallo) — "
                    f"consider swapping"
                ))
                flag_n += 1

        # F8: Manchester / persistent UZUU2018-type double-accepted TPs (flagged by dedup report, echo here)
        for zone in all_zones:
            zv = vetted[vetted['sourcezone'] == zone]
            # If any HOLD_EI reason mentions 'already accepted' it was caught by dedup; just note it
            dup_held = zv[zv['vetting_reason'].str.contains('already accepted', na=False, case=False) & (zv['vetting_status'] == 'HOLD_EI')]
            if not dup_held.empty:
                for _, r in dup_held.iterrows():
                    flags_list.append((flag_n, f"{zone}: {r['USERNAME']} has multiple accepted reservations — ops awareness only"))
                    flag_n += 1

        print('\n' + '-' * W)
        print('  FLAGS')
        print('-' * W)
        if flags_list:
            for n, msg in flags_list:
                print(f"  {n}. {msg}")
        else:
            print('  No flags — all zones healthy.')

        # ── Section 3: Vetting Summary ─────────────────────────────────────────
        summary_rows = []
        for zone in sorted(day_summary['_zk'].unique()):
            zs   = day_summary[day_summary['_zk'] == zone]
            zv   = vetted[vetted['sourcezone'] == zone]
            gap  = int(_zval(zs, 'Gap'))
            unf  = int(_zval(zs, 'Unfilled Gap'))
            pend = int(_zval(zs, 'Pending'))
            new_acc_algo = int(_zval(zs, 'Newly Accepted'))
            n_acc  = int((zv['vetting_status'] == 'ACCEPT').sum())
            n_hold = int((zv['vetting_status'] == 'HOLD_EI').sum())
            g1m  = int(_zval(zs, 'Gap1M'))
            g2m  = int(_zval(zs, 'Gap2M'))

            concern = ''
            action  = ''

            if unf > 0 and pend == 0:
                concern = f"Unfill={unf} — pool empty"
                action  = "EI must cover — flag ops"
            elif unf > 0 and pend > 0:
                concern = f"Unfill={unf}, {pend} TPs pending"
                action  = "EI fallback; check at D-2"
            elif new_acc_algo > 0 and n_acc == 0 and n_hold > 0 and gap > 0:
                quota_zones = set(EI_RESERVATION_QUOTA.keys())
                reason = "quota" if zone in quota_zones else "EI balancing"
                concern = f"0 net accepted — {n_hold} held ({reason})"
                action  = f"Deep pool ({pend} pend) — revisit D-2" if pend >= 3 else f"EI covers; monitor"
            elif gap > 0 and pend == 0:
                g_str = []
                if g1m: g_str.append(f"G1M={g1m}")
                if g2m: g_str.append(f"G2M={g2m}")
                concern = f"Gap={gap} ({', '.join(g_str)}) — dry pool"
                action  = "EI covers"
            elif gap > 0 and n_acc > 0:
                concern = f"Partially filled — Gap={gap} remains"
                action  = "EI covers remainder"
            else:
                # Check for quality flags
                hi_d_z = zv[(zv['vetting_status'] == 'ACCEPT') & (zv['deallo_pct'] > 20)]
                if not hi_d_z.empty:
                    worst = hi_d_z.loc[hi_d_z['deallo_pct'].idxmax()]
                    concern = f"{worst['USERNAME']} accepted ({worst['deallo_pct']:.0f}% deallo)"
                    action  = "Consider manual swap"
                elif n_acc > 0 and gap == 0:
                    concern = "Covered"
                    action  = "No action"

            if concern:
                summary_rows.append([zone, concern, action])

        print('\n' + '-' * W)
        print('  VETTING SUMMARY')
        print('-' * W)
        if summary_rows:
            # indent each row of tabulate output
            tbl = _tabulate(summary_rows, ['Zone', 'Concern', 'Action'])
            for line in tbl.splitlines():
                print('  ' + line)
        else:
            print('  All zones covered — no concerns.')
        print()


# ── Shared output loader ───────────────────────────────────────────────────────

def _load_output_csv(path: str):
    import pandas as pd
    df = pd.read_csv(path, low_memory=False)
    df = df.loc[:, ~df.columns.str.startswith('Unnamed')]
    df['DATE']               = pd.to_datetime(df['DATE'], dayfirst=True, format='mixed')
    df['USERNAME']           = df['USERNAME'].str.strip()
    df['sourcezone']         = df['sourcezone'].str.strip().str.lower()
    df['IRES_STATUS']        = df['IRES_STATUS'].str.strip().str.lower()
    df['new_recommendation'] = df['new_recommendation'].fillna(False).astype(bool)
    df['NUMBER_OF_MEN']      = df['NUMBER_OF_MEN'].apply(lambda m: int(m) if __import__('pandas').notna(m) else 1)
    df['RES_TYPE']           = df['RES_TYPE'].str.strip().str.lower()
    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    dry_run = '--dry-run' in args
    if dry_run:
        args = [a for a in args if a != '--dry-run']

    if not args:
        args = [(date.today() + timedelta(days=1)).strftime('%Y-%m-%d')]

    uk_now = get_uk_now()
    print(f"[integrated-v2] Current UK time : {uk_now.strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"[integrated-v2] Pickup date(s)  : {args}\n")

    actuals_dates  = []
    forecast_dates = []

    for d_str in args:
        pickup = date.fromisoformat(d_str)
        mode, days_out = decide(pickup, uk_now)
        label = f"D-{days_out}" if days_out >= 0 else f"D+{abs(days_out)}"
        print(f"  {d_str}  ({label})  →  {mode.upper()}")
        if mode == 'actuals':
            actuals_dates.append(d_str)
        else:
            forecast_dates.append(d_str)

    if dry_run:
        print("\n[dry-run] No data fetched or algorithm run.")
        return

    print()

    for d_str in actuals_dates:
        print(f"[integrated-v2] → ACTUALS run for: {d_str}")
        output_path = run_actuals_v2([d_str])
        if output_path:
            report_tp_duplicates(output_path)
            write_recommendations_csv(output_path)
            write_vetted_recommendations_csv(output_path)
            print_vetted_report(output_path, is_forecast=False)

    for d_str in forecast_dates:
        print(f"[integrated-v2] → FORECAST run for: {d_str}")
        output_path = run_forecast_v2([d_str])
        report_tp_duplicates(output_path)
        write_recommendations_csv(output_path)
        write_vetted_recommendations_csv(output_path)
        print_vetted_report(output_path, is_forecast=True)


if __name__ == '__main__':
    main()
