"""
Integrated Supply Acceptor
──────────────────────────
Auto-selects mode (actuals vs forecast) and JPJ multiplier based on how many
days before pickup the script is run, and the current UK time.

Decision logic (per pickup date P, D = days until pickup):
  D-1  (any time)            → actuals,  JPJ ×1.0
  D-2  before 09:00 UK       → forecast, JPJ ×1.2
  D-2  from   09:00 UK       → actuals,  JPJ ×1.0
  D-3                        → forecast, JPJ ×1.2
  D-4  or earlier            → forecast, JPJ ×1.2

The higher multiplier further out means fewer journeys are targeted, so fewer
TPs are accepted — intentional conservatism when the forecast may be off by ~20%.

Usage:
    python3 integrated_supply_acceptor.py 2026-03-29
    python3 integrated_supply_acceptor.py 2026-03-29 2026-03-30
    python3 integrated_supply_acceptor.py --dry-run 2026-03-29
"""

import sys, os, glob, importlib
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR   = os.path.dirname(SCRIPT_DIR)
FORECAST_DIR = os.path.join(PARENT_DIR, 'updated_forecast', 'production_v5')
UK_TZ        = ZoneInfo('Europe/London')

# ── S3 forecast source ─────────────────────────────────────────────────────────
FORECAST_S3_BUCKET = 'supply-acceptor-data-production'
FORECAST_S3_PREFIX = 'demand-forecast/'

# Birmingham and Manchester always use this higher multiplier
ZONE_HIGH_MULTIPLIER = 1.3
ZONE_HIGH_SET        = {'birmingham', 'manchester'}

# Zones with enough EI volume to absorb a duplicate TP.
# In all other zones the dedup report advises "do not accept" rather than "leave for EI".
EI_ELIGIBLE_ZONES = {'london', 'birmingham', 'manchester'}

# London multiplier is date-dependent:
#   last 4 days of the month (end-of-month uplift) → 1.3
#   all other dates                                 → 1.5
LONDON_EOM_MULTIPLIER    = 1.3
LONDON_NORMAL_MULTIPLIER = 1.5


def get_london_multiplier(d_str: str) -> float:
    import calendar
    d = date.fromisoformat(d_str)
    days_in_month = calendar.monthrange(d.year, d.month)[1]
    if d.day >= days_in_month - 3:   # last 4 days
        return LONDON_EOM_MULTIPLIER
    return LONDON_NORMAL_MULTIPLIER


# ── Mode/multiplier decision ───────────────────────────────────────────────────

def get_uk_now() -> datetime:
    return datetime.now(UK_TZ)


def decide(pickup_date: date, uk_now: datetime) -> tuple:
    """
    Returns (mode, jpj_multiplier, days_out) for a given pickup date.
      mode           : 'actuals' or 'forecast'
      jpj_multiplier : 1.0 or 1.2
      days_out       : integer days until pickup
    """
    days_out = (pickup_date - uk_now.date()).days
    uk_hour  = uk_now.hour + uk_now.minute / 60.0

    if days_out <= 1:
        return 'actuals', 1.0, days_out
    elif days_out == 2:
        if uk_hour < 9.0:
            return 'forecast', 1.2, days_out
        else:
            return 'actuals', 1.0, days_out
    elif days_out == 3:
        return 'forecast', 1.2, days_out
    else:
        return 'forecast', 1.2, days_out


# ── Actuals mode ──────────────────────────────────────────────────────────────

def run_actuals(dates: list) -> str | None:
    """Delegates straight to fetch_and_run.py — no JPJ changes needed.
    Returns the path of the output CSV, or None if it cannot be determined."""
    import subprocess
    before = set(glob.glob(os.path.join(SCRIPT_DIR, 'supply_acceptor_output_*.csv')))
    cmd    = [sys.executable, os.path.join(SCRIPT_DIR, 'fetch_and_run.py')] + dates
    subprocess.run(cmd, capture_output=False)
    after  = set(glob.glob(os.path.join(SCRIPT_DIR, 'supply_acceptor_output_*.csv')))
    new    = after - before
    return max(new, key=os.path.getmtime) if new else None


# ── S3 forecast sync ──────────────────────────────────────────────────────────

def sync_forecast_from_s3(dest_dir: str) -> bool:
    """
    Downloads the two latest V5 cluster lt200km forecast files from S3 into
    dest_dir, replacing any older copies.

    S3 structure:
        s3://supply-acceptor-data-production/demand-forecast/YYYY-MM-DD/
            v5_furniture_cluster_lt200km_YYYY-MM-DD.csv
            v5_removals_cluster_lt200km_YYYY-MM-DD.csv

    Returns True if files were downloaded successfully, False on any failure
    (caller falls back to local files already present in dest_dir).
    """
    try:
        import boto3
    except ImportError:
        print("[forecast-s3] boto3 not installed — using local forecast files")
        return False

    try:
        s3 = boto3.client('s3', region_name='eu-west-1')

        # List all date folders, pick the most recent
        resp = s3.list_objects_v2(
            Bucket=FORECAST_S3_BUCKET,
            Prefix=FORECAST_S3_PREFIX,
            Delimiter='/'
        )
        folders = [p['Prefix'] for p in resp.get('CommonPrefixes', [])]
        if not folders:
            print("[forecast-s3] No forecast folders found in S3 — using local files")
            return False

        latest = sorted(folders)[-1]           # e.g. 'demand-forecast/2026-03-31/'
        run_date = latest.rstrip('/').split('/')[-1]

        target_files = [
            f'v5_furniture_cluster_lt200km_{run_date}.csv',
            f'v5_removals_cluster_lt200km_{run_date}.csv',
        ]

        os.makedirs(dest_dir, exist_ok=True)
        downloaded = []
        for fname in target_files:
            s3_key   = latest + fname
            local_path = os.path.join(dest_dir, fname)
            # Skip if already present (same run date)
            if os.path.exists(local_path):
                downloaded.append(fname)
                continue
            s3.download_file(FORECAST_S3_BUCKET, s3_key, local_path)
            downloaded.append(fname)
            print(f"[forecast-s3] Downloaded {fname}")

        if len(downloaded) == len(target_files):
            print(f"[forecast-s3] Using forecast run: {run_date}")
            return True
        else:
            print("[forecast-s3] Some files missing in S3 — using local files")
            return False

    except Exception as e:
        print(f"[forecast-s3] S3 sync failed ({e}) — using local forecast files")
        return False


# ── Forecast mode with JPJ patching ───────────────────────────────────────────

def run_forecast(dates: list, multiplier: float) -> str:
    """
    1. Fetches reservations from Snowflake (via fetch_and_run helpers).
    2. Reloads supply_acceptor_forecast to get a clean module state.
    3. Patches all JPJ constants by the multiplier.
    4. Calls supply_acceptor_forecast.run() directly.
    Returns the path of the output CSV.
    """
    import fetch_and_run as far

    # Pull latest forecast from S3 (falls back to local if unavailable)
    sync_forecast_from_s3(FORECAST_DIR)

    stamp       = datetime.now().strftime('%Y-%m-%d_%H%M')
    res_file    = os.path.join(SCRIPT_DIR, f'recommended_reservations_{stamp}.csv')
    output_path = os.path.join(SCRIPT_DIR, f'supply_acceptor_forecast_output_{stamp}.csv')

    # ── Fetch ─────────────────────────────────────────────────────────────────
    print("\nConnecting to Snowflake (browser auth)...")
    conn = far.get_conn()

    print("\nFetching TP quality...")
    tp_quality = far.fetch_tp_quality(conn)

    print("\nFetching reservations...")
    res = far.fetch_reservations(dates, conn, tp_quality)
    res.to_csv(res_file, index=False)
    print(f"  → {res_file}")

    conn.close()

    # ── Patch JPJ ─────────────────────────────────────────────────────────────
    # Reload to guarantee a clean state (handles multiple runs in same session)
    import supply_acceptor_forecast as saf
    importlib.reload(saf)

    orig = {
        'DENSE_JPJ':             saf.DENSE_JPJ,
        'LIGHT_JPJ':             saf.LIGHT_JPJ,
        'LONDON_1MAN_JPJ':       saf.LONDON_1MAN_JPJ,
        'LONDON_OVERSUPPLY_JPJ': saf.LONDON_OVERSUPPLY_JPJ,
        'ZONE_JPJ_OVERRIDES':    dict(saf.ZONE_JPJ_OVERRIDES),
    }

    saf.DENSE_JPJ             = round(saf.DENSE_JPJ             * multiplier, 4)
    saf.LIGHT_JPJ             = round(saf.LIGHT_JPJ             * multiplier, 4)
    saf.LONDON_1MAN_JPJ       = round(saf.LONDON_1MAN_JPJ       * multiplier, 4)
    saf.LONDON_OVERSUPPLY_JPJ = round(saf.LONDON_OVERSUPPLY_JPJ * multiplier, 4)
    for zone in list(saf.ZONE_JPJ_OVERRIDES.keys()):
        saf.ZONE_JPJ_OVERRIDES[zone] = round(saf.ZONE_JPJ_OVERRIDES[zone] * multiplier, 4)

    # ── Zone-specific higher multipliers ──────────────────────────────────────
    # Birmingham + Manchester: flat multiplier
    for zone in ZONE_HIGH_SET:
        base = orig['ZONE_JPJ_OVERRIDES'].get(zone, orig['DENSE_JPJ'])
        saf.ZONE_JPJ_OVERRIDES[zone] = round(base * ZONE_HIGH_MULTIPLIER, 4)

    # London: date-dependent multiplier (single date per run)
    london_mult = get_london_multiplier(dates[0])
    saf.ZONE_JPJ_OVERRIDES['london'] = round(orig['ZONE_JPJ_OVERRIDES'].get('london', orig['DENSE_JPJ']) * london_mult, 4)

    print(f"\n[integrated] JPJ multipliers: base ×{multiplier} | birmingham/manchester ×{ZONE_HIGH_MULTIPLIER} | london ×{london_mult}")
    print(f"  DENSE  : {orig['DENSE_JPJ']} → {saf.DENSE_JPJ}  (×{multiplier})")
    print(f"  LIGHT  : {orig['LIGHT_JPJ']} → {saf.LIGHT_JPJ}  (×{multiplier})")
    print(f"  London : {orig['ZONE_JPJ_OVERRIDES'].get('london')} → {saf.ZONE_JPJ_OVERRIDES.get('london')}  (×{london_mult})")
    print(f"  Birm   : {orig['DENSE_JPJ']} → {saf.ZONE_JPJ_OVERRIDES.get('birmingham')}  (×{ZONE_HIGH_MULTIPLIER})")
    print(f"  Manc   : {orig['DENSE_JPJ']} → {saf.ZONE_JPJ_OVERRIDES.get('manchester')}  (×{ZONE_HIGH_MULTIPLIER})")
    print(f"  1-man  : {orig['LONDON_1MAN_JPJ']} → {saf.LONDON_1MAN_JPJ}")
    print(f"  OvrSup : {orig['LONDON_OVERSUPPLY_JPJ']} → {saf.LONDON_OVERSUPPLY_JPJ}")

    # ── Run ───────────────────────────────────────────────────────────────────
    print(f"\nRunning supply acceptor (forecast mode, ×{multiplier})...")
    saf.run(FORECAST_DIR, res_file, output_path=output_path)

    return output_path


# ── Post-processing: TP deduplication report ──────────────────────────────────

def report_tp_duplicates(output_path: str):
    """
    Analyses the output CSV and highlights any TP that appears more than once
    on the same day (across already-accepted + newly recommended).

    Cap = 1 per TP per day. For each excess slot:
      1. Search for a same-type (NUMBER_OF_MEN) replacement from pending pool.
         New TPs (rating == 6.00) are prioritised first, then ranked by score.
      2. If no replacement exists → recommend dropping the slot and leaving it
         for EI, rather than accepting the same TP twice.

    This is a read-only vetting report — the output CSV is NOT modified.
    """
    import pandas as pd
    import supply_acceptor_forecast as saf

    df = pd.read_csv(output_path, low_memory=False)
    df = df.loc[:, ~df.columns.str.startswith('Unnamed')]
    df['DATE']             = pd.to_datetime(df['DATE'], dayfirst=True, format='mixed')
    df['USERNAME']         = df['USERNAME'].str.strip()
    df['sourcezone']       = df['sourcezone'].str.strip().str.lower()
    df['IRES_STATUS']      = df['IRES_STATUS'].str.strip().str.lower()
    df['new_recommendation'] = df['new_recommendation'].fillna(False).astype(bool)
    df['NUMBER_OF_MEN']    = df['NUMBER_OF_MEN'].apply(lambda m: int(m) if pd.notna(m) else 1)
    df['RES_TYPE']         = df['RES_TYPE'].str.strip().str.lower()

    # Re-score for ranking replacements
    df['_score']   = df.apply(saf.score_tp, axis=1)
    # Flag new TPs (no rating history — placeholder 6.00)
    df['_is_new_tp'] = df['rating'].apply(lambda r: float(r or 0) == 6.0)

    active_mask = (df['IRES_STATUS'] == 'accepted') | df['new_recommendation']

    any_duplicates = False

    for (date_val, zone), group_idx in df.groupby(['DATE', 'sourcezone']).groups.items():
        group   = df.loc[group_idx]
        active  = group[active_mask.loc[group_idx]]

        if active.empty:
            continue

        # Count appearances per USERNAME in active set
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

            # Only recommended slots can be replaced — accepted slots are fixed
            replaceable = tp_rows[tp_rows['new_recommendation']]

            # How many recommended slots to keep: max(0, 1 - accepted_count)
            # If already accepted once, all recommended slots are excess.
            # If not accepted, keep the best recommended, replace the rest.
            keep_count   = max(0, 1 - accepted_count)
            excess_rows  = replaceable.iloc[keep_count:]

            if excess_rows.empty:
                print(f"    Both slots already accepted — cannot replace either.")
                continue

            # Current active usernames — used to exclude from replacement pool
            active_usernames = set(active['USERNAME'])

            for _, excess_row in excess_rows.iterrows():
                excess_men  = int(excess_row['NUMBER_OF_MEN'])
                excess_type = excess_row['RES_TYPE']

                # Pending, not active, for this zone+date — must match NUMBER_OF_MEN exactly.
                # New TPs (rating == 6.00) are sorted first to give them priority.
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
                    best     = pending.iloc[0]
                    repl_rat = round(float(best.get('rating', 0)), 2)
                    repl_dal = f"{round(float(best.get('Deallo Rate Overall', 0)) * 100, 1)}%"
                    repl_cap = float(best.get('RESERVATION_CAPACITY', 0) or 0)
                    new_flag = '  ★ new TP' if bool(best['_is_new_tp']) else ''

                    quality_warnings = []
                    if repl_rat < 4.2 and not bool(best['_is_new_tp']):
                        quality_warnings.append(f'rating {repl_rat} < 4.2')
                    if repl_cap < 8:
                        quality_warnings.append(f'capacity {int(repl_cap)} < 8 cubes')
                    quality_flag = f'  ⚠ LOW QUALITY ({", ".join(quality_warnings)})' if quality_warnings else ''

                    print(f"    Slot ({excess_men}-man, {excess_type})  →  ✓ Replace with: "
                          f"{best['USERNAME']}  ({excess_men}-man, {best['RES_TYPE']}, "
                          f"rating={repl_rat}, deallo={repl_dal}, cap={int(repl_cap)}){new_flag}{quality_flag}")
                    active_usernames.add(best['USERNAME'])

    if not any_duplicates:
        print("\n[dedup] No TP duplicates found — all TPs appear at most once per day.")

    df.drop(columns=['_score', '_is_new_tp'], inplace=True, errors='ignore')


# ── Post-processing: write recommendations CSV ────────────────────────────────

def write_recommendations_csv(output_path: str):
    """
    Reads the output CSV and writes the newly recommended rows in the same
    format as the reservations input CSV (reservations columns only, no extras).

    Output: recommended_tps_YYYY-MM-DD.csv  (one file per pickup date in the run)
    """
    import pandas as pd

    RES_COLS = ['DATE', 'ID', 'USERNAME', 'IRES_STATUS', 'RES_TYPE', 'NUMBER_OF_MEN',
                'START_POSTCODE', 'RESERVATION_CAPACITY', 'HOURS_AVAILABLE', 'sourcezone',
                'consider_res_type', 'rating', 'Deallo Rate', 'Deallo Rate Overall', 'VAT_STATUS']

    df = pd.read_csv(output_path, low_memory=False)
    df = df.loc[:, ~df.columns.str.startswith('Unnamed')]
    df['new_recommendation'] = df['new_recommendation'].fillna(False).astype(bool)

    recs = df[df['new_recommendation']].copy()
    if recs.empty:
        print("[recommendations] No new recommendations to write.")
        return

    recs['DATE'] = pd.to_datetime(recs['DATE'], dayfirst=True, format='mixed')
    recs = recs.sort_values(['DATE', 'sourcezone', 'new_recommendation_rank'])

    out_cols = [c for c in RES_COLS if c in recs.columns]
    out = recs[out_cols]

    for pickup_date, group in out.groupby(out['DATE'].dt.date):
        csv_path = os.path.join(SCRIPT_DIR, f'recommended_tps_{pickup_date}.csv')
        group.to_csv(csv_path, index=False)
        print(f"[recommendations] Written {len(group)} row(s)  →  {csv_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    dry_run = '--dry-run' in args
    if dry_run:
        args = [a for a in args if a != '--dry-run']

    if not args:
        # Default: tomorrow
        args = [(date.today() + timedelta(days=1)).strftime('%Y-%m-%d')]

    uk_now = get_uk_now()
    print(f"[integrated] Current UK time : {uk_now.strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"[integrated] Pickup date(s)  : {args}\n")

    # ── Classify each date ────────────────────────────────────────────────────
    actuals_dates   = []
    forecast_groups = {}  # multiplier → [date_str, ...]

    for d_str in args:
        pickup = date.fromisoformat(d_str)
        mode, mult, days_out = decide(pickup, uk_now)
        label = f"D-{days_out}" if days_out >= 0 else f"D+{abs(days_out)}"
        print(f"  {d_str}  ({label})  →  {mode.upper()},  JPJ ×{mult}")
        if mode == 'actuals':
            actuals_dates.append(d_str)
        else:
            forecast_groups.setdefault(mult, []).append(d_str)

    if dry_run:
        print("\n[dry-run] No data fetched or algorithm run.")
        return

    print()

    # ── Execute (each date individually, then aggregate) ──────────────────────
    forecast_dates = [d for dates in forecast_groups.values() for d in dates]

    for d_str in actuals_dates:
        print(f"[integrated] → ACTUALS run for: {d_str}")
        output_path = run_actuals([d_str])
        if output_path:
            report_tp_duplicates(output_path)
            write_recommendations_csv(output_path)

    for d_str in forecast_dates:
        pickup = date.fromisoformat(d_str)
        _, mult, _ = decide(pickup, uk_now)
        print(f"[integrated] → FORECAST (×{mult}) run for: {d_str}")
        output_path = run_forecast([d_str], mult)
        report_tp_duplicates(output_path)
        write_recommendations_csv(output_path)


if __name__ == '__main__':
    main()
