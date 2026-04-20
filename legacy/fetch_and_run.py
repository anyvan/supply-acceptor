"""
Supply Acceptor — Fetch data from Snowflake and run the algorithm.

Usage:
    python3 fetch_and_run.py 2026-03-22
    python3 fetch_and_run.py 2026-03-22 2026-03-23
    python3 fetch_and_run.py          # defaults to tomorrow

Forecast mode (reservations only — demand comes from forecast files):
    python3 fetch_and_run.py --forecast 2026-03-24
    python3 fetch_and_run.py --forecast 2026-03-24 2026-03-25

    In forecast mode:
      - Only reservations + TP quality are fetched from Snowflake (no demand query)
      - supply_acceptor_forecast.py is run instead of supply_acceptor.py
      - Demand is read from the latest v2_*_cluster_*.csv in the production forecast folder
"""

import sys, os, subprocess
import pandas as pd
import numpy as np
import snowflake.connector
from datetime import date, timedelta

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR    = os.path.dirname(SCRIPT_DIR)
FORECAST_DIR  = os.path.join(PARENT_DIR, 'updated_forecast', 'production')

CLUSTERS_CSV = os.path.join(PARENT_DIR, 'centers for cluster info - Final_clusters.csv')
LNDATA_CSV   = os.path.join(PARENT_DIR, 'localnational.csv')

# ── Snowflake connection ───────────────────────────────────────────────────────
def get_conn():
    return snowflake.connector.connect(
        user="salmanmemon.external@anyvan.com",
        authenticator="externalbrowser",
        account="pu40889.eu-west-1",
        role="MART_ROUTE_OPT_GROUP",
        warehouse="MART_ROUTE_OPT_WH",
        database="HARMONISED",
        schema="PRODUCTION",
    )

def sf_query(conn, sql) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute(sql)
    cols = [c[0].upper() for c in cur.description]
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

# ── Reference data ────────────────────────────────────────────────────────────
UK_LAT_MIN, UK_LAT_MAX = 49.809432, 58.700162
UK_LNG_MIN, UK_LNG_MAX = -7.092700, 2.133975

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
        result[key] = row['allocatedcity'].lower().strip()
    return result

def load_lndata() -> pd.DataFrame:
    raw = pd.read_csv(LNDATA_CSV, header=None)
    cities = [c.lower().strip() for c in raw.iloc[0].tolist()]
    data = raw.iloc[1:].reset_index(drop=True)
    return pd.DataFrame(data.values, index=cities, columns=cities)

# ── Demand ────────────────────────────────────────────────────────────────────
NUTS_FILTER = """
    ((sn.nuts118cd IN ('UKC','UKD','UKE','UKF','UKG','UKH','UKI','UKL','UKK')
      OR sn.nuts218cd IN ('UKJ1','UKJ2','UKJ4','UKM7','UKM8','UKM9','UKK1','UKK2','UKK4')
      OR sn.nuts318cd IN ('UKJ31','UKJ32','UKJ35','UKJ36','UKJ37'))
     AND
     (en.nuts118cd IN ('UKC','UKD','UKE','UKF','UKG','UKH','UKI','UKL','UKK')
      OR en.nuts218cd IN ('UKJ1','UKJ2','UKJ4','UKM7','UKM8','UKM9','UKK1','UKK2','UKK4')
      OR en.nuts318cd IN ('UKJ31','UKJ32','UKJ35','UKJ36','UKJ37')))
"""

def fetch_demand(dates: list, conn) -> pd.DataFrame:
    cluster_map = load_cluster_map()
    lndata      = load_lndata()
    date_strs   = ', '.join(f"'{d}'" for d in dates)

    sql = f"""
    SELECT DISTINCT
        t.LISTING_ID,
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
      AND ca.IDENT IN ('general_goods_move', 'house_move')
    """
    print("  Fetching demand...")
    raw = sf_query(conn, sql)
    print(f"  Raw jobs: {len(raw)}")

    for col in ['START_LAT', 'START_LNG', 'END_LAT', 'END_LNG']:
        raw[col] = pd.to_numeric(raw[col], errors='coerce')
    raw['MANS'] = pd.to_numeric(raw['MANS'], errors='coerce').fillna(1).astype(int)

    # UK bounding box
    df = raw[
        (raw['START_LAT'].between(UK_LAT_MIN, UK_LAT_MAX)) &
        (raw['START_LNG'].between(UK_LNG_MIN, UK_LNG_MAX)) &
        (raw['END_LAT'].between(UK_LAT_MIN, UK_LAT_MAX)) &
        (raw['END_LNG'].between(UK_LNG_MIN, UK_LNG_MAX))
    ].copy()

    # Zone mapping
    df['start_key'] = df['START_LAT'].apply(fmt) + ':' + df['START_LNG'].apply(fmt)
    df['end_key']   = df['END_LAT'].apply(fmt)   + ':' + df['END_LNG'].apply(fmt)
    df['sourcezone']      = df['start_key'].map(cluster_map)
    df['destinationzone'] = df['end_key'].map(cluster_map)
    before = len(df)
    df = df.dropna(subset=['sourcezone', 'destinationzone'])
    print(f"  Zone mapping: {len(df)}/{before} matched")

    # Local/national classification
    def get_ln(src, dst):
        try:
            return lndata.loc[src, dst]
        except KeyError:
            return 'local'
    df['local_national'] = df.apply(lambda r: get_ln(r['sourcezone'], r['destinationzone']), axis=1)

    df['sourcezone'] = df['sourcezone'].map(ZONE_MAP).fillna(df['sourcezone'])

    demand = (
        df.groupby(['sourcezone', 'MANS', 'local_national', 'PICKUP_DATE'])
        .size()
        .reset_index(name='realized lane level jobs')
        .rename(columns={'MANS': 'number_of_men', 'PICKUP_DATE': 'pickup day'})
    )
    demand['number_of_men'] = demand['number_of_men'].astype(int)
    demand = demand[demand['number_of_men'].isin([1, 2])]

    totals = demand.groupby('pickup day')['realized lane level jobs'].sum().to_dict()
    print(f"  Demand totals: {totals}")
    return demand

# ── TP Quality ────────────────────────────────────────────────────────────────
def fetch_tp_quality(conn) -> pd.DataFrame:
    sql = """
    WITH dl AS (
        SELECT
            lp.PROVIDER_ID,
            u.NICKNAME,
            l.category_id,
            COUNT(DISTINCT lp.LISTING_ID) AS assigned_listings,
            COUNT(lp.DE_ALLOCATE_REASON_IDENTIFIER) AS all_deallo_count
        FROM harmonised.production.listing l
        LEFT JOIN harmonised.production.listing_provider lp ON l.listing_id = lp.listing_id
        LEFT JOIN harmonised.production.USER u ON lp.PROVIDER_ID = u.USER_ID
        LEFT JOIN harmonised.staging.category ct ON l.category_id = ct.category_id
        WHERE l.PICKUP_DATE > DATEADD(MONTH, -3, CURRENT_DATE())
          AND l.locale IN ('en-gb','es-es','fr-fr')
          AND l.classification = 'instant-price'
          AND l.PICKUP_DATE <= CURRENT_DATE() - 1
          AND l.category_id IN (1)
          AND l.status <> 24
          AND u.role = 2
          AND l.feedback_score <> 2
          AND l.feedback_score IS NOT NULL
        GROUP BY 1, 2, 3
        HAVING assigned_listings > 0
    ),
    stats AS (
        SELECT
            tp.user_id,
            tp.nickname AS driver_nickname,
            l.category_id,
            AVG((lf.PROVIDER_CARE_OF_GOODS + lf.PROVIDER_COMMUNICATION
                 + lf.PROVIDER_PRESENTATION + lf.PROVIDER_PUNCTUALITY) / 4) AS TP_RATING,
            COUNT(DISTINCT l.listing_id) AS jobs_completed
        FROM harmonised.production.listing_feedback lf
        JOIN harmonised.production.listing l ON l.listing_id = lf.listing_id
        LEFT JOIN harmonised.production.user tp ON tp.user_id = l.chosen_provider AND tp.role = 2
        WHERE l.feedback_score <> 2
          AND l.locale IN ('en-gb','es-es','fr-fr')
          AND l.classification = 'instant-price'
          AND l.feedback_score IS NOT NULL
          AND l.pickup_date > DATEADD(MONTH, -3, CURRENT_DATE())
          AND l.pickup_date <= CURRENT_DATE() - 1
          AND l.status <> 24
          AND l.category_id IN (1)
        GROUP BY 1, 2, 3
    ),
    vat_info AS (
        SELECT
            u.nickname,
            CASE WHEN pvi.user_id IS NOT NULL THEN 1 ELSE 0 END AS vat_status
        FROM harmonised.production.user u
        LEFT JOIN harmonised.production.provider_vat_info pvi
            ON pvi.user_id = u.user_id AND pvi.is_deleted = 0
        WHERE u.role = 2
    )
    SELECT
        COALESCE(dl.NICKNAME, stats.driver_nickname)    AS TP_USERNAME,
        COALESCE(dl.assigned_listings, 0)               AS ASSIGNED_LISTINGS,
        COALESCE(dl.all_deallo_count, 0)                AS ALL_DEALLO_COUNT,
        stats.TP_RATING,
        COALESCE(stats.jobs_completed, 0)               AS JOBS_COMPLETED,
        vi.vat_status                                   AS VAT_STATUS
    FROM dl
    FULL OUTER JOIN stats ON dl.PROVIDER_ID = stats.user_id AND dl.category_id = stats.category_id
    LEFT JOIN vat_info vi ON vi.nickname = COALESCE(dl.NICKNAME, stats.driver_nickname)
    WHERE COALESCE(dl.PROVIDER_ID, stats.user_id) IS NOT NULL
    """
    print("  Fetching TP quality (3-month window, overall deallo, Bayesian shrinkage)...")
    df = sf_query(conn, sql)

    for col in ['ASSIGNED_LISTINGS', 'ALL_DEALLO_COUNT', 'JOBS_COMPLETED']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['TP_RATING'] = pd.to_numeric(df['TP_RATING'], errors='coerce')
    df['VAT_STATUS'] = pd.to_numeric(df['VAT_STATUS'], errors='coerce').fillna(0).astype(int)

    # Aggregate across category rows before computing rates
    df['TP_USERNAME'] = df['TP_USERNAME'].str.upper().str.strip()
    df = df.groupby('TP_USERNAME', as_index=False).agg({
        'ASSIGNED_LISTINGS': 'sum',
        'ALL_DEALLO_COUNT': 'sum',
        'JOBS_COMPLETED': 'sum',
        'TP_RATING': 'mean',
        'VAT_STATUS': 'max',
    })

    # Overall deallo rate (all deallocations / assigned listings)
    df['overall_deallo_rate'] = (
        df['ALL_DEALLO_COUNT'] / df['ASSIGNED_LISTINGS'].replace(0, pd.NA)
    ).fillna(0.0)

    # Bayesian shrinkage — deallo (k=10, n=assigned_listings)
    pop_avg_deallo = df['overall_deallo_rate'].mean() if len(df) > 0 else 0.05
    n_d = df['ASSIGNED_LISTINGS'].fillna(0)
    w_d = n_d / (n_d + 10)
    df['overall_deallo_rate'] = (w_d * df['overall_deallo_rate'] + (1 - w_d) * pop_avg_deallo).round(4)

    # Bayesian shrinkage — rating (k=10, n=jobs_completed)
    pop_avg_rating = df['TP_RATING'].dropna().mean() if df['TP_RATING'].notna().any() else 6.0
    n_r = df['JOBS_COMPLETED'].fillna(0)
    w_r = n_r / (n_r + 10)
    df['TP_RATING'] = (w_r * df['TP_RATING'].fillna(pop_avg_rating) + (1 - w_r) * pop_avg_rating).round(4)

    df.rename(columns={'TP_RATING': 'rating', 'overall_deallo_rate': 'Deallo Rate Overall'}, inplace=True)
    df['Deallo Rate'] = df['Deallo Rate Overall']  # backward-compat alias
    df['rating'] = df['rating'].fillna(6.0)
    df['Deallo Rate Overall'] = df['Deallo Rate Overall'].fillna(0.05)
    df['Deallo Rate'] = df['Deallo Rate'].fillna(0.05)

    print(f"  TP quality: {len(df)} TPs loaded")
    return df[['TP_USERNAME', 'rating', 'Deallo Rate', 'Deallo Rate Overall', 'VAT_STATUS']]

# ── Reservations ──────────────────────────────────────────────────────────────
def fetch_reservations(dates: list, conn, tp_quality: pd.DataFrame) -> pd.DataFrame:
    cluster_map = load_cluster_map()
    date_strs   = ', '.join(f"'{d}'" for d in dates)

    sql = f"""
    SELECT
        TO_VARCHAR(res.DATE, 'YYYY-MM-DD')          AS DATE,
        res.ID,
        res.USERNAME,
        LOWER(res.STATUS)                            AS IRES_STATUS,
        LOWER(res.TYPE)                              AS RES_TYPE,
        IFF(res.ROUTE_DISTANCE < 50000, 'local', 'nationwide') AS TYPE,
        res.PEOPLE                                   AS NUMBER_OF_MEN,
        sp.POSTAL_CODE                               AS START_POSTCODE,
        res.MAX_CAPACITY_M3                          AS RESERVATION_CAPACITY,
        ROUND(res.ROUTE_DURATION / 3600, 1)          AS HOURS_AVAILABLE,
        sp.LAT  AS START_LAT,
        sp.LNG  AS START_LNG
    FROM harmonised.production.tpp_reservations res
    INNER JOIN harmonised.production.reservation_nodes rsn ON rsn.id = res.start_node_id
    INNER JOIN harmonised.production.PLACE sp ON sp.place_id = rsn.place_id
    WHERE res.DATE IN ({date_strs})
      AND res.DELETED_ROW = FALSE
    """
    print("  Fetching reservations...")
    df = sf_query(conn, sql)
    print(f"  Reservations: {len(df)} rows")

    df['START_LAT'] = pd.to_numeric(df['START_LAT'], errors='coerce')
    df['START_LNG'] = pd.to_numeric(df['START_LNG'], errors='coerce')
    df['start_key'] = df['START_LAT'].apply(fmt) + ':' + df['START_LNG'].apply(fmt)
    df['_city'] = df['start_key'].map(cluster_map)
    df['sourcezone'] = df['_city'].map(ZONE_MAP).fillna(df['_city'])

    df['NUMBER_OF_MEN'] = pd.to_numeric(df['NUMBER_OF_MEN'], errors='coerce').fillna(1).astype(int)
    df['RESERVATION_CAPACITY'] = pd.to_numeric(df['RESERVATION_CAPACITY'], errors='coerce')
    df['HOURS_AVAILABLE'] = pd.to_numeric(df['HOURS_AVAILABLE'], errors='coerce').fillna(9.0)

    # consider_res_type
    df['consider_res_type'] = df['RES_TYPE']
    mask = df['consider_res_type'] == 'custom'
    df.loc[mask, 'consider_res_type'] = np.where(
        df.loc[mask, 'TYPE'] == 'local', 'local', 'national'
    )

    # Merge TP quality
    df['USERNAME'] = df['USERNAME'].str.upper().str.strip()
    if not tp_quality.empty:
        df = df.merge(
            tp_quality[['TP_USERNAME', 'rating', 'Deallo Rate', 'Deallo Rate Overall', 'VAT_STATUS']],
            left_on='USERNAME', right_on='TP_USERNAME', how='left'
        ).drop(columns='TP_USERNAME', errors='ignore')
        df['rating']              = df['rating'].fillna(6.0)
        df['Deallo Rate']         = df['Deallo Rate'].fillna(0.05)
        df['Deallo Rate Overall'] = df['Deallo Rate Overall'].fillna(0.05)
        df['VAT_STATUS']          = df['VAT_STATUS'].fillna(0).astype(int)
    else:
        df['rating'] = 6.0
        df['Deallo Rate'] = 0.05
        df['Deallo Rate Overall'] = 0.05
        df['VAT_STATUS'] = 0

    df.drop(columns=['start_key', '_city', 'START_LAT', 'START_LNG', 'TYPE'], inplace=True, errors='ignore')
    return df

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    from datetime import datetime

    args = sys.argv[1:]

    # Detect --forecast flag
    forecast_mode = '--forecast' in args
    if forecast_mode:
        args = [a for a in args if a != '--forecast']

    dates = args if args else [(date.today() + timedelta(days=1)).strftime('%Y-%m-%d')]
    print(f"Mode    : {'FORECAST (reservations only)' if forecast_mode else 'CONFIRMED JOBS'}")
    print(f"Dates   : {dates}")

    stamp    = datetime.now().strftime('%Y-%m-%d_%H%M')
    res_file = os.path.join(SCRIPT_DIR, f'recommended_reservations_{stamp}.csv')

    print("\nConnecting to Snowflake (browser auth)...")
    conn = get_conn()

    print("\nFetching TP quality...")
    tp_quality = fetch_tp_quality(conn)

    if not forecast_mode:
        demand_file = os.path.join(SCRIPT_DIR, f'final_recommendations_{stamp}.csv')
        print("\nFetching demand...")
        demand = fetch_demand(dates, conn)
        demand.to_csv(demand_file, index=False)
        print(f"  → {demand_file}")

    print("\nFetching reservations...")
    res = fetch_reservations(dates, conn, tp_quality)
    res.to_csv(res_file, index=False)
    print(f"  → {res_file}")

    conn.close()

    if forecast_mode:
        print(f"\nRunning supply acceptor (forecast mode)...")
        result = subprocess.run(
            [sys.executable,
             os.path.join(SCRIPT_DIR, 'supply_acceptor_forecast.py'),
             FORECAST_DIR,
             res_file],
            capture_output=False
        )
    else:
        print(f"\nRunning supply acceptor (confirmed jobs mode)...")
        result = subprocess.run(
            [sys.executable,
             os.path.join(SCRIPT_DIR, 'supply_acceptor.py'),
             demand_file,
             res_file],
            capture_output=False
        )

    sys.exit(result.returncode)

if __name__ == '__main__':
    main()
