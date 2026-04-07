# Snowflake Schema — Supply Acceptor

## Demand Query (Confirmed Bookings)

```sql
SELECT
    TO_VARCHAR(DATE(l.PICKUP_DATE), 'YYYY-MM-DD') AS pickup_day,
    ROUND(p.LAT, 1)  AS src_lat,
    ROUND(p.LNG, 1)  AS src_lon,
    ls.NUMBER_OF_MEN_REQUIRED AS number_of_men,
    COUNT(DISTINCT l.LISTING_ID) AS job_count
FROM harmonised.production.LISTING l
JOIN harmonised.production.LISTING_SCORE ls ON ls.LISTING_ID = l.LISTING_ID
JOIN harmonised.production.ROUTE r            ON r.ROUTE_ID  = l.ROUTE_ID
JOIN harmonised.production.PLACE p            ON p.PLACE_ID  = r.START_PLACE_ID
JOIN harmonised.staging.CATEGORY c            ON c.CATEGORY_ID = l.CATEGORY_ID
WHERE l.STATUS = 21
  AND l.ROUTABLE = 1
  AND l.LOCALE = 'en-gb'
  AND DATE(l.PICKUP_DATE) = DATE(l.DELIVERY_DATE)
  AND DATE(l.PICKUP_DATE) IN ('2026-03-16')       -- date strings, not integers
  AND c.IDENT IN ('general_goods_move', 'house_move')
  AND ls.NUMBER_OF_MEN_REQUIRED IN (1, 2)
GROUP BY 1, 2, 3, 4
```

## Reservations Query (correct — from repo)

```sql
SELECT
    TO_VARCHAR(res.DATE, 'YYYY-MM-DD')                      AS DATE,
    res.ID,
    res.USERNAME,
    LOWER(res.STATUS)                                        AS IRES_STATUS,
    LOWER(res.TYPE)                                          AS RES_TYPE,
    IFF(res.route_distance < 50000, 'local', 'nationwide')  AS TYPE,  -- ⚠ computed, not res.type
    res.PEOPLE                                               AS NUMBER_OF_MEN,
    sp.POSTAL_CODE                                           AS START_POSTCODE,
    ep.POSTAL_CODE                                           AS END_POSTCODE,
    res.MAX_CAPACITY_M3                                      AS RESERVATION_CAPACITY,
    ROUND(res.ROUTE_DURATION / 3600, 1)                      AS RESERVATION_DURATION_HRS,
    TO_TIME(rsn.TIME)                                        AS START_TIME,
    TO_TIME(ren.TIME)                                        AS END_TIME,
    ROUND(sp.LAT, 1) || ':' || ROUND(sp.LNG, 1)             AS SRC_COORD,
    sp.LAT AS START_LAT,   sp.LNG AS START_LNG,
    ep.LAT AS END_LAT,     ep.LNG AS END_LNG
FROM harmonised.production.tpp_reservations res
INNER JOIN harmonised.production.reservation_nodes rsn ON rsn.ID = res.START_NODE_ID
INNER JOIN harmonised.production.PLACE sp ON sp.PLACE_ID = rsn.PLACE_ID
LEFT JOIN  harmonised.production.reservation_nodes ren ON ren.ID = res.END_NODE_ID
LEFT JOIN  harmonised.production.PLACE ep ON ep.PLACE_ID = ren.PLACE_ID
WHERE res.DATE IN ('2026-03-16')
  AND res.DELETED_ROW = FALSE
```

Then JOIN with TP quality data (rating, deallo, VAT) on USERNAME = TP_USERNAME.
Filter `res.type != 'return'` **after** fetching (or in WHERE clause — the recommender filters it in Python).
```

## Table Reference

### LISTING
| Column | Type | Notes |
|--------|------|-------|
| LISTING_ID | NUMBER | Primary key (NOT `ID`) |
| STATUS | NUMBER | 21 = confirmed/active |
| ROUTABLE | BOOLEAN | Must be 1 |
| LOCALE | VARCHAR | Filter on 'en-gb' |
| PICKUP_DATE | TIMESTAMP_NTZ | Use DATE() wrapper |
| DELIVERY_DATE | TIMESTAMP_NTZ | Same-day = pickup = delivery |
| ROUTE_ID | NUMBER | FK to ROUTE |
| CATEGORY_ID | NUMBER | FK to CATEGORY |

### LISTING_SCORE
| Column | Notes |
|--------|-------|
| LISTING_ID | FK to LISTING |
| NUMBER_OF_MEN_REQUIRED | 1 or 2 for furniture jobs |
| VOLUME | m3 × 1000000 |

### ROUTE
| Column | Notes |
|--------|-------|
| ROUTE_ID | PK |
| START_PLACE_ID | FK to PLACE |
| END_PLACE_ID | FK to PLACE |
| DISTANCE | route distance in metres |

### PLACE
| Column | Notes |
|--------|-------|
| PLACE_ID | PK |
| LAT | float (round to 1dp for zone mapping) |
| LNG | float |
| POSTAL_CODE | e.g. 'SW1A 1AA' |
| UK_POSTCODE_DISTRICT | e.g. 'SW1A' |

### CATEGORY (harmonised.staging)
| Column | Notes |
|--------|-------|
| CATEGORY_ID | PK |
| IDENT | 'general_goods_move', 'house_move', etc. |

### TPP_RESERVATIONS
| Column | Notes |
|--------|-------|
| ID | PK |
| DATE | DATE type — compare with 'YYYY-MM-DD' strings |
| USERNAME | TP username |
| STATUS | 'pending'/'draft'/'accepted'/'tp_withdrawn'/'av_cancelled'/'unsuccessful' |
| TYPE | 'local'/'national'/'custom'/'return' — this IS the res type |
| PEOPLE | number of men (1 or 2 typically; watch for bad data) |
| MAX_CAPACITY_M3 | volume capacity in m3 |
| ROUTE_DURATION | duration in seconds |
| ROUTE_DISTANCE | distance in metres |
| START_NODE_ID | FK to RESERVATION_NODES.ID |
| END_NODE_ID | FK to RESERVATION_NODES.ID |
| DELETED_ROW | filter FALSE |

### RESERVATION_NODES
| Column | Notes |
|--------|-------|
| ID | PK |
| RESERVATION_ID | FK to TPP_RESERVATIONS |
| PLACE_ID | FK to PLACE |
| TIME | datetime of the node |
| ORDER | node order (0 = start, 1 = end typically) |
| ⚠️ NO TYPE | — use tpp_reservations.TYPE instead |

### USER
| Column | Notes |
|--------|-------|
| USER_ID | PK |
| NICKNAME | username (join key) |
| RATING | Not 0–5 stars! Different scale. Use listing_feedback for proper ratings. |
| ROLE | 2 = TP (transport provider) |
| ⚠️ NO VAT_REGISTERED | — not present |

### ⛔ Tables that DON'T exist / are inaccessible
- `USER_RATING` — does not exist or no permission
- `listing_provider` — use LISTING_SCORE for TP assignment data

## TYPE Column (local vs nationwide)

Computed from `res.route_distance` (metres):
```sql
IFF(res.route_distance < 50000, 'local', 'nationwide') AS type
```
- `< 50,000m` (50km) → `'local'`
- `≥ 50,000m` → `'nationwide'`

Note: returns `'nationwide'` not `'national'`. The `consider_res_type` logic only checks `== 'local'`, so this is fine.

## HOURS_AVAILABLE

Computed from `reservation_nodes.TIME` (start and end nodes), clamped to 08:00–20:00 window:

```python
def calc_hours(row) -> float:
    start_t, end_t = row["START_TIME"], row["END_TIME"]
    if pd.isna(start_t) and pd.isna(end_t):
        return 9  # default if no times
    start_clamped = max(datetime.combine(date, start_t), datetime(date.year, date.month, date.day, 8, 0))
    end_clamped   = min(datetime.combine(date, end_t),   datetime(date.year, date.month, date.day, 20, 0))
    return (end_clamped - start_clamped).total_seconds() / 3600.0
```

Fetch start/end times in the reservations query:
```sql
TO_TIME(rsn.time) AS start_time,  -- rsn = start_node (rsn.id = res.start_node_id)
TO_TIME(ren.time) AS end_time,    -- ren = end_node   (ren.id = res.end_node_id)
```

## Full TP Quality Data Query (rating + deallo + VAT)

```sql
WITH dl AS (
    SELECT
        lp.PROVIDER_ID,
        u.NICKNAME,
        l.category_id,
        COUNT(DISTINCT lp.LISTING_ID)              AS assigned_listings,
        COUNT(lp.DE_ALLOCATE_REASON_IDENTIFIER)    AS all_deallocations,
        COUNT(IFF(
            DATE(lp.DE_ALLOCATED_AT) = DATE(l.PICKUP_DATE)
            AND lp.DE_ALLOCATE_REASON_IDENTIFIER ILIKE '%charged%',
            lp.DE_ALLOCATED_AT, NULL))             AS same_day_charged_deallo,
        all_deallocations / assigned_listings      AS deallo_rate,
        same_day_charged_deallo / assigned_listings AS same_day_charged_deallo_rate
    FROM harmonised.production.listing l
    LEFT JOIN harmonised.production.listing_provider lp ON l.listing_id = lp.listing_id
    LEFT JOIN harmonised.production.USER u ON lp.PROVIDER_ID = u.USER_ID
    LEFT JOIN harmonised.staging.category ct ON l.category_id = ct.category_id
    WHERE l.PICKUP_DATE > DATEADD(MONTH, -2, CURRENT_DATE())
      AND l.locale IN ('en-gb', 'es-es', 'fr-fr')
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
        AVG((lf.PROVIDER_CARE_OF_GOODS + lf.PROVIDER_COMMUNICATION
           + lf.PROVIDER_PRESENTATION + lf.PROVIDER_PUNCTUALITY) / 4) AS TP_RATING
    FROM harmonised.production.listing_feedback lf
    JOIN harmonised.production.listing l ON l.listing_id = lf.listing_id
    LEFT JOIN harmonised.production.user tp ON tp.user_id = l.chosen_provider AND tp.role = 2
    WHERE l.feedback_score <> 2
      AND l.locale IN ('en-gb', 'es-es', 'fr-fr')
      AND l.classification = 'instant-price'
      AND l.feedback_score IS NOT NULL
      AND l.pickup_date > DATEADD(MONTH, -2, CURRENT_DATE())
      AND l.pickup_date <= CURRENT_DATE() - 1
      AND l.status <> 24
      AND l.category_id IN (1)
    GROUP BY 1, 2
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
    COALESCE(dl.NICKNAME, stats.driver_nickname) AS TP_USERNAME,
    dl.same_day_charged_deallo_rate              AS DEALLO_RATE,
    dl.deallo_rate                               AS DEALLO_RATE_OVERALL,
    stats.TP_RATING                              AS rating,
    vi.vat_status                                AS VAT_STATUS
FROM dl
FULL OUTER JOIN stats ON dl.PROVIDER_ID = stats.user_id AND dl.category_id = stats.category_id
LEFT JOIN vat_info vi ON vi.nickname = COALESCE(dl.NICKNAME, stats.driver_nickname)
WHERE COALESCE(dl.PROVIDER_ID, stats.user_id) IS NOT NULL
ORDER BY TP_USERNAME
```

### Key tables for TP quality:
- `harmonised.production.listing_provider` — TP assignment + deallocation data (has `PROVIDER_ID`, `LISTING_ID`, `DE_ALLOCATE_REASON_IDENTIFIER`, `DE_ALLOCATED_AT`)
- `harmonised.production.listing_feedback` — star ratings (has `PROVIDER_CARE_OF_GOODS`, `PROVIDER_COMMUNICATION`, `PROVIDER_PRESENTATION`, `PROVIDER_PUNCTUALITY`)
- `harmonised.production.provider_vat_info` — VAT registration (has `user_id`, `is_deleted`)
- All queries use: `category_id IN (1)`, `classification = 'instant-price'`, `last 2 months`, `locale IN ('en-gb','es-es','fr-fr')`
