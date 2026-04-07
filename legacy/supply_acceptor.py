"""
Supply Acceptor Algorithm — Furniture (UK)
==========================================
Decides which PENDING TP reservations to accept for each (sourcezone, pickup_date).

Core logic:
  1. For each zone+date, sum realized_lane_level_jobs from the demand file.
  2. Estimate total journeys needed:
       total_journeys = realized_jobs / jobs_per_journey
       (5.8 for dense zones, 5.0 for light zones)
  3. Target reservations = ceil(total_journeys * coverage_ratio)
       Default coverage = 100% of confirmed demand.
       Rationale: realized_lane_level_jobs is only ~60% of final demand at D-3.
       The remaining 40% that arrives later naturally fills the EI (Express Interest) slots.
       So we accept reservations to cover ALL currently confirmed journeys, and EI
       takes care of the late-arriving demand organically.

       Zone-level overrides apply for areas with structural supply excess
       (e.g. Birmingham: 50% normal, 75% at end/start of month).
  4. Gap = max(0, target - already_accepted_non_return_reservations)
  5. Score and rank all pending non-return TPs for the zone+date.
  6. Accept the top `gap` TPs.

Reservation type rules:
  - local      : can only serve local journeys (<= 100 km)
  - national   : can serve both local AND national journeys
  - custom     : TP has constraints (hours, specific end location)
                 custom_local (col: consider_res_type) → preferred
                 custom_national                        → not preferred (deprioritised)
  - return     : TP returning home after a national journey → SKIP (handled elsewhere)
  - 12-man     : flexible TP, can serve 1-man OR 2-man journeys → most versatile

TP scoring (higher = better to accept):
  Weight  Factor
  45%     Rating         (linear: 4.5→0.60, 5.0→1.00; 6.0 = new TP at 0.90)
  34%     Deallo rate    (overall, col K — lower is better)
   6%     VAT status     (non-VAT preferred — saves ~20% on TP cost)
  10%     Capacity       (higher van capacity is better, especially for 2-man)
   5%     Type fitness   (custom local > standard > custom national)

Usage:
    python3 supply_acceptor.py
    python3 supply_acceptor.py path/to/demand.csv path/to/reservations.csv
"""

import sys
import os
import math
import random
import pandas as pd
import numpy as np
from datetime import datetime

# ── Configuration ─────────────────────────────────────────────────────────────

DENSE_ZONES = {'london', 'manchester', 'oxford', 'birmingham'}
DENSE_JPJ   = 5.5   # jobs per journey — dense zones
LIGHT_JPJ   = 5.3   # jobs per journey — light zones

# Per-zone JPJ overrides (take precedence over DENSE_JPJ / LIGHT_JPJ)
ZONE_JPJ_OVERRIDES = {
    'london': 5.8,   # London uses its own tightness logic based on 5.8 JPJ
}

# Default coverage ratio: 100% of confirmed demand.
# realized_lane_level_jobs is ~60% of final demand at D-3; the remaining 40%
# arrives later and naturally fills EI slots — no artificial reduction needed.
DEFAULT_COVERAGE = 1.00

# Zone-level coverage overrides
# Each entry: zone_name → { 'default': ratio, 'period_overrides': [(day_range, ratio), ...] }
# day_range is a set/list of day-of-month integers.
# Period overrides are checked first; if none match, 'default' is used.
# Zones not listed here fall through to the dynamic supply-ratio logic above.
ZONE_COVERAGE_OVERRIDES = {
    'birmingham': {
        'default': 0.50,                          # normal: accept 50% of demand
        'period_overrides': [
            (set(range(1, 4)),   0.75),           # days 1–3 of month  → 75%
            (set(range(24, 32)), 0.75),           # days 24–31 of month → 75%
        ],
    },
    'manchester': {
        'default': 0.75,                          # structural supply excess: accept 75%
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
            (set(range(1, 4)),   1.00),           # days 1–3  → 100%
            (set(range(24, 32)), 1.00),           # days 24–31 → 100%
        ],
    },
    'salisbury': {
        'default': 0.75,
        'period_overrides': [
            (set(range(1, 4)),   1.00),           # days 1–3  → 100%
            (set(range(24, 32)), 1.00),           # days 24–31 → 100%
        ],
    },
    'sheffield': {
        'default': 0.75,
        'period_overrides': [],
    },
    # Add more zones here as needed, e.g.:
    # 'london': { 'default': 0.55, 'period_overrides': [] },
}

# Zone overflow: when a zone has more accepted TPs than total journeys needed,
# the integer excess can offset gaps in nearby zones (reduces how many new TPs
# we need to accept there).
# Format: source_zone → [target_zone, ...] (applied in order until overflow exhausted)
ZONE_OVERFLOW_TARGETS = {
    'birmingham':        ['oxford', 'peterborough'],
    'brighton':          ['salisbury', 'kent'],
    'edinburgh-glasgow': ['north lake district', 'newcastle'],
    'london':            ['oxford', 'peterborough', 'kent', 'brighton'],
    'peterborough':      ['oxford', 'norwich'],
    'manchester':        ['sheffield', 'north wales', 'lake district'],
    # Add more as needed
}

# Versatility threshold: 12-man TPs are ranked above standard TPs only when
# the zone has enough 1-man jobs to plausibly form a 1-man journey.
# If the 1-man job ratio is below this threshold, sort purely by quality score.
VERSATILITY_1MAN_THRESHOLD = 0.30   # 30% of jobs must be 1-man for versatility to matter

# Minimum 1-man jobs required before accepting a pure 1-man reservation.
# 1-man TPs can ONLY serve 1-man journeys — they are inflexible.
# Below this threshold, prefer 2-man TPs (can serve both 1-man and 2-man journeys).
MIN_1MAN_JOBS_FOR_1MAN_TP = 9

# ── Global candidate hard filters ─────────────────────────────────────────────
MIN_RATING   = 4.4   # TPs below this rating are never recommended
MIN_CAPACITY = 8     # vans below this capacity are excluded from all pools

# ── Diversity sampling ────────────────────────────────────────────────────────
# Candidates are split into 3 equal tiers by score (top/mid/bot third).
# Within each tier, TPs are sampled randomly so the same top names don't
# always get accepted — giving lower-tier TPs a chance to improve.
DIVERSITY_T1_PCT = 0.50   # ~50% of picks from top third
DIVERSITY_T2_PCT = 0.30   # ~30% of picks from mid third
                           # T3 (bottom third) gets the remainder (~20%)

# ── London-specific configuration ─────────────────────────────────────────────
# South London postcodes — underserved area, prioritise TPs based here
SOUTH_LONDON_PREFIXES    = {'SE', 'SW', 'KT', 'CR', 'TW', 'BR', 'SM'}
LONDON_SOUTH_QUOTA_PCT   = 0.16   # ~16% of total target from South London
# 1-man bucket
LONDON_1MAN_FACTOR       = 0.66   # fraction of 1-man jobs expected to form journeys
LONDON_1MAN_JPJ          = 5.5    # jobs per 1-man journey
# Tightness check: if total available reservations < ratio × base_journeys → tight
LONDON_OVERSUPPLY_RATIO  = 1.2
LONDON_OVERSUPPLY_JPJ    = 6.0    # JPJ divisor when oversupplied
LONDON_OVERSUPPLY_BUFFER = 5      # subtract from target when oversupplied

# Minimum total confirmed jobs required before accepting ANY reservation in a zone.
# For thin/low-volume zones where demand is too uncertain to commit supply.
ZONE_MIN_JOBS = {
    'east yorkshire':    10,
    'kent':               8,
    'north wales':        8,
    'north lake district': 8,
    'norwich':            8,
    # Add more zones here as needed
}

# Scoring weights
W_RATING = 0.45
W_DEALLO = 0.34
W_VAT    = 0.06
W_CAP    = 0.10
W_TYPE   = 0.05

# Default file paths (relative to this script's folder)
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
DEMAND_FILE   = os.path.join(BASE_DIR, 'FURN_Supply_Summary - Detailed view (2).csv')
RES_FILE      = os.path.join(BASE_DIR, 'FURN_Supply_Summary - reservations (10).csv')


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_jpj(zone: str) -> float:
    zone = zone.strip().lower()
    if zone in ZONE_JPJ_OVERRIDES:
        return ZONE_JPJ_OVERRIDES[zone]
    return DENSE_JPJ if zone in DENSE_ZONES else LIGHT_JPJ


def dynamic_coverage(zone: str = '', pickup_date=None) -> float:
    """
    Returns the fraction of confirmed journeys to cover via reservations.

    Zone-specific overrides (ZONE_COVERAGE_OVERRIDES) are checked first.
    If none apply, defaults to DEFAULT_COVERAGE (100%).
    """
    zone = zone.strip().lower()

    # ── Zone override ─────────────────────────────────────────────────────────
    if zone in ZONE_COVERAGE_OVERRIDES and pickup_date is not None:
        override = ZONE_COVERAGE_OVERRIDES[zone]
        day = pickup_date.day
        for day_range, ratio in override.get('period_overrides', []):
            if day in day_range:
                return ratio
        return override['default']

    return DEFAULT_COVERAGE


def score_tp(row: pd.Series) -> float:
    """
    Compute a composite acceptance score for a single pending TP reservation.
    Returns a float in [0, 1] — higher means more desirable to accept.
    """

    # ── Rating ────────────────────────────────────────────────────────────────
    # Linear scale extended to 4.4: 4.4→0.52, 4.5→0.60, 5.0→1.00 (slope=0.80/pt)
    # rating=6: new TP bonus at 0.90
    # Below 4.4: hard-filtered before scoring, but keep a fallback
    rating = float(row.get('rating') or 0)
    if rating == 6:
        rating_score = 0.90                           # new TP: good chance
    elif rating >= 4.4:
        rating_score = 0.60 + (rating - 4.5) * 0.80  # linear: 4.4→0.52, 5.0→1.00
    else:
        rating_score = max(0.0, rating / 5.0)

    # ── Deallocation rate (col K = Deallo Rate Overall) ───────────────────────
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

    # ── VAT status (0 = non-VAT, saves ~20%) ──────────────────────────────────
    try:
        vat = int(row.get('VAT_STATUS') or 0)
    except (ValueError, TypeError):
        vat = 0
    vat_score = 1.0 if vat == 0 else 0.0

    # ── Capacity ──────────────────────────────────────────────────────────────
    try:
        cap = float(row.get('RESERVATION_CAPACITY') or 15)
    except (ValueError, TypeError):
        cap = 15.0
    # <8: should be pre-filtered; 8-10: small van (1-man only); >10: diminishing returns
    if cap < 8:
        cap_score = 0.0
    elif cap < 10:
        cap_score = 0.3
    elif cap < 15:
        cap_score = 0.5
    else:
        cap_score = 0.65

    # ── Type fitness ──────────────────────────────────────────────────────────
    res_type     = str(row.get('RES_TYPE') or '').strip().lower()
    consider     = str(row.get('consider_res_type') or '').strip().lower()
    men          = int(row.get('NUMBER_OF_MEN') or 1)

    if men == 12:
        type_score = 1.00        # 12-man is most versatile
    elif res_type == 'custom' and consider == 'local':
        type_score = 0.80        # custom local: preferred
    elif res_type in ('local', 'national'):
        type_score = 0.70        # standard reservations: good
    elif res_type == 'custom' and consider == 'national':
        type_score = 0.20        # custom national: deprioritised
    else:
        type_score = 0.50

    score = (rating_score * W_RATING +
             deallo_score * W_DEALLO +
             vat_score    * W_VAT    +
             cap_score    * W_CAP    +
             type_score   * W_TYPE)

    return round(score, 4)


def versatility_rank(row: pd.Series) -> int:
    """
    Assign a versatility tier (higher = more useful to accept when gap exists).
      4 = 12-man national  (covers everything)
      3 = 12-man local/custom-local
      2 = 1/2-man national (covers both local + national)
      1 = 1/2-man local / custom-local
    """
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


# ── Diversity sampling ────────────────────────────────────────────────────────

def diversified_sample(candidates: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    Pick n candidates using a per-pick probabilistic tier draw.

    Candidates are ranked by score into 3 equal tiers (top/mid/bot third).
    For each pick, roll r = random() and choose the source tier:
      r < 0.50          → T1 (top third)
      0.50 <= r < 0.80  → T2 (mid third)
      r >= 0.80         → T3 (bottom third)

    If the chosen tier is exhausted, fall back to the next available tier.
    This gives each pick an independent chance of diversity without batch
    rounding issues (works correctly even for gap=1).
    """
    if candidates.empty or n <= 0:
        return candidates.iloc[0:0]
    n = min(n, len(candidates))
    if n == len(candidates):
        return candidates   # take all — no sampling needed

    unique_count = candidates['USERNAME'].nunique()

    if n <= unique_count:
        # Enough unique TPs to fill — enforce diversity by deduplicating first
        pool = candidates.sort_values('_score', ascending=False).drop_duplicates(subset='USERNAME', keep='first')
    else:
        # Not enough unique TPs — allow multiple reservations from the same TP (extra vehicles)
        pool = candidates

    n = min(n, len(pool))
    sorted_cands = pool.sort_values('_score', ascending=False)
    total = len(sorted_cands)
    t1_end = math.ceil(total / 3)
    t2_end = t1_end + math.ceil((total - t1_end) / 2)

    # Mutable pools of original DataFrame indices per tier
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
    """
    Standard-zone selection with two-tier versatility, diversity, and
    username deduplication.

    Versatility (applied when 1-man ratio >= VERSATILITY_1MAN_THRESHOLD):
      Group 1 — 12-man TPs : preferred first (can serve both 1-man and 2-man)
      Group 2 — all others : filled after Group 1 is exhausted

    Within each group, diversified_sample provides probabilistic tier picks
    (50% T1, 30% T2, 20% T3 by score) so all TPs get a chance.

    Deduplication:
      Primary  — username NOT already accepted for this zone+date → filled first
      Fallback — username already accepted → only used if primary exhausted
    """
    def _fill(pool: pd.DataFrame, n: int) -> pd.DataFrame:
        """Fill n slots from pool, preferring 12-man when 1-man ratio is high."""
        if pool.empty or n <= 0:
            return pool.iloc[0:0]
        if one_man_ratio >= VERSATILITY_1MAN_THRESHOLD:
            twelve_man = pool[pool['NUMBER_OF_MEN'].apply(
                lambda m: int(m) if pd.notna(m) else 1) == 12]
            others     = pool[pool['NUMBER_OF_MEN'].apply(
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


# ── London helpers ────────────────────────────────────────────────────────────

def is_south_london(postcode) -> bool:
    """True if the postcode is in a South London area (SE/SW/KT/CR/TW/BR/SM)."""
    if not postcode or pd.isna(postcode):
        return False
    pc = str(postcode).strip().upper()
    return any(pc.startswith(p) for p in SOUTH_LONDON_PREFIXES)


def london_calc_target(realized_jobs: float, total_available: int, apply_buffer: bool = True):
    """
    Returns (target, is_tight) for London.
      Oversupplied: available >= 1.2 × base_journeys → round(jobs/6) - 5 (D+1 only)
                                                      → round(jobs/6)     (D+2 and beyond)
      Tight:        available <  1.2 × base_journeys → round(jobs/5.8)

    apply_buffer=True  → pickup date is tomorrow (D+1): apply the -5 buffer
    apply_buffer=False → pickup date is D+2 or beyond: no buffer
    """
    base_journeys = round(realized_jobs / ZONE_JPJ_OVERRIDES.get('london', DENSE_JPJ))
    is_tight = total_available < LONDON_OVERSUPPLY_RATIO * base_journeys
    if is_tight:
        target = base_journeys
    else:
        buffer = LONDON_OVERSUPPLY_BUFFER if apply_buffer else 0
        target = max(0, round(realized_jobs / LONDON_OVERSUPPLY_JPJ) - buffer)
    return target, is_tight


def london_select_candidates(candidates: pd.DataFrame,
                              zone_data:  pd.Series,
                              one_man_jobs: float,
                              gap: int,
                              already_accepted: pd.DataFrame = None) -> pd.DataFrame:
    """
    London-specific TP selection logic.

    Pass 1 — South London quota : round(target × 16%) best-scored South London TPs
    Pass 2 — 1-man bucket       : round(1man_jobs × 0.66 / 5.5) from {1-man, 12-man}
                                  caps 8-10 acceptable here
    Pass 3 — 2-man bucket       : remaining slots, 50/50 national/local, cap >= 10

    Sub-quotas are adjusted for already-accepted TPs so the composition of the
    new acceptances correctly completes the target mix.
    Hard filters applied first: rating >= 4.5, cap >= 8.
    Returns at most `gap` selected candidates.
    """
    if candidates.empty or gap <= 0:
        return candidates.iloc[0:0]

    is_tight     = bool(zone_data.get('is_tight', False))
    final_target = int(zone_data['target_reservations'])

    cands = candidates.copy()
    cands['_cap_val']    = cands['RESERVATION_CAPACITY'].apply(lambda c: float(c or 0))
    cands['_rating_val'] = cands['rating'].apply(lambda r: float(r or 0))
    cands['_men_val']    = cands['NUMBER_OF_MEN'].apply(lambda m: int(m) if pd.notna(m) else 1)

    # Hard filters
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

    sl_1man_count     = len(south_sel[south_sel['_men_val'].isin([1, 12])])
    sl_2man_nat_count = len(south_sel[(south_sel['_men_val'].isin([2, 12])) & (south_sel['RES_TYPE'].str.lower() == 'national')])
    sl_2man_loc_count = len(south_sel[(south_sel['_men_val'].isin([2, 12])) & (south_sel['RES_TYPE'].str.lower() == 'local')])

    if len(south_sel) < south_quota_need:
        print(f'  ⚠ London South quota: only {acc_south + len(south_sel)} of {south_quota_total} South London TPs filled')

    # ── Pass 2: 1-man bucket (cap >= 8 ok) ────────────────────────────────────
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

    # ── Pass 3: 2-man bucket — 50/50 national/local, cap >= 10 ───────────────
    # If 1-man bucket was underfilled, compensate with up to 4 extra 2-man TPs
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


# ── Main algorithm ────────────────────────────────────────────────────────────

def run(demand_path: str, res_path: str, output_path: str = None):

    # ── Load ──────────────────────────────────────────────────────────────────
    demand = pd.read_csv(demand_path)
    res    = pd.read_csv(res_path, low_memory=False)

    # Strip Excel-generated trailing unnamed columns
    res = res.loc[:, ~res.columns.str.startswith('Unnamed')]

    # Normalise demand column names
    demand.columns = [c.strip().lower().replace(' ', '_') for c in demand.columns]
    demand['pickup_day']   = pd.to_datetime(demand['pickup_day'])
    demand['sourcezone']   = demand['sourcezone'].str.strip().str.lower()

    # Normalise reservation columns (preserve originals, just strip whitespace)
    res['sourcezone']    = res['sourcezone'].str.strip().str.lower()
    res['DATE']          = pd.to_datetime(res['DATE'], dayfirst=True, format='mixed')
    res['IRES_STATUS']   = res['IRES_STATUS'].str.strip().str.lower()
    res['RES_TYPE']      = res['RES_TYPE'].str.strip().str.lower()

    # ── Step 1: Zone + date demand targets ────────────────────────────────────
    # Sum realized jobs across all (men, local_national) segments per zone+date
    zone_day = (
        demand[demand['number_of_men'].isin([1, 2])]  # exclude placeholder 12-man demand rows
        .groupby(['sourcezone', 'pickup_day'])['realized_lane_level_jobs']
        .sum()
        .reset_index()
        .rename(columns={'realized_lane_level_jobs': 'realized_jobs'})
    )

    # Add rows for any zones that have reservations but no demand entries,
    # so they appear in the full picture table (with 0 jobs).
    pickup_dates = zone_day['pickup_day'].unique()
    res_zones = set(res[res['DATE'].isin(pickup_dates)]['sourcezone'].unique())
    demand_zone_dates = set(zip(zone_day['sourcezone'], zone_day['pickup_day']))
    extra_rows = [
        {'sourcezone': z, 'pickup_day': d, 'realized_jobs': 0}
        for d in pickup_dates
        for z in res_zones
        if (z, d) not in demand_zone_dates
    ]
    if extra_rows:
        zone_day = pd.concat([zone_day, pd.DataFrame(extra_rows)], ignore_index=True)

    # 1-man job ratio per zone+date — drives whether versatility ranking is applied
    one_man = (
        demand[demand['number_of_men'] == 1]
        .groupby(['sourcezone', 'pickup_day'])['realized_lane_level_jobs']
        .sum()
        .reset_index()
        .rename(columns={'realized_lane_level_jobs': 'one_man_jobs'})
    )
    zone_day = zone_day.merge(one_man, on=['sourcezone', 'pickup_day'], how='left')
    zone_day['one_man_jobs']  = zone_day['one_man_jobs'].fillna(0)
    zone_day['one_man_ratio'] = zone_day['one_man_jobs'] / zone_day['realized_jobs'].replace(0, 1)

    zone_day['jpj']            = zone_day['sourcezone'].apply(get_jpj)
    zone_day['total_journeys'] = zone_day['realized_jobs'] / zone_day['jpj']

    # ── Step 2: Count accepted and pending (excluding return reservations) ─────
    non_return = res[res['RES_TYPE'] != 'return']

    # Accepted count includes ALL accepted reservations (returns too).
    # A TP returning home after a national job still fills a slot on that date.
    # We just never *recommend* new return reservations — but existing ones count.
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

    zone_day['accepted_count'] = zone_day['accepted_count'].fillna(0).astype(int)
    zone_day['pending_count']  = zone_day['pending_count'].fillna(0).astype(int)
    zone_day['total_available'] = zone_day['accepted_count'] + zone_day['pending_count']

    # Coverage ratio: 100% by default; zone overrides may reduce it (e.g. Birmingham)
    zone_day['coverage_ratio'] = zone_day.apply(
        lambda r: dynamic_coverage(zone=r['sourcezone'], pickup_date=r['pickup_day']),
        axis=1
    )
    zone_day['target_reservations'] = zone_day.apply(
        lambda r: int(r['total_journeys'] * r['coverage_ratio']), axis=1
    ).astype(int)
    zone_day['gap'] = (zone_day['target_reservations'] - zone_day['accepted_count']).clip(lower=0)

    # ── Tightness override for coverage-override zones ─────────────────────────
    # If a zone normally runs below 100% coverage (e.g. Birmingham=50%) but total
    # available supply (accepted + pending) is less than the 100% target, the zone
    # is tight — switch to 100% coverage so we don't leave demand uncovered.
    zone_day['is_tight'] = False
    zone_day['london_mode'] = ''
    for idx, row in zone_day[zone_day['sourcezone'].isin(ZONE_COVERAGE_OVERRIDES.keys())].iterrows():
        if row['sourcezone'] == 'london':
            continue  # London handled separately below
        if float(row['coverage_ratio']) >= 1.0:
            continue  # already at 100%, nothing to do
        full_target = round(float(row['total_journeys']) * 1.0)
        if int(row['total_available']) < full_target:
            print(f"  [tight] {row['sourcezone']} {row['pickup_day'].date()}: "
                  f"supply tight (avail={int(row['total_available'])} < full_target={full_target}) "
                  f"— switching to 100% coverage (was {int(row['coverage_ratio']*100)}%)")
            zone_day.at[idx, 'coverage_ratio']      = 1.0
            zone_day.at[idx, 'target_reservations'] = full_target
            zone_day.at[idx, 'gap']  = max(0, full_target - int(row['accepted_count']))
            zone_day.at[idx, 'is_tight'] = True

    # ── London target override ─────────────────────────────────────────────────
    # London uses a tightness-aware formula instead of the standard coverage ratio.
    # The -5 buffer is only applied for D+1 (tomorrow); D+2 and beyond use no buffer
    # since confirmed demand is still early-stage (~60% of final) and the buffer
    # would make the target too conservative.
    tomorrow = pd.Timestamp.now().normalize() + pd.Timedelta(days=1)
    for idx, row in zone_day[zone_day['sourcezone'] == 'london'].iterrows():
        total_avail  = int(row['accepted_count']) + int(row['pending_count'])
        apply_buffer = (row['pickup_day'] == tomorrow)
        target, tight = london_calc_target(float(row['realized_jobs']), total_avail, apply_buffer)
        zone_day.at[idx, 'target_reservations'] = target
        zone_day.at[idx, 'is_tight']   = tight
        zone_day.at[idx, 'london_mode'] = 'TIGHT' if tight else 'OVERSUP'
        zone_day.at[idx, 'gap'] = max(0, target - int(row['accepted_count']))
        # Update coverage_ratio for display
        base = round(float(row['realized_jobs']) / DENSE_JPJ)
        zone_day.at[idx, 'coverage_ratio'] = target / base if base > 0 else 1.0

    # ── Overflow credits ───────────────────────────────────────────────────────
    # tp_excess: genuine excess TPs that can be redirected to nearby zones.
    # - Oversupplied zones (ZONE_COVERAGE_OVERRIDES, e.g. Birmingham): 80% threshold.
    #   We intentionally accept fewer TPs there, so anything covering >80% of journeys
    #   is surplus. e.g. accepted=6, journeys=5.3 → floor(6 - 0.8*5.3) = 1 excess.
    # - All other zones: excess = accepted beyond journeys actually needed (round).
    #   e.g. Brighton 6 jobs → 1.2 journeys → need 1 TP → 2 accepted → 1 excess.
    #        Brighton 10 jobs → 2.0 journeys → need 2 TPs → 2 accepted → 0 excess.
    OVERSUPPLIED_EXCESS_THRESHOLD = 0.85

    zone_day = zone_day.reset_index(drop=True)

    def calc_excess(row):
        if row['sourcezone'] in ZONE_COVERAGE_OVERRIDES:
            return max(0, math.floor(row['accepted_count'] - row['total_journeys'] * OVERSUPPLIED_EXCESS_THRESHOLD))
        else:
            return max(0, int(row['accepted_count']) - round(row['total_journeys']))

    zone_day['tp_excess'] = zone_day.apply(calc_excess, axis=1).astype(int)
    zone_day['overflow_credit'] = 0  # how much gap reduction this zone received from overflow

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
                reduction = min(overflow, current_gap)
                zone_day.at[idx, 'gap']             -= reduction
                zone_day.at[idx, 'overflow_credit'] += reduction
                overflow -= reduction

    # ── Step 3: Score all pending (non-return) reservations ───────────────────
    pending_mask = (res['IRES_STATUS'] == 'pending') & (res['RES_TYPE'] != 'return')
    res.loc[pending_mask, '_score']       = res[pending_mask].apply(score_tp, axis=1)
    res.loc[pending_mask, '_versatility'] = res[pending_mask].apply(versatility_rank, axis=1)

    # Initialise output columns
    res['new_recommendation']      = False
    res['new_recommendation_rank'] = pd.NA

    # ── Step 4: For each zone+date, build summary and select best pending TPs ───
    accepted_summary = []

    for _, zd in zone_day.iterrows():
        zone = zd['sourcezone']
        date = zd['pickup_day']
        gap  = int(zd['gap'])

        newly_accepted = 0
        unfilled       = 0

        # Skip zone entirely if confirmed jobs are below the minimum threshold
        min_jobs = ZONE_MIN_JOBS.get(zone, 0)
        if min_jobs > 0 and int(zd['realized_jobs']) < min_jobs:
            print(f"  [skip] {zone} {date.date()}: only {int(zd['realized_jobs'])} confirmed jobs "
                  f"(min {min_jobs} required) — no reservations accepted")
            gap = 0

        if gap > 0:
            # Candidates: pending, non-return, matching zone+date
            cand_mask = (
                (res['sourcezone'] == zone) &
                (res['DATE']       == date) &
                pending_mask
            )
            candidates = res.loc[cand_mask].copy()

            # Global hard filter: exclude TPs below rating floor
            candidates = candidates[
                candidates['rating'].apply(lambda r: float(r or 0)) >= MIN_RATING
            ]

            if not candidates.empty:

                if zone == 'london':
                    # ── London: specialised multi-bucket selection ─────────────
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
                    # ── Standard zones ────────────────────────────────────────
                    # Filter out pure 1-man TPs when zone has too few 1-man jobs.
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
                                  f"(only {int(one_man_jobs_z)} 1-man jobs ≤ {MIN_1MAN_JOBS_FOR_1MAN_TP} threshold)")

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

        # ── London 1-man composition check (always, even when gap=0) ─────────
        london_1man_warning = ''
        if zone == 'london' and int(zd['accepted_count']) > 0:
            one_man_target_ldn = round(float(zd.get('one_man_jobs', 0)) * LONDON_1MAN_FACTOR / LONDON_1MAN_JPJ)
            accepted_london = res[
                (res['sourcezone'] == 'london') &
                (res['DATE'] == date) &
                (res['IRES_STATUS'] == 'accepted')
            ]
            accepted_1man = len(accepted_london[
                accepted_london['NUMBER_OF_MEN'].apply(lambda m: int(m) if pd.notna(m) else 1).isin([1, 12])
            ])
            if accepted_1man < one_man_target_ldn:
                shortfall = one_man_target_ldn - accepted_1man
                london_1man_warning = (
                    f'  ⚠ London {date.date()}: 1-man shortfall — need {one_man_target_ldn}, '
                    f'accepted {accepted_1man} (shortfall={shortfall})'
                )
                print(london_1man_warning)

        # Always include every zone in summary for a full picture
        tp_excess     = int(zd['tp_excess'])
        ovfl_credit   = int(zd['overflow_credit'])
        zone_label = zone
        if zd.get('london_mode'):
            zone_label = f"{zone} [{zd['london_mode']}]"
        elif bool(zd.get('is_tight')):
            zone_label = f"{zone} [TIGHT]"
        accepted_summary.append({
            'Zone':             zone_label,
            'Pickup Date':      date.date(),
            'Realized Jobs':    int(zd['realized_jobs']),
            'One Man Jobs':     float(zd.get('one_man_jobs', 0)),
            'Journeys':         round(float(zd['total_journeys']), 1),
            'Coverage %':       int(zd['coverage_ratio'] * 100),
            'Target Res':       int(zd['target_reservations']),
            'Already Accepted': int(zd['accepted_count']),
            'Pending':          int(zd['pending_count']),
            'TP Excess':        tp_excess,
            'Ovfl Credit':      ovfl_credit,
            'Gap':              gap,
            'Newly Accepted':   newly_accepted,
            'Unfilled Gap':     unfilled,
        })

    # ── Output ────────────────────────────────────────────────────────────────
    # Drop internal working columns before saving
    res = res.drop(columns=['_score', '_versatility'], errors='ignore')

    if output_path is None:
        ts = datetime.now().strftime('%Y-%m-%d_%H%M')
        output_path = os.path.join(BASE_DIR, f'supply_acceptor_output_{ts}.csv')

    res.to_csv(output_path, index=False)

    # ── Print summary ─────────────────────────────────────────────────────────
    print("\n" + "=" * 80)
    print("  SUPPLY ACCEPTOR — RECOMMENDATIONS")
    print(f"  Run time : {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 80)

    if accepted_summary:
        summary_df = pd.DataFrame(accepted_summary)
        summary_df = summary_df.sort_values(['Pickup Date', 'Zone'])

        total_new = res['new_recommendation'].sum()

        for date, grp in summary_df.groupby('Pickup Date'):
            print(f"\n  Pickup: {date}")
            print(f"  {'Zone':<22} {'Jobs':>5} {'Jrnys':>6} {'Cov%':>5} {'Target':>7} {'Acc':>5} {'Pend':>5} {'Excess':>7} {'OvflCrd':>8} {'Gap':>5} {'NewAcc':>7} {'Unfill':>7}")
            print(f"  {'-'*106}")
            for _, r in grp.iterrows():
                unfill_flag    = ' ⚠' if r['Unfilled Gap'] > 0 else ''
                zone_clean     = r['Zone'].lower().replace(' [tight]', '').replace(' [oversup]', '').strip()
                has_ovfl_route = zone_clean in ZONE_OVERFLOW_TARGETS
                excess_flag    = f"+{r['TP Excess']}→" if (r['TP Excess'] > 0 and has_ovfl_route) else (f"+{r['TP Excess']}" if r['TP Excess'] > 0 else '')
                credit_flag    = f"-{r['Ovfl Credit']}" if r['Ovfl Credit'] > 0 else ''
                print(f"  {r['Zone']:<22} {r['Realized Jobs']:>5} {r['Journeys']:>6.1f} {r['Coverage %']:>4}% "
                      f"{r['Target Res']:>7} {r['Already Accepted']:>5} {r['Pending']:>5} "
                      f"{excess_flag:>7} {credit_flag:>8} {r['Gap']:>5} "
                      f"{r['Newly Accepted']:>7} {r['Unfilled Gap']:>7}{unfill_flag}")
            day_total = grp['Realized Jobs'].sum()
            day_new   = grp['Newly Accepted'].sum()
            print(f"  {'TOTAL':<22} {day_total:>5} {'':>6} {'':>5} {'':>7} {'':>5} {'':>5} {'':>7} {'':>8} {'':>5} {day_new:>7}")

            # ── EI Vetting ────────────────────────────────────────────────────
            day_post_acc      = grp['Already Accepted'].sum() + grp['Newly Accepted'].sum()
            expected_journeys = day_total / 5.5
            journeys_to_ei    = expected_journeys - day_post_acc
            dow               = pd.Timestamp(date).day_name()
            ei_lo, ei_hi      = (20, 30) if dow == 'Sunday' else (25, 40)
            if journeys_to_ei < ei_lo:
                ei_status = f'⚠ BELOW range ({ei_lo}–{ei_hi}) — over-reserved'
            elif journeys_to_ei > ei_hi:
                ei_status = f'⚠ ABOVE range ({ei_lo}–{ei_hi}) — under-reserved'
            else:
                ei_status = f'✓ within range ({ei_lo}–{ei_hi})'
            print(f"\n  EI Vetting ({dow}): {day_total} jobs ÷ 5.5 = {expected_journeys:.1f} expected journeys  |  "
                  f"Post-acceptance TPs: {int(day_post_acc)}  |  "
                  f"→ Journeys to EI: {journeys_to_ei:.1f}  {ei_status}")

        print(f"\n{'─'*80}")
        print(f"  Total new recommendations : {int(total_new)}")
        print(f"  Output saved to           : {output_path}")

    else:
        print("\n  No gaps found — no new recommendations needed.")

    # ── Print recommended TPs ─────────────────────────────────────────────────
    recommended = res[res['new_recommendation'] == True].sort_values(
        ['DATE', 'sourcezone', 'new_recommendation_rank']
    )

    if not recommended.empty:
        print(f"\n{'=' * 80}")
        print("  RECOMMENDED TPs TO ACCEPT")
        print(f"{'=' * 80}")
        print(f"  {'Date':<12} {'Zone':<22} {'Username':<14} {'Men':>4} {'Type':<10} "
              f"{'Consider':<10} {'Rating':>7} {'Deallo':>7} {'VAT':>4} {'Cap':>4} {'Rank':>5}")
        print(f"  {'-'*100}")
        for _, r in recommended.iterrows():
            vat_label = 'No' if int(r.get('VAT_STATUS') or 0) == 0 else 'Yes'
            print(f"  {str(r['DATE'].date()):<12} {r['sourcezone']:<22} {str(r['USERNAME']):<14} "
                  f"{str(r['NUMBER_OF_MEN']):>4} {str(r['RES_TYPE']):<10} "
                  f"{str(r.get('consider_res_type') or ''):<10} "
                  f"{float(r.get('rating') or 0):>7.2f} "
                  f"{float(r.get('Deallo Rate Overall') or 0):>7.3f} "
                  f"{vat_label:>4} "
                  f"{str(r.get('RESERVATION_CAPACITY') or ''):>4} "
                  f"{str(int(r['new_recommendation_rank'])):>5}")

    # ── FLAGS ─────────────────────────────────────────────────────────────────
    if accepted_summary:
        print(f"\n{'=' * 80}")
        print("  FLAGS")
        print(f"{'=' * 80}")

        # Overflow routes for Flag 7
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
                # Already accepted (from reservations with IRES_STATUS=accepted for london on this date)
                ldn_accepted = res[
                    (res['DATE'].dt.date == date) &
                    (res['sourcezone'] == 'london') &
                    (res['IRES_STATUS'] == 'accepted')
                ]
                ldn_new_rec = date_rec[date_rec['sourcezone'] == 'london'] if not date_rec.empty else pd.DataFrame()
                def is_1man_capable(m):
                    try: return int(m) in [1, 12]
                    except: return False
                acc_1man  = ldn_accepted['NUMBER_OF_MEN'].apply(is_1man_capable).sum()
                new_1man  = ldn_new_rec['NUMBER_OF_MEN'].apply(is_1man_capable).sum() if not ldn_new_rec.empty else 0
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
                        zone_clean = r['sourcezone']
                        date_flags.append(f"⚠ {r['USERNAME']} ({zone_clean}): high deallo rate ({d:.0%}) — limited alternatives available")

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
                    ldn_target = int(ldn_target_row.iloc[0]['Target Res'])
                    south_quota = round(ldn_target * LONDON_SOUTH_QUOTA_PCT)
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

    print(f"\n{'=' * 80}\n")
    return res, summary_df if accepted_summary else pd.DataFrame()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    demand_path = sys.argv[1] if len(sys.argv) > 1 else DEMAND_FILE
    res_path    = sys.argv[2] if len(sys.argv) > 2 else RES_FILE
    output_path = sys.argv[3] if len(sys.argv) > 3 else None

    run(demand_path, res_path, output_path)
