# %% [0] Imports
import csv
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv

# %% [1] Logging Setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("growth_report")
log.info("=" * 70)
log.info("Growth Monthly Report — run started")

# %% [2] Config
load_dotenv()

# Data filter — display window. The output reproduces the template for every
# month in [REPORT_START, REPORT_END) and appends the newest month as the last
# column. Set to the full history to reproduce + verify against the template;
# narrow it to regenerate a shorter span. MoM/YoY are computed within the series,
# so the first month(s) of the window carry blank growth.
REPORT_START = "2024-01-01"
REPORT_END   = "2026-08-01"   # exclusive → last displayed month is Jun-2026

# Derived bounds used in SQL
_start = datetime.strptime(REPORT_START, "%Y-%m-%d")
_end   = datetime.strptime(REPORT_END,   "%Y-%m-%d")
# Q1 txns CTE needs one month before start (for M-1 retention lookups)
_q1_raw_start = f"{_start.year - (_start.month == 1)}-{12 if _start.month == 1 else _start.month - 1:02d}-01"
# Q1 txns CTE needs one month after end (upper bound)
_q1_raw_end   = f"{_end.year + (_end.month == 12)}-{1 if _end.month == 12 else _end.month + 1:02d}-01"

# Platform mapping: raw marketplace_name → display label
PLATFORM_MAP: dict[str, str] = {
    "media-world":                   "third_party_merchant",
    "amwal":                         "third_party_merchant",
    "pure-platfrom":                 "third_party_merchant",
    "standalone-digital-zone-app":   "standalone-digital-zone-app",
    "taif":                          "third_party_merchant",
    "toters":                        "third_party_merchant",
    "qi-services":                   "qi-services",
    "kushuk":                        "third_party_merchant",
    "pos-app":                       "pos-app",
    "super-qi":                      "super-qi",
    "dot":                           "third_party_merchant",
}

# Category mapping: raw transformed_category → display label (concerts excluded)
CATEGORY_MAP: dict[str, str] = {
    "donation":               "donation",
    "music-streaming":        "music-streaming",
    "e-commerce":             "e-commerce",
    "gsm":                    "gsm",
    "local-services":         "local-services",
    "learning and bootcamps": "learning and bootcamps",
    "security-software":      "security-software",
    "isp-subscriptions":      "isp-subscriptions",
    "unidentified":           "unidentified",
    "local-entertainment":    "local-entertainment",
    "mobile-cards":           "mobile-cards",
    "gaming":                 "gaming",
    "video-streaming":        "video-streaming",
    "social-media":           "social-media",
}

PLATFORMS  = list(dict.fromkeys(PLATFORM_MAP.values()))  # ordered, deduped
CATEGORIES = list(CATEGORY_MAP.values())

# Platform display labels (internal label → Title-Case template label)
PLATFORM_DISPLAY: dict[str, str] = {
    "third_party_merchant":        "Third Party Merchant",
    "standalone-digital-zone-app": "Standalone Digital Zone App",
    "qi-services":                 "Qi Services",
    "pos-app":                     "POS App",
    "super-qi":                    "Super Qi",
}

log.info(f"Report window: {REPORT_START} → {REPORT_END}")
log.info(f"Platforms ({len(PLATFORMS)}): {PLATFORMS}")
log.info(f"Categories ({len(CATEGORIES)}): {CATEGORIES}")

# %% [3] ClickHouse Connection
log.info("--- [3] Connecting to ClickHouse ---")
ch_client = clickhouse_connect.get_client(
    host=os.getenv("CH_HOST"),
    port=int(os.getenv("CH_PORT", "8443")),
    username=os.getenv("CH_USERNAME"),
    password=os.getenv("CH_PASSWORD"),
    database=os.getenv("CH_DATABASE", "dz_data_warehouse"),
    secure=True,
)
log.info("Connected to ClickHouse successfully")

# %% [4] SQL Definitions

Q1_SQL = f"""
WITH

txns AS (
    SELECT
        customer_id,
        toStartOfMonth(created_at)                                    AS report_month,
        total_price + ifNull(fees, 0) + ifNull(donation_amount, 0)   AS total_price
    FROM dz_data_warehouse.digital_zone_customer_transactions_local
    WHERE status = 'SUCCESS'
      AND created_at >= '{_q1_raw_start}'
      AND created_at <  '{_q1_raw_end}'
),

first_purchase AS (
    SELECT
        customer_id,
        MIN(toStartOfMonth(created_at)) AS first_purchase_month
    FROM dz_data_warehouse.digital_zone_customer_transactions_local
    WHERE status = 'SUCCESS'
    GROUP BY customer_id
),

signups AS (
    SELECT
        customer_id,
        MIN(toStartOfMonth(system_created_at)) AS signup_month
    FROM dz_data_warehouse.digital_zone_users_local
    GROUP BY customer_id
),

buyers_per_month AS (
    SELECT DISTINCT
        customer_id,
        report_month
    FROM txns
),

prev_month_check AS (
    SELECT
        b.customer_id                  AS customer_id,
        b.report_month                 AS report_month,
        notEmpty(prev.customer_id)      AS bought_prev_month
    FROM buyers_per_month AS b
    LEFT JOIN buyers_per_month AS prev
        ON  prev.customer_id  = b.customer_id
        AND prev.report_month = addMonths(b.report_month, -1)
),

classified AS (
    SELECT
        t.customer_id             AS customer_id,
        t.report_month            AS report_month,
        t.total_price             AS total_price,
        fp.first_purchase_month   AS first_purchase_month,
        multiIf(
            fp.first_purchase_month = t.report_month AND s.signup_month = t.report_month,
                'new_same_month',
            fp.first_purchase_month = t.report_month AND s.signup_month = addMonths(t.report_month, -1),
                'new_prev_month',
            fp.first_purchase_month = t.report_month AND s.signup_month < addMonths(t.report_month, -1),
                'harvested_new',
            fp.first_purchase_month < t.report_month AND pc.bought_prev_month = 1,
                'existing_retained',
            fp.first_purchase_month < t.report_month AND pc.bought_prev_month = 0,
                'existing_reactivated',
            'unknown'
        ) AS segment
    FROM txns AS t
    LEFT JOIN first_purchase   AS fp ON fp.customer_id = t.customer_id
    LEFT JOIN signups          AS s  ON s.customer_id  = t.customer_id
    LEFT JOIN prev_month_check AS pc
        ON  pc.customer_id  = t.customer_id
        AND pc.report_month = t.report_month
),

agg AS (
    SELECT
        report_month,
        sum(total_price)                                                                         AS gross_sales,
        sumIf(total_price, segment = 'new_same_month')                                          AS rev_new_same_month,
        sumIf(total_price, segment IN ('new_prev_month', 'harvested_new'))                      AS rev_new_prev_month,
        sumIf(total_price, segment IN ('new_same_month', 'new_prev_month', 'harvested_new'))    AS rev_new_all,
        sumIf(total_price, segment = 'existing_retained')                                       AS rev_existing_retained,
        sumIf(total_price, segment = 'existing_reactivated')                                    AS rev_existing_reactivated,
        sumIf(total_price, segment IN ('existing_retained', 'existing_reactivated'))            AS rev_existing_all,
        count()                                                                                  AS txn_total,
        countIf(segment = 'new_same_month')                                                     AS txn_new_same_month,
        countIf(segment IN ('new_prev_month', 'harvested_new'))                                AS txn_new_prev_month,
        countIf(segment = 'harvested_new')                                                      AS txn_harvested_new,
        countIf(segment IN ('new_same_month', 'new_prev_month', 'harvested_new'))               AS txn_new_all,
        countIf(segment = 'existing_retained')                                                  AS txn_existing_retained,
        countIf(segment = 'existing_reactivated')                                               AS txn_existing_reactivated,
        countIf(segment IN ('existing_retained', 'existing_reactivated'))                       AS txn_existing_all,
        uniqIf(customer_id, segment = 'new_same_month')                                         AS cust_new_same_month,
        uniqIf(customer_id, segment IN ('new_prev_month', 'harvested_new'))                     AS cust_new_prev_month,
        uniqIf(customer_id, segment = 'harvested_new')                                          AS cust_harvested_new,
        uniqIf(customer_id, segment IN ('new_same_month', 'new_prev_month', 'harvested_new'))   AS cust_new_all,
        uniqIf(customer_id, segment = 'existing_retained')                                      AS cust_existing_retained,
        uniqIf(customer_id, segment = 'existing_reactivated')                                   AS cust_existing_reactivated,
        uniqIf(customer_id, segment IN ('existing_retained', 'existing_reactivated'))           AS cust_existing_all,
        uniq(customer_id)                                                                        AS cust_total
    FROM classified
    WHERE report_month >= '{REPORT_START}'
      AND report_month <  '{REPORT_END}'
    GROUP BY report_month
),

prev_buyers_count AS (
    SELECT
        addMonths(report_month, 1) AS report_month,
        uniq(customer_id)          AS prev_month_total_buyers
    FROM buyers_per_month
    GROUP BY report_month
),

prev_nonbuyers AS (
    SELECT
        addMonths(s.signup_month, 1) AS report_month,
        countIf(
            fp.first_purchase_month IS NULL
            OR fp.first_purchase_month > s.signup_month
        ) AS prev_month_non_buyers_from_signups
    FROM signups AS s
    LEFT JOIN first_purchase AS fp ON fp.customer_id = s.customer_id
    GROUP BY s.signup_month
)

SELECT
    agg.report_month,
    agg.gross_sales,
    agg.rev_new_same_month,
    agg.rev_new_prev_month,
    agg.rev_new_all,
    agg.rev_existing_retained,
    agg.rev_existing_reactivated,
    agg.rev_existing_all,
    agg.txn_total,
    agg.txn_new_same_month,
    agg.txn_new_prev_month,
    agg.txn_harvested_new,
    agg.txn_new_all,
    agg.txn_existing_retained,
    agg.txn_existing_reactivated,
    agg.txn_existing_all,
    agg.cust_new_same_month,
    agg.cust_new_prev_month,
    agg.cust_harvested_new,
    agg.cust_new_all,
    agg.cust_existing_retained,
    agg.cust_existing_reactivated,
    agg.cust_existing_all,
    agg.cust_total,
    pb.prev_month_total_buyers,
    pn.prev_month_non_buyers_from_signups
FROM agg
LEFT JOIN prev_buyers_count AS pb ON pb.report_month = agg.report_month
LEFT JOIN prev_nonbuyers    AS pn ON pn.report_month = agg.report_month
ORDER BY agg.report_month
SETTINGS allow_experimental_analyzer = 1
"""

Q2_SQL = f"""
SELECT
    toStartOfMonth(system_created_at) AS signup_month,
    count()                           AS total_new_signups
FROM dz_data_warehouse.digital_zone_users_local
WHERE system_created_at >= '{REPORT_START}'
  AND system_created_at <  '{REPORT_END}'
GROUP BY signup_month
ORDER BY signup_month
"""

Q3_SQL = f"""
WITH txns AS (
    SELECT
        customer_id,
        toStartOfMonth(created_at)                                    AS report_month,
        total_price + ifNull(fees, 0) + ifNull(donation_amount, 0)   AS total_price,
        marketplace_name
    FROM dz_data_warehouse.digital_zone_customer_transactions_local
    WHERE status = 'SUCCESS'
      AND created_at >= '{REPORT_START}'
      AND created_at <  '{REPORT_END}'
)
SELECT
    report_month,
    marketplace_name  AS platform_raw,
    sum(total_price)  AS rev_platform,
    count()           AS txn_platform,
    uniq(customer_id) AS cust_platform
FROM txns
GROUP BY report_month, marketplace_name
ORDER BY report_month, marketplace_name
SETTINGS allow_experimental_analyzer = 1
"""

Q4_SQL = f"""
WITH txns AS (
    SELECT
        t.customer_id,
        toStartOfMonth(t.created_at)                                                  AS report_month,
        t.total_price + ifNull(t.fees, 0) + ifNull(t.donation_amount, 0)             AS total_price,
        p.transformed_category
    FROM dz_data_warehouse.digital_zone_customer_transactions_local AS t
    LEFT JOIN dz_data_warehouse.digital_zone_products_local AS p
        ON p.variant_id = t.variant_id
    WHERE t.status = 'SUCCESS'
      AND t.created_at >= '{REPORT_START}'
      AND t.created_at <  '{REPORT_END}'
)
SELECT
    report_month,
    transformed_category  AS category_raw,
    sum(total_price)      AS rev_category,
    count()               AS txn_category,
    uniq(customer_id)     AS cust_category
FROM txns
GROUP BY report_month, transformed_category
ORDER BY report_month, transformed_category
SETTINGS allow_experimental_analyzer = 1
"""

Q5_SQL = """
WITH
signups_dedup AS (
    SELECT customer_id, toStartOfMonth(MIN(system_created_at)) AS month
    FROM dz_data_warehouse.digital_zone_users_local
    GROUP BY customer_id
),
first_buyers_dedup AS (
    SELECT customer_id, toStartOfMonth(MIN(created_at)) AS month
    FROM dz_data_warehouse.digital_zone_customer_transactions_local
    WHERE status = 'SUCCESS'
    GROUP BY customer_id
)
SELECT 'signup'      AS type, month, count() AS n FROM signups_dedup      GROUP BY month
UNION ALL
SELECT 'first_buyer' AS type, month, count() AS n FROM first_buyers_dedup GROUP BY month
ORDER BY type, month
SETTINGS allow_experimental_analyzer = 1
"""

# %% [5] Q1 — Execute Core Metrics
log.info("--- [5] Running Q1: Core Metrics ---")
df_core = ch_client.query_df(Q1_SQL)
log.info(f"Q1 actual columns: {df_core.columns.tolist()}")
df_core.columns = [c.split('.')[-1] for c in df_core.columns]
log.info(
    f"Q1 result: {len(df_core)} rows | "
    f"months {df_core['report_month'].min()} → {df_core['report_month'].max()}"
)
log.debug(f"Q1 sample:\n{df_core[['report_month', 'gross_sales']].head(3)}")

# %% [6] Q2 — Execute Signups
log.info("--- [6] Running Q2: Signups ---")
df_signups = ch_client.query_df(Q2_SQL)
log.info(
    f"Q2 result: {len(df_signups)} rows | "
    f"months {df_signups['signup_month'].min()} → {df_signups['signup_month'].max()}"
)
log.debug(f"Q2 sample:\n{df_signups.head(3)}")

# %% [7] Q3 — Execute Platform Breakdown
log.info("--- [7] Running Q3: Platform Breakdown ---")
df_platform = ch_client.query_df(Q3_SQL)
log.info(f"Q3 result: {len(df_platform)} rows")

_raw_platforms = sorted(df_platform["platform_raw"].dropna().unique().tolist())
log.info(f"Unique raw platform values ({len(_raw_platforms)}): {_raw_platforms}")
_unmapped = [p for p in _raw_platforms if p not in PLATFORM_MAP]
if _unmapped:
    log.warning(f"UNMAPPED platforms (will be 'Other'): {_unmapped}")
df_platform["platform"] = df_platform["platform_raw"].map(PLATFORM_MAP).fillna("Other")

# %% [8] Q4 — Execute Category Breakdown
log.info("--- [8] Running Q4: Category Breakdown ---")
df_category = ch_client.query_df(Q4_SQL)
log.info(f"Q4 result: {len(df_category)} rows")

_raw_categories = sorted(df_category["category_raw"].dropna().unique().tolist())
log.info(f"Unique raw category values ({len(_raw_categories)}): {_raw_categories}")
_unmapped = [c for c in _raw_categories if c not in CATEGORY_MAP]
if _unmapped:
    log.warning(f"UNMAPPED categories (will be 'Other'): {_unmapped}")
_null_rows = df_category["category_raw"].isna().sum()
if _null_rows:
    log.warning(f"{_null_rows} rows with NULL category — excluded from category pivot")
df_category["category"] = df_category["category_raw"].map(CATEGORY_MAP).fillna("Other")

# %% [8b] Q5 — Execute Cumulative Signups & First Buyers
log.info("--- [8b] Running Q5: All-time cumulative signups and first buyers ---")
df_q5 = ch_client.query_df(Q5_SQL)
df_q5.columns = [c.split('.')[-1] for c in df_q5.columns]
log.info(f"Q5 result: {len(df_q5)} rows")

_signups_hist  = df_q5[df_q5['type'] == 'signup'].set_index('month')['n'].sort_index()
_firstbuy_hist = df_q5[df_q5['type'] == 'first_buyer'].set_index('month')['n'].sort_index()

_all_months = pd.date_range(
    start=min(_signups_hist.index.min(), _firstbuy_hist.index.min()),
    end=max(_signups_hist.index.max(), _firstbuy_hist.index.max()),
    freq='MS',
)
_signups_hist  = _signups_hist.reindex(_all_months, fill_value=0)
_firstbuy_hist = _firstbuy_hist.reindex(_all_months, fill_value=0)

_cum_signups   = _signups_hist.cumsum()
_cum_first_buy = _firstbuy_hist.cumsum()

# Key: 'YYYY-MM' string for fast lookup
_cum_signups_dict   = {str(k)[:7]: float(v) for k, v in _cum_signups.items()}
_cum_first_buy_dict = {str(k)[:7]: float(v) for k, v in _cum_first_buy.items()}

log.info(
    f"Cumulative series: signups up to {max(_cum_signups_dict)} = "
    f"{_cum_signups_dict.get(max(_cum_signups_dict), 0):,.0f}, "
    f"first buyers = {_cum_first_buy_dict.get(max(_cum_first_buy_dict), 0):,.0f}"
)

# %% [9] Derived Metrics
log.info("--- [9] Computing derived metrics ---")

df_core = df_core.merge(
    df_signups.rename(columns={"signup_month": "report_month"}),
    on="report_month",
    how="left",
)
df_core["total_new_signups"] = df_core["total_new_signups"].fillna(0).astype(int)
df_core = df_core.sort_values("report_month").reset_index(drop=True)

def _mom(series: pd.Series) -> pd.Series:
    prior = series.shift(1)
    out = (series - prior) / prior.abs() * 100
    return out.replace([float("inf"), float("-inf")], float("nan")).round(2)

def _yoy(series: pd.Series) -> pd.Series:
    prior = series.shift(12)
    out = (series - prior) / prior.abs() * 100
    return out.replace([float("inf"), float("-inf")], float("nan")).round(2)

def _pct_share(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return (numerator / denominator.replace(0, float("nan")) * 100).round(2)

# Revenue MoM & % share
df_core["gross_sales_mom"]               = _mom(df_core["gross_sales"])
df_core["rev_new_all_mom"]               = _mom(df_core["rev_new_all"])
df_core["rev_new_same_month_mom"]        = _mom(df_core["rev_new_same_month"])
df_core["rev_new_prev_month_mom"]        = _mom(df_core["rev_new_prev_month"])
df_core["rev_existing_all_mom"]          = _mom(df_core["rev_existing_all"])
df_core["rev_existing_retained_mom"]     = _mom(df_core["rev_existing_retained"])
df_core["rev_existing_reactivated_mom"]  = _mom(df_core["rev_existing_reactivated"])
df_core["rev_new_all_pct"]               = _pct_share(df_core["rev_new_all"],              df_core["gross_sales"])
df_core["rev_new_same_month_pct"]        = _pct_share(df_core["rev_new_same_month"],        df_core["gross_sales"])
df_core["rev_new_prev_month_pct"]        = _pct_share(df_core["rev_new_prev_month"],        df_core["gross_sales"])
df_core["rev_existing_all_pct"]          = _pct_share(df_core["rev_existing_all"],          df_core["gross_sales"])
df_core["rev_existing_retained_pct"]     = _pct_share(df_core["rev_existing_retained"],     df_core["gross_sales"])
df_core["rev_existing_reactivated_pct"]  = _pct_share(df_core["rev_existing_reactivated"],  df_core["gross_sales"])

# Revenue YoY (Global Calculations section only)
df_core["gross_sales_yoy"]               = _yoy(df_core["gross_sales"])
df_core["rev_new_all_yoy"]               = _yoy(df_core["rev_new_all"])
df_core["rev_existing_all_yoy"]          = _yoy(df_core["rev_existing_all"])
df_core["rev_new_same_month_yoy"]        = _yoy(df_core["rev_new_same_month"])
df_core["rev_new_prev_month_yoy"]        = _yoy(df_core["rev_new_prev_month"])
df_core["rev_existing_retained_yoy"]     = _yoy(df_core["rev_existing_retained"])
df_core["rev_existing_reactivated_yoy"]  = _yoy(df_core["rev_existing_reactivated"])

log.debug(f"Revenue MoM (last 3):\n{df_core[['report_month','gross_sales','gross_sales_mom']].tail(3)}")

# Customer MoM, % share & rates
df_core["total_new_signups_mom"]         = _mom(df_core["total_new_signups"])
df_core["cust_new_same_month_mom"]       = _mom(df_core["cust_new_same_month"])
df_core["cust_new_prev_month_mom"]       = _mom(df_core["cust_new_prev_month"])
df_core["cust_existing_all_mom"]         = _mom(df_core["cust_existing_all"])
df_core["cust_existing_retained_mom"]    = _mom(df_core["cust_existing_retained"])
df_core["cust_existing_reactivated_mom"] = _mom(df_core["cust_existing_reactivated"])
df_core["cust_new_same_month_pct"]       = _pct_share(df_core["cust_new_same_month"],       df_core["cust_total"])
df_core["cust_new_prev_month_pct"]       = _pct_share(df_core["cust_new_prev_month"],        df_core["cust_total"])
df_core["cust_existing_all_pct"]         = _pct_share(df_core["cust_existing_all"],          df_core["cust_total"])
df_core["cust_existing_retained_pct"]    = _pct_share(df_core["cust_existing_retained"],     df_core["cust_total"])
df_core["cust_existing_reactivated_pct"] = _pct_share(df_core["cust_existing_reactivated"],  df_core["cust_total"])
df_core["activation_rate"]               = _pct_share(df_core["cust_new_same_month"],        df_core["total_new_signups"])
df_core["cumulative_non_buyers"] = df_core["report_month"].apply(
    lambda m: _cum_signups_dict.get(str(pd.Timestamp(m) - pd.DateOffset(months=1))[:7], float("nan"))
            - _cum_first_buy_dict.get(str(pd.Timestamp(m) - pd.DateOffset(months=1))[:7], float("nan"))
)
df_core["harvesting_activation_rate"]    = _pct_share(
    df_core["cust_new_prev_month"],
    df_core["cumulative_non_buyers"],
)
df_core["new_user_share"]                = _pct_share(df_core["cust_new_all"],               df_core["cust_total"])
df_core["retention_rate"]                = _pct_share(df_core["cust_existing_retained"],     df_core["prev_month_total_buyers"])
df_core["cum_buyers_prev_month"] = df_core["report_month"].apply(
    lambda m: _cum_first_buy_dict.get(str(pd.Timestamp(m) - pd.DateOffset(months=1))[:7], float("nan"))
)
df_core["total_inactive_base"]           = (
    df_core["cum_buyers_prev_month"] - df_core["prev_month_total_buyers"]
)
df_core["reactivation_rate"]             = _pct_share(
    df_core["cust_existing_reactivated"],
    df_core["total_inactive_base"],
)

# Total Active Customers = new (all) + existing (all) buyers. This excludes the
# `unknown` (no-signup) segment, so it is smaller than cust_total — the latter
# remains the denominator for the Users %share rows.
df_core["cust_active"] = df_core["cust_new_all"] + df_core["cust_existing_all"]

log.debug(f"Rates (last 3):\n{df_core[['report_month','activation_rate','retention_rate','reactivation_rate']].tail(3)}")

# Transaction MoM & % share
df_core["txn_total_mom"]                  = _mom(df_core["txn_total"])
df_core["txn_new_all_mom"]                = _mom(df_core["txn_new_all"])
df_core["txn_new_same_month_mom"]         = _mom(df_core["txn_new_same_month"])
df_core["txn_new_prev_month_mom"]         = _mom(df_core["txn_new_prev_month"])
df_core["txn_harvested_new_mom"]          = _mom(df_core["txn_harvested_new"])
df_core["txn_existing_all_mom"]           = _mom(df_core["txn_existing_all"])
df_core["txn_existing_retained_mom"]      = _mom(df_core["txn_existing_retained"])
df_core["txn_existing_reactivated_mom"]   = _mom(df_core["txn_existing_reactivated"])
df_core["txn_new_all_pct"]                = _pct_share(df_core["txn_new_all"],               df_core["txn_total"])
df_core["txn_new_same_month_pct"]         = _pct_share(df_core["txn_new_same_month"],         df_core["txn_total"])
df_core["txn_new_prev_month_pct"]         = _pct_share(df_core["txn_new_prev_month"],         df_core["txn_total"])
df_core["txn_harvested_new_pct"]          = _pct_share(df_core["txn_harvested_new"],          df_core["txn_total"])
df_core["txn_existing_all_pct"]           = _pct_share(df_core["txn_existing_all"],           df_core["txn_total"])
df_core["txn_existing_retained_pct"]      = _pct_share(df_core["txn_existing_retained"],      df_core["txn_total"])
df_core["txn_existing_reactivated_pct"]   = _pct_share(df_core["txn_existing_reactivated"],   df_core["txn_total"])

# TPC & RPU
df_core["tpc_new_all"]               = (df_core["txn_new_all"]              / df_core["cust_new_all"].replace(0, float("nan"))).round(1)
df_core["tpc_new_same_month"]        = (df_core["txn_new_same_month"]       / df_core["cust_new_same_month"].replace(0, float("nan"))).round(1)
df_core["tpc_new_prev_month"]        = (df_core["txn_new_prev_month"]       / df_core["cust_new_prev_month"].replace(0, float("nan"))).round(1)
df_core["tpc_existing_all"]          = (df_core["txn_existing_all"]         / df_core["cust_existing_all"].replace(0, float("nan"))).round(1)
df_core["tpc_existing_retained"]     = (df_core["txn_existing_retained"]    / df_core["cust_existing_retained"].replace(0, float("nan"))).round(1)
df_core["tpc_existing_reactivated"]  = (df_core["txn_existing_reactivated"] / df_core["cust_existing_reactivated"].replace(0, float("nan"))).round(1)
df_core["rpu_new_all"]               = (df_core["rev_new_all"]              / df_core["cust_new_all"].replace(0, float("nan"))).round(0)
df_core["rpu_new_same_month"]        = (df_core["rev_new_same_month"]       / df_core["cust_new_same_month"].replace(0, float("nan"))).round(0)
df_core["rpu_new_prev_month"]        = (df_core["rev_new_prev_month"]       / df_core["cust_new_prev_month"].replace(0, float("nan"))).round(0)
df_core["rpu_existing_all"]          = (df_core["rev_existing_all"]         / df_core["cust_existing_all"].replace(0, float("nan"))).round(0)
df_core["rpu_existing_retained"]     = (df_core["rev_existing_retained"]    / df_core["cust_existing_retained"].replace(0, float("nan"))).round(0)
df_core["rpu_existing_reactivated"]  = (df_core["rev_existing_reactivated"] / df_core["cust_existing_reactivated"].replace(0, float("nan"))).round(0)

log.info(f"Derived metrics computed for {len(df_core)} months")

# %% [10] Build Template-Shaped Output
log.info("--- [10] Building template-shaped output ---")

df_core["month_label"] = pd.to_datetime(df_core["report_month"]).dt.strftime("%b-%y")
month_cols = df_core["month_label"].tolist()
log.info(f"Month columns ({len(month_cols)}): {month_cols}")

# --- Value formatters (match the template's per-section conventions) ---
def _finite(v) -> bool:
    return pd.notna(v) and v not in (float("inf"), float("-inf"))

def _f_int(v) -> str:                       # counts / IQD values: comma-grouped integer
    return f"{v:,.0f}" if _finite(v) else ""

def _f_pct(v) -> str:                        # Revenue growth/share: 2dp with % sign
    return f"{v:.2f}%" if _finite(v) else ""

def _f_num(v) -> str:                         # other growth/share/rates: 2dp, comma-grouped
    return f"{v:,.2f}" if _finite(v) else ""

def _f_num1(v) -> str:                          # rates shown to 1dp (e.g. Activation Rate)
    return f"{v:,.1f}" if _finite(v) else ""

def _f_tpc(v) -> str:                          # transactions per customer: 1dp
    return f"{v:.1f}" if _finite(v) else ""

_FMT = {"int": _f_int, "pct": _f_pct, "num": _f_num, "num1": _f_num1, "tpc": _f_tpc}

out_rows: list[list[str]] = []
_blank = ["" for _ in month_cols]

def emit_header(label: str) -> None:
    """Section / sub-section header: label only, empty value cells."""
    out_rows.append([label, "", *_blank])

def emit_series(label: str, series: dict, fmt: str) -> None:
    f = _FMT[fmt]
    out_rows.append([label, "", *[f(series.get(m, float("nan"))) for m in month_cols]])

def emit_core(label: str, col: str, fmt: str) -> None:
    emit_series(label, dict(zip(df_core["month_label"], df_core[col])), fmt)

# --- Global Calculations (Revenue) — value + MoM + YoY (+ %share) ---
emit_header("Global Calculations")
emit_core("Gross Sales (IQD)",                    "gross_sales",              "int")
emit_core("- MoM Growth",                         "gross_sales_mom",          "pct")
emit_core("- YoY Growth %",                       "gross_sales_yoy",          "pct")
for label, base in [
    ("Revenue from New Customers",                  "rev_new_all"),
    ("Revenue from Existing Customers",             "rev_existing_all"),
    ("Revenue from New Customers Same Month",       "rev_new_same_month"),
    ("Revenue from New Customers Prev Month",       "rev_new_prev_month"),
    ("Revenue from Existing Retained Customers",    "rev_existing_retained"),
    ("Revenue from Existing Reactivated Customers", "rev_existing_reactivated"),
]:
    emit_core(label,             base,           "int")
    emit_core("- MoM Growth",    f"{base}_mom",  "pct")
    emit_core("- YoY Growth %",  f"{base}_yoy",  "pct")
    emit_core("- %share",        f"{base}_pct",  "pct")

# --- Users ---
emit_header("Users")
emit_core("Total Active Customers",              "cust_active",                "int")
emit_core("Total New Signups",                   "total_new_signups",          "int")
emit_core("- Growth",                            "total_new_signups_mom",      "num")
emit_core("Total New Customers",                 "cust_new_all",               "int")
emit_core("Total New Customers Same Month",      "cust_new_same_month",        "int")
emit_core("- Growth",                            "cust_new_same_month_mom",    "num")
emit_core("- %share",                            "cust_new_same_month_pct",    "num")
emit_core("Total New Customers Prev Month",      "cust_new_prev_month",        "int")
emit_core("- Growth",                            "cust_new_prev_month_mom",    "num")
emit_core("- %share",                            "cust_new_prev_month_pct",    "num")
emit_core("Activation Rate",                     "activation_rate",            "num1")
emit_core("Harvesting Activation Rate",          "harvesting_activation_rate", "num")
emit_core("New User Share",                      "new_user_share",             "num")
emit_core("Total Existing Customers",            "cust_existing_all",          "int")
emit_core("- Growth",                            "cust_existing_all_mom",      "num")
emit_core("- %share",                            "cust_existing_all_pct",      "num")
emit_core("Total Existing Retained Customers",   "cust_existing_retained",     "int")
emit_core("- Growth",                            "cust_existing_retained_mom", "num")
emit_core("- %share",                            "cust_existing_retained_pct", "num")
emit_core("Total Existing Reactivated Customers","cust_existing_reactivated",  "int")
emit_core("- Growth",                            "cust_existing_reactivated_mom", "num")
emit_core("- %share",                            "cust_existing_reactivated_pct", "num")
emit_core("Retention Rate",                      "retention_rate",             "num")
emit_core("Total Inactive Base",                 "total_inactive_base",        "int")
emit_core("Reactivation Rate",                   "reactivation_rate",          "num")

# --- Transactions ---
emit_header("Transactions")
emit_core("Total Transactions",                  "txn_total",                  "int")
emit_core("- Growth",                            "txn_total_mom",              "num")
for label, base in [
    ("Total Transactions New Customers",              "txn_new_all"),
    ("Total Transactions New Customers Same Month",   "txn_new_same_month"),
    ("Total Transactions New Customers Prev Month",   "txn_new_prev_month"),
    ("Total Transactions Harvested New Customers",    "txn_harvested_new"),
    ("Total Transactions Existing Customers",         "txn_existing_all"),
    ("Total Transactions Existing Retained Customers","txn_existing_retained"),
    ("Total Transactions Existing Reactivated Customers", "txn_existing_reactivated"),
]:
    emit_core(label,          base,          "int")
    emit_core("- Growth",     f"{base}_mom", "num")
    emit_core("- %share",     f"{base}_pct", "num")

# --- Unit Metrics ---
emit_header("Unit Metrics")
emit_header("TPC - Transaction Per Customer")
emit_core("New Customers - Blended",            "tpc_new_all",                "tpc")
emit_core("New Customers Same Month",           "tpc_new_same_month",         "tpc")
emit_core("New Customers Prev Month",           "tpc_new_prev_month",         "tpc")
emit_core("Existing Customers - Blended",       "tpc_existing_all",           "tpc")
emit_core("Existing Retained Customers",        "tpc_existing_retained",      "tpc")
emit_core("Existing Reactivated Customers",     "tpc_existing_reactivated",   "tpc")
emit_header("RPU - Revenue Per Customer")
emit_core("New Customers - Blended",            "rpu_new_all",                "int")
emit_core("New Customers Same Month",           "rpu_new_same_month",         "int")
emit_core("New Customers Prev Month",           "rpu_new_prev_month",         "int")
emit_core("Existing Customers - Blended",       "rpu_existing_all",           "int")
emit_core("Existing Retained Customers",        "rpu_existing_retained",      "int")
emit_core("Existing Reactivated Customers",     "rpu_existing_reactivated",   "int")

# --- Breakdowns (Platform & Category) ---
df_platform["month_label"] = pd.to_datetime(df_platform["report_month"]).dt.strftime("%b-%y")
df_category["month_label"] = pd.to_datetime(df_category["report_month"]).dt.strftime("%b-%y")

_gs       = dict(zip(df_core["month_label"], df_core["gross_sales"]))
_tot_txn  = dict(zip(df_core["month_label"], df_core["txn_total"]))
_tot_cust = dict(zip(df_core["month_label"], df_core["cust_total"]))

def emit_breakdown(section, df, key_col, items, display_map, val_col, denom):
    emit_header(section)
    for item in items:
        _df   = df[df[key_col] == item]
        pivot = dict(zip(_df["month_label"], _df[val_col]))
        vals  = {m: pivot.get(m, float("nan")) for m in month_cols}
        label = display_map.get(item, item) if display_map else item
        emit_series(label, vals, "int")
        _series = pd.Series([vals[m] for m in month_cols], index=month_cols)
        emit_series("- Growth", dict(zip(month_cols, _mom(_series).tolist())), "num")
        share = {}
        for m in month_cols:
            d = denom.get(m, float("nan"))
            share[m] = (vals[m] / d * 100) if (_finite(d) and d != 0 and _finite(vals[m])) else float("nan")
        emit_series("- %share", share, "num")
    log.debug(f"Built {section}")

emit_header("Breakdowns")
emit_breakdown("Platform Breakdown - Revenue",      df_platform, "platform", PLATFORMS, PLATFORM_DISPLAY, "rev_platform",  _gs)
emit_breakdown("Platform Breakdown - Transactions", df_platform, "platform", PLATFORMS, PLATFORM_DISPLAY, "txn_platform",  _tot_txn)
emit_breakdown("Platform Breakdown - Users",        df_platform, "platform", PLATFORMS, PLATFORM_DISPLAY, "cust_platform", _tot_cust)
emit_breakdown("Category Breakdown - Revenue",      df_category, "category", CATEGORIES, None, "rev_category",  _gs)
emit_breakdown("Category Breakdown - Transactions", df_category, "category", CATEGORIES, None, "txn_category",  _tot_txn)
emit_breakdown("Category Breakdown - Users",        df_category, "category", CATEGORIES, None, "cust_category", _tot_cust)

log.info(f"Template rows built: {len(out_rows)} rows × {len(month_cols) + 2} cols")

# %% [11] Export CSV (2-column gutter + 2-row header: months / Actuals)
log.info("--- [11] Exporting combined CSV ---")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

_last_ts = pd.to_datetime(df_core["report_month"]).max()
out_path = OUTPUT_DIR / f"growth_accounting_{_last_ts:%Y-%m}.csv"

header_months  = ["", "", *month_cols]
header_actuals = ["", "", *["Actuals" for _ in month_cols]]

with open(out_path, "w", newline="", encoding="utf-8") as fh:
    writer = csv.writer(fh)                 # QUOTE_MINIMAL → wraps comma-bearing values
    writer.writerow(header_months)
    writer.writerow(header_actuals)
    writer.writerows(out_rows)

_size_kb = out_path.stat().st_size / 1024
log.info(f"Exported → {out_path.resolve()} ({_size_kb:.1f} KB)")

log.info("=" * 70)
log.info(f"Run complete. Output: {out_path.resolve()}")

# %%
