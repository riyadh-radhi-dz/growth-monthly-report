# %% [0] Imports
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

REPORT_START = "2024-01-01"
REPORT_END   = "2026-04-01"

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
        sumIf(total_price, first_purchase_month >= addMonths(report_month, -1))                 AS rev_new_all,
        sumIf(total_price, segment = 'existing_retained')                                       AS rev_existing_retained,
        sumIf(total_price, segment = 'existing_reactivated')                                    AS rev_existing_reactivated,
        sumIf(total_price, segment IN ('existing_retained', 'existing_reactivated'))            AS rev_existing_all,
        count()                                                                                  AS txn_total,
        countIf(segment = 'new_same_month')                                                     AS txn_new_same_month,
        countIf(segment = 'new_prev_month')                                                     AS txn_new_prev_month,
        countIf(segment = 'harvested_new')                                                      AS txn_harvested_new,
        countIf(segment IN ('new_same_month', 'new_prev_month', 'harvested_new'))               AS txn_new_all,
        countIf(segment = 'existing_retained')                                                  AS txn_existing_retained,
        countIf(segment = 'existing_reactivated')                                               AS txn_existing_reactivated,
        countIf(segment IN ('existing_retained', 'existing_reactivated'))                       AS txn_existing_all,
        uniqIf(customer_id, segment = 'new_same_month')                                         AS cust_new_same_month,
        uniqIf(customer_id, segment = 'new_prev_month')                                         AS cust_new_prev_month,
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
    return ((series - prior) / prior.abs() * 100).round(2)

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
    df_core["cust_new_prev_month"] + df_core["cust_harvested_new"],
    df_core["cumulative_non_buyers"],
)
df_core["new_user_share"]                = _pct_share(df_core["cust_new_all"],               df_core["cust_total"])
df_core["retention_rate"]                = _pct_share(df_core["cust_existing_retained"],     df_core["prev_month_total_buyers"])
df_core["cum_buyers_prev_month"] = df_core["report_month"].apply(
    lambda m: _cum_first_buy_dict.get(str(pd.Timestamp(m) - pd.DateOffset(months=1))[:7], float("nan"))
)
df_core["reactivation_rate"]             = _pct_share(
    df_core["cust_existing_reactivated"],
    df_core["cum_buyers_prev_month"] - df_core["prev_month_total_buyers"],
)

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

# %% [10] Build Template DataFrame
log.info("--- [10] Building template-shaped output ---")

df_core["month_label"] = pd.to_datetime(df_core["report_month"]).dt.strftime("%b-%y")
month_cols = df_core["month_label"].tolist()
log.info(f"Month columns ({len(month_cols)}): {month_cols}")

def _row(label: str, col: str, df: pd.DataFrame = df_core) -> dict:
    return {"Metric": label, **dict(zip(df["month_label"], df[col]))}

rows: list[dict] = []

# Revenue
rows.append({"Metric": "Global Calculations"})
rows.append(_row("Gross Sales (IQD)",                           "gross_sales"))
rows.append(_row("  Growth",                                    "gross_sales_mom"))
rows.append(_row("Revenue from New Customers",                  "rev_new_all"))
rows.append(_row("  Growth",                                    "rev_new_all_mom"))
rows.append(_row("  %share",                                    "rev_new_all_pct"))
rows.append(_row("Revenue from Existing Customers",             "rev_existing_all"))
rows.append(_row("  Growth",                                    "rev_existing_all_mom"))
rows.append(_row("  %share",                                    "rev_existing_all_pct"))
rows.append(_row("Revenue from New Customers Same Month",       "rev_new_same_month"))
rows.append(_row("  Growth",                                    "rev_new_same_month_mom"))
rows.append(_row("  %share",                                    "rev_new_same_month_pct"))
rows.append(_row("Revenue from New Customers Prev Month",       "rev_new_prev_month"))
rows.append(_row("  Growth",                                    "rev_new_prev_month_mom"))
rows.append(_row("  %share",                                    "rev_new_prev_month_pct"))
rows.append(_row("Revenue from Existing Retained Customers",    "rev_existing_retained"))
rows.append(_row("  Growth",                                    "rev_existing_retained_mom"))
rows.append(_row("  %share",                                    "rev_existing_retained_pct"))
rows.append(_row("Revenue from Existing Reactivated Customers", "rev_existing_reactivated"))
rows.append(_row("  Growth",                                    "rev_existing_reactivated_mom"))
rows.append(_row("  %share",                                    "rev_existing_reactivated_pct"))

# Users
rows.append({"Metric": "Users"})
rows.append(_row("Total New Signups",                           "total_new_signups"))
rows.append(_row("  Growth",                                    "total_new_signups_mom"))
rows.append(_row("Total New Customers Same Month",              "cust_new_same_month"))
rows.append(_row("  Growth",                                    "cust_new_same_month_mom"))
rows.append(_row("  %share",                                    "cust_new_same_month_pct"))
rows.append(_row("Total New Customers Prev Month",              "cust_new_prev_month"))
rows.append(_row("  Growth",                                    "cust_new_prev_month_mom"))
rows.append(_row("  %share",                                    "cust_new_prev_month_pct"))
rows.append(_row("Activation Rate",                             "activation_rate"))
rows.append(_row("Harvesting Activation Rate",                  "harvesting_activation_rate"))
rows.append(_row("New User Share",                              "new_user_share"))
rows.append(_row("Total Existing Customers",                    "cust_existing_all"))
rows.append(_row("  Growth",                                    "cust_existing_all_mom"))
rows.append(_row("  %share",                                    "cust_existing_all_pct"))
rows.append(_row("Total Existing Retained Customers",           "cust_existing_retained"))
rows.append(_row("  Growth",                                    "cust_existing_retained_mom"))
rows.append(_row("  %share",                                    "cust_existing_retained_pct"))
rows.append(_row("Total Existing Reactivated Customers",        "cust_existing_reactivated"))
rows.append(_row("  Growth",                                    "cust_existing_reactivated_mom"))
rows.append(_row("  %share",                                    "cust_existing_reactivated_pct"))
rows.append(_row("Retention Rate",                              "retention_rate"))
rows.append(_row("Reactivation Rate",                           "reactivation_rate"))

# Transactions
rows.append({"Metric": "Transactions"})
rows.append(_row("Total Transactions",                          "txn_total"))
rows.append(_row("  Growth",                                    "txn_total_mom"))
rows.append(_row("Total Transactions New Customers",            "txn_new_all"))
rows.append(_row("  Growth",                                    "txn_new_all_mom"))
rows.append(_row("  %share",                                    "txn_new_all_pct"))
rows.append(_row("Total Transactions New Customers Same Month", "txn_new_same_month"))
rows.append(_row("  Growth",                                    "txn_new_same_month_mom"))
rows.append(_row("  %share",                                    "txn_new_same_month_pct"))
rows.append(_row("Total Transactions New Customers Prev Month", "txn_new_prev_month"))
rows.append(_row("  Growth",                                    "txn_new_prev_month_mom"))
rows.append(_row("  %share",                                    "txn_new_prev_month_pct"))
rows.append(_row("Total Transactions Harvested New Customers",  "txn_harvested_new"))
rows.append(_row("  Growth",                                    "txn_harvested_new_mom"))
rows.append(_row("  %share",                                    "txn_harvested_new_pct"))
rows.append(_row("Total Transactions Existing Customers",       "txn_existing_all"))
rows.append(_row("  Growth",                                    "txn_existing_all_mom"))
rows.append(_row("  %share",                                    "txn_existing_all_pct"))
rows.append(_row("Total Transactions Existing Retained",        "txn_existing_retained"))
rows.append(_row("  Growth",                                    "txn_existing_retained_mom"))
rows.append(_row("  %share",                                    "txn_existing_retained_pct"))
rows.append(_row("Total Transactions Existing Reactivated",     "txn_existing_reactivated"))
rows.append(_row("  Growth",                                    "txn_existing_reactivated_mom"))
rows.append(_row("  %share",                                    "txn_existing_reactivated_pct"))

# Unit Metrics
rows.append({"Metric": "Unit Metrics"})
rows.append(_row("TPC New Customers Blended",          "tpc_new_all"))
rows.append(_row("TPC New Customers Same Month",        "tpc_new_same_month"))
rows.append(_row("TPC New Customers Prev Month",        "tpc_new_prev_month"))
rows.append(_row("TPC Existing Customers Blended",      "tpc_existing_all"))
rows.append(_row("TPC Existing Retained Customers",     "tpc_existing_retained"))
rows.append(_row("TPC Existing Reactivated Customers",  "tpc_existing_reactivated"))
rows.append(_row("RPU New Customers Blended",           "rpu_new_all"))
rows.append(_row("RPU New Customers Same Month",        "rpu_new_same_month"))
rows.append(_row("RPU New Customers Prev Month",        "rpu_new_prev_month"))
rows.append(_row("RPU Existing Customers Blended",      "rpu_existing_all"))
rows.append(_row("RPU Existing Retained Customers",     "rpu_existing_retained"))
rows.append(_row("RPU Existing Reactivated Customers",  "rpu_existing_reactivated"))

# Platform Breakdown
df_platform["month_label"] = pd.to_datetime(df_platform["report_month"]).dt.strftime("%b-%y")
_gs  = dict(zip(df_core["month_label"], df_core["gross_sales"]))
_tot_txn = dict(zip(df_core["month_label"], df_core["txn_total"]))
_tot_cust = dict(zip(df_core["month_label"], df_core["cust_total"]))

for section, val_col, denom in [
    ("Platform Breakdown - Revenue",      "rev_platform",  _gs),
    ("Platform Breakdown - Transactions", "txn_platform",  _tot_txn),
    ("Platform Breakdown - Users",        "cust_platform", _tot_cust),
]:
    rows.append({"Metric": section})
    for platform in PLATFORMS:
        _df = df_platform[df_platform["platform"] == platform]
        _pivot = dict(zip(_df["month_label"], _df[val_col]))
        rows.append({"Metric": platform, **_pivot})
        _series = pd.Series([_pivot.get(m, float("nan")) for m in month_cols])
        rows.append({"Metric": "  Growth", **dict(zip(month_cols, _mom(_series).tolist()))})
        rows.append({"Metric": "  %share", **{
            m: round(_pivot.get(m, float("nan")) / denom.get(m, float("nan")) * 100, 1)
            for m in month_cols
        }})
    log.debug(f"Built {section}")

# Category Breakdown
df_category["month_label"] = pd.to_datetime(df_category["report_month"]).dt.strftime("%b-%y")

for section, val_col, denom in [
    ("Category Breakdown - Revenue",      "rev_category",  _gs),
    ("Category Breakdown - Transactions", "txn_category",  _tot_txn),
    ("Category Breakdown - Users",        "cust_category", _tot_cust),
]:
    rows.append({"Metric": section})
    for cat in CATEGORIES:
        _df = df_category[df_category["category"] == cat]
        _pivot = dict(zip(_df["month_label"], _df[val_col]))
        rows.append({"Metric": cat, **_pivot})
        _series = pd.Series([_pivot.get(m, float("nan")) for m in month_cols])
        rows.append({"Metric": "  Growth", **dict(zip(month_cols, _mom(_series).tolist()))})
        rows.append({"Metric": "  %share", **{
            m: round(_pivot.get(m, float("nan")) / denom.get(m, float("nan")) * 100, 1)
            for m in month_cols
        }})
    log.debug(f"Built {section}")

df_out = pd.DataFrame(rows)
for m in month_cols:
    if m not in df_out.columns:
        df_out[m] = float("nan")

log.info(f"Template DataFrame built: {len(df_out)} rows × {len(df_out.columns)} cols")

# %% [11] Export CSV
log.info("--- [11] Exporting combined CSV ---")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

out_path = OUTPUT_DIR / "growth_accounting_all_months.csv"
df_out.to_csv(out_path, index=False, encoding="utf-8")
_size_kb = out_path.stat().st_size / 1024
log.info(f"Exported → {out_path.resolve()} ({_size_kb:.1f} KB)")

log.info("=" * 70)
log.info(f"Run complete. Output: {out_path.resolve()}")

# %%
