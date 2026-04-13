

## Database Tables

| Table | Purpose |
|---|---|
| `dz_data_warehouse.digital_zone_customer_transactions_local` | One row per transaction; source for all revenue, transaction count, and customer activity metrics |
| `dz_data_warehouse.digital_zone_users_local` | One row per user registration; source for signup counts and signup dates |
| `dz_data_warehouse.digital_zone_products_local` | Product catalogue; joined via `variant_id` to get `transformed_category` |

---

## Customer Segmentation

All revenue, transaction, and customer metrics are broken down by **customer segment**. The segment is assigned per transaction based on three inputs:

1. **`first_purchase_month`** — the earliest month in which the customer ever completed a successful transaction (`MIN(toStartOfMonth(created_at))`, no date filter, across all time).
2. **`signup_month`** — the earliest month in which the customer registered (`MIN(toStartOfMonth(system_created_at))` from `digital_zone_users_local`).
3. **`bought_prev_month`** — `1` if the customer had at least one successful transaction in the calendar month immediately before `report_month`, `0` otherwise.

### Segment Rules (evaluated in order)

| Segment | Condition |
|---|---|
| `new_same_month` | Customer's first purchase is in `report_month` **AND** they signed up in `report_month` |
| `new_prev_month` | Customer's first purchase is in `report_month` **AND** they signed up in `report_month - 1 month` |
| `harvested_new` | Customer's first purchase is in `report_month` **AND** they signed up **before** `report_month - 1 month` |
| `existing_retained` | Customer's first purchase was **before** `report_month` **AND** they bought in the previous month (`bought_prev_month = 1`) |
| `existing_reactivated` | Customer's first purchase was **before** `report_month` **AND** they did **not** buy in the previous month (`bought_prev_month = 0`) |
| `unknown` | None of the above conditions match (e.g. no signup record found) |

**"Existing Customers" (blended)** = `existing_retained` + `existing_reactivated`

---

## Notation

- `M` = the calendar month being reported
- `SUM(revenue | condition)` = sum of `total_price` for transactions meeting that condition
- `UNIQ(customers | condition)` = distinct count of `customer_id` meeting that condition
- `COUNT(transactions | condition)` = total transaction rows meeting that condition
- `MoM%` = Month-over-Month growth percentage
- `%share` = percentage share relative to the all-segment total for that metric type

---

## 1. Global Calculations — Revenue

### Gross Sales (IQD)

**Definition:** Total transaction value across all successful transactions in the month.

**Formula:**
```
Gross Sales = SUM(total_price)
              WHERE status = 'SUCCESS'
              AND report_month = M
```

**Source:** `digital_zone_customer_transactions_local`
**Filter:** `status = 'SUCCESS'`

---

### Gross Sales — Growth

**Definition:** Month-over-Month percentage change in Gross Sales.

**Formula:**
```
Growth = ((Gross Sales[M] - Gross Sales[M-1]) / |Gross Sales[M-1]|) × 100
```

Computed in Python from the monthly series.

---

### Revenue from New Customers

**Definition:** Total revenue attributable to customers whose **first-ever purchase** occurred in month M **or** month M-1. This captures both customers making their debut purchase this month and customers who first purchased last month and are transacting again.

**Formula:**
```
Revenue from New Customers = SUM(total_price | first_purchase_month >= M-1)
```

In SQL:
```sql
sumIf(total_price, first_purchase_month >= addMonths(report_month, -1))
```

**Who is included:**
- All customers in `new_same_month`, `new_prev_month`, `harvested_new` segments (first purchase = M)
- `existing_retained` customers whose `first_purchase_month = M-1` (they first bought last month and bought again this month)

**Source:** `digital_zone_customer_transactions_local` + `digital_zone_users_local`
**Filter:** `status = 'SUCCESS'`

---

### Revenue from New Customers — Growth & %share

```
Growth  = MoM% of Revenue from New Customers
%share  = Revenue from New Customers / Gross Sales × 100
```

---

### Revenue from Existing Customers

**Definition:** Total revenue from customers who have purchased at least once **before** month M.

**Formula:**
```
Revenue from Existing Customers = SUM(total_price | segment IN (existing_retained, existing_reactivated))
```

**Filter:** `status = 'SUCCESS'`, `first_purchase_month < M`

---

### Revenue from New Customers Same Month

**Definition:** Revenue from customers who both signed up and made their first purchase in month M.

**Formula:**
```
rev_new_same_month = SUM(total_price | segment = new_same_month)
                   = SUM(total_price | first_purchase_month = M AND signup_month = M)
```

---

### Revenue from New Customers Prev Month

**Definition:** Revenue from customers whose first purchase was in month M but who signed up in the previous month (M-1).

**Formula:**
```
rev_new_prev_month = SUM(total_price | segment = new_prev_month)
                   = SUM(total_price | first_purchase_month = M AND signup_month = M-1)
```

---

### Revenue from Existing Retained Customers

**Definition:** Revenue from returning customers who also purchased in the immediately preceding month (consecutive buyers).

**Formula:**
```
rev_existing_retained = SUM(total_price | segment = existing_retained)
                      = SUM(total_price | first_purchase_month < M AND bought_prev_month = 1)
```

---

### Revenue from Existing Reactivated Customers

**Definition:** Revenue from returning customers who did **not** purchase in the immediately preceding month (lapsed buyers who came back).

**Formula:**
```
rev_existing_reactivated = SUM(total_price | segment = existing_reactivated)
                         = SUM(total_price | first_purchase_month < M AND bought_prev_month = 0)
```

---

## 2. Users

### Total New Signups

**Definition:** Number of user accounts created in month M.

**Formula:**
```
Total New Signups = COUNT(DISTINCT customer_id)
                   WHERE toStartOfMonth(system_created_at) = M
```

**Source:** `digital_zone_users_local`
**Note:** Uses `MIN(system_created_at)` per customer to deduplicate duplicate registration rows.

---

### Total New Customers Same Month

**Definition:** Distinct count of customers making their first purchase in month M who also signed up in M.

**Formula:**
```
cust_new_same_month = UNIQ(customer_id | segment = new_same_month)
```

---

### Total New Customers Prev Month

**Definition:** Distinct count of customers making their first purchase in month M who signed up in M-1.

**Formula:**
```
cust_new_prev_month = UNIQ(customer_id | segment = new_prev_month)
```

---

### Activation Rate

**Definition:** Percentage of users who signed up in month M and also made their first purchase in month M.

**Formula:**
```
Activation Rate = (cust_new_same_month[M] / Total New Signups[M]) × 100
```

**Denominator source:** `digital_zone_users_local`, filtered to `toStartOfMonth(system_created_at) = M`

---

### Harvesting Activation Rate

**Definition:** Percentage of the total cumulative non-buyer base (all users who ever registered but have never made a purchase, as of start of M) who were converted into first-time buyers in month M. Captures how effectively the platform activates its entire dormant registered user pool each month.

**Numerator:** All first-time buyers in M who signed up **before** M — i.e., `new_prev_month` + `harvested_new`.

**Denominator:** Cumulative count of users registered up to end of M-1 who have never made a purchase as of start of M.

**Formula:**
```
Harvesting Activation Rate =
    (cust_new_prev_month[M] + cust_harvested_new[M])
    / cumulative_non_buyers[M]  × 100

cumulative_non_buyers[M] =
    cumulative_all_time_signups[M-1] − cumulative_all_time_first_buyers[M-1]
```

Where:
- `cumulative_all_time_signups[M-1]` = total distinct users ever registered (from `digital_zone_users_local`) up to and including end of M-1
- `cumulative_all_time_first_buyers[M-1]` = total distinct customers who have ever completed at least one successful transaction up to and including end of M-1

**Note:** When more buyers exist than registered users (data coverage gap), `cumulative_non_buyers` can be negative, producing a negative rate. This reflects the data quality limit rather than a computation error.

**Source:** `digital_zone_users_local` and `digital_zone_customer_transactions_local` (both queried all-time, no date filter)

---

### New User Share

**Definition:** Proportion of total buyers in month M who are first-time buyers (across all new sub-segments).

**Formula:**
```
New User Share = (cust_new_all / cust_total) × 100
               = UNIQ(customer_id | segment IN (new_same_month, new_prev_month, harvested_new))
                 / UNIQ(customer_id)  × 100
```

---

### Total Existing Customers

**Definition:** Distinct count of returning buyers (any existing sub-segment) in month M.

**Formula:**
```
cust_existing_all = UNIQ(customer_id | segment IN (existing_retained, existing_reactivated))
```

---

### Total Existing Retained Customers

**Definition:** Distinct count of returning buyers who also purchased in M-1.

**Formula:**
```
cust_existing_retained = UNIQ(customer_id | segment = existing_retained)
                       = UNIQ(customer_id | first_purchase_month < M AND bought_prev_month = 1)
```

---

### Total Existing Reactivated Customers

**Definition:** Distinct count of returning buyers who skipped M-1 but returned in M.

**Formula:**
```
cust_existing_reactivated = UNIQ(customer_id | segment = existing_reactivated)
                          = UNIQ(customer_id | first_purchase_month < M AND bought_prev_month = 0)
```

---

### Retention Rate

**Definition:** Percentage of last month's buyers who purchased again this month.

**Formula:**
```
Retention Rate = (cust_existing_retained[M] / prev_month_total_buyers[M]) × 100
```

Where `prev_month_total_buyers[M]` = `UNIQ(customer_id)` from successful transactions in month M-1.

**Source:** `digital_zone_customer_transactions_local`, `status = 'SUCCESS'`

---

### Reactivation Rate

**Definition:** Percentage of the total **inactive buyer base** who returned and purchased in month M. The inactive base is every customer who has ever bought (up to end of M-1) but did **not** buy in M-1 — the full dormant pool going into month M.

**Formula:**
```
Reactivation Rate = cust_existing_reactivated[M] / total_inactive_base[M]  × 100

total_inactive_base[M] = cumulative_all_time_first_buyers[M-1] − prev_month_total_buyers[M-1]
```

Where:
- `cumulative_all_time_first_buyers[M-1]` = total distinct customers who have ever made at least one successful transaction up to and including end of M-1
- `prev_month_total_buyers[M-1]` = distinct buyers in month M-1 (the active portion)

**Source:** `digital_zone_customer_transactions_local`, all-time (no date filter for cumulative base)

---

## 3. Transactions

All transaction metrics follow the same segmentation logic as revenue, counting rows rather than summing value.

### Total Transactions

```
txn_total = COUNT(*) WHERE status = 'SUCCESS' AND report_month = M
```

### Total Transactions New Customers

```
txn_new_all = COUNT(*  | segment IN (new_same_month, new_prev_month, harvested_new))
```

### Total Transactions New Customers Same Month

```
txn_new_same_month = COUNT(* | segment = new_same_month)
```

### Total Transactions New Customers Prev Month

```
txn_new_prev_month = COUNT(* | segment = new_prev_month)
```

### Total Transactions Harvested New Customers

**Definition:** Transaction count from customers whose first purchase is in M but who signed up 2+ months earlier (long-dormant sign-ups finally activated).

```
txn_harvested_new = COUNT(* | segment = harvested_new)
                  = COUNT(* | first_purchase_month = M AND signup_month < M-1)
```

### Total Transactions Existing Customers

```
txn_existing_all = COUNT(* | segment IN (existing_retained, existing_reactivated))
```

### Total Transactions Existing Retained

```
txn_existing_retained = COUNT(* | segment = existing_retained)
```

### Total Transactions Existing Reactivated

```
txn_existing_reactivated = COUNT(* | segment = existing_reactivated)
```

All transaction metrics also have **Growth** (MoM%) and **%share** (share of `txn_total`) sub-rows, computed identically to the revenue equivalents.

---

## 4. Unit Metrics

### TPC — Transactions Per Customer

**Definition:** Average number of transactions per unique buyer within a segment in month M.

**Formula:**
```
TPC[segment] = COUNT(transactions | segment) / UNIQ(customer_id | segment)
```

| Metric | Numerator | Denominator |
|---|---|---|
| TPC New Customers Blended | `txn_new_all` | `cust_new_all` |
| TPC New Customers Same Month | `txn_new_same_month` | `cust_new_same_month` |
| TPC New Customers Prev Month | `txn_new_prev_month` | `cust_new_prev_month` |
| TPC Existing Customers Blended | `txn_existing_all` | `cust_existing_all` |
| TPC Existing Retained Customers | `txn_existing_retained` | `cust_existing_retained` |
| TPC Existing Reactivated Customers | `txn_existing_reactivated` | `cust_existing_reactivated` |

---

### RPU — Revenue Per User

**Definition:** Average revenue generated per unique buyer within a segment in month M.

**Formula:**
```
RPU[segment] = SUM(total_price | segment) / UNIQ(customer_id | segment)
```

| Metric | Numerator | Denominator |
|---|---|---|
| RPU New Customers Blended | `rev_new_all` | `cust_new_all` |
| RPU New Customers Same Month | `rev_new_same_month` | `cust_new_same_month` |
| RPU New Customers Prev Month | `rev_new_prev_month` | `cust_new_prev_month` |
| RPU Existing Customers Blended | `rev_existing_all` | `cust_existing_all` |
| RPU Existing Retained Customers | `rev_existing_retained` | `cust_existing_retained` |
| RPU Existing Reactivated Customers | `rev_existing_reactivated` | `cust_existing_reactivated` |

---

## 5. Platform Breakdown

**Definition:** Revenue, transaction count, and unique buyer count split by the platform (marketplace) through which the transaction was placed.

**Source:** `digital_zone_customer_transactions_local.marketplace_name`
**Filter:** `status = 'SUCCESS'`

**Platform mapping** (raw `marketplace_name` → display label):

| Raw Value | Display Label |
|---|---|
| `media-world` | `third_party_merchant` |
| `amwal` | `third_party_merchant` |
| `pure-platfrom` | `third_party_merchant` |
| `taif` | `third_party_merchant` |
| `toters` | `third_party_merchant` |
| `kushuk` | `third_party_merchant` |
| `dot` | `third_party_merchant` |
| `standalone-digital-zone-app` | `standalone-digital-zone-app` |
| `qi-services` | `qi-services` |
| `pos-app` | `pos-app` |
| `super-qi` | `super-qi` |

Raw values not in the mapping are grouped as **Other**.

**Formulas per platform `P` and month `M`:**

```
Platform Revenue[P, M]      = SUM(total_price     | platform = P, report_month = M)
Platform Transactions[P, M] = COUNT(*              | platform = P, report_month = M)
Platform Users[P, M]        = UNIQ(customer_id     | platform = P, report_month = M)

Platform Revenue Growth[P, M]      = MoM% of Platform Revenue[P]
Platform Revenue %share[P, M]      = Platform Revenue[P, M]      / Gross Sales[M]       × 100
Platform Transactions Growth[P, M] = MoM% of Platform Transactions[P]
Platform Transactions %share[P, M] = Platform Transactions[P, M] / Total Transactions[M] × 100
Platform Users Growth[P, M]        = MoM% of Platform Users[P]
Platform Users %share[P, M]        = Platform Users[P, M]        / Total Buyers[M]       × 100
```

---

## 6. Category Breakdown

**Definition:** Revenue, transaction count, and unique buyer count split by product category.

**Source:** `digital_zone_customer_transactions_local` LEFT JOIN `digital_zone_products_local` ON `variant_id`, using the `transformed_category` column from the products table.
**Filter:** `status = 'SUCCESS'`

**Categories tracked:**

`donation`, `music-streaming`, `e-commerce`, `gsm`, `local-services`, `learning and bootcamps`, `security-software`, `isp-subscriptions`, `unidentified`, `local-entertainment`, `mobile-cards`, `gaming`, `video-streaming`, `social-media`

The `concerts` category is excluded. Raw values not in the list are grouped as **Other**.

**Formulas per category `C` and month `M`:**

```
Category Revenue[C, M]      = SUM(total_price  | category = C, report_month = M)
Category Transactions[C, M] = COUNT(*           | category = C, report_month = M)
Category Users[C, M]        = UNIQ(customer_id  | category = C, report_month = M)

Category Revenue Growth[C, M]      = MoM% of Category Revenue[C]
Category Revenue %share[C, M]      = Category Revenue[C, M]      / Gross Sales[M]        × 100
Category Transactions Growth[C, M] = MoM% of Category Transactions[C]
Category Transactions %share[C, M] = Category Transactions[C, M] / Total Transactions[M] × 100
Category Users Growth[C, M]        = MoM% of Category Users[C]
Category Users %share[C, M]        = Category Users[C, M]        / Total Buyers[M]        × 100
```
