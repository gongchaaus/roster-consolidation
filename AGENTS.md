# Agent Memory

## Gong Cha Bonus — Sales Source

- `gc_bonus` pulls `sum(net_amount)` from `analytics_gongchaaus.txn_lines` (all sources, no `source = 'redcat'` filter)
- Exclusions read dynamically from `analytics_gongchaaus.ops_bonus_exclusion` (same format as `product_mapping`) — no hardcoded list
- Targets sheet is in net sales units; `Bonus Rate` applies when `Sales >= Target Sales`
- Sales merge uses `Date` as a merge key alongside `Store ID`/`store_id` (per-day match). Earlier code merged only on store, producing `Date_x`/`Date_y` and breaking the subsequent targets merge with `KeyError: 'Date'` (fixed).
- `net_amount` differs from portal "Total Ex GST" by a few cents per store because the portal rounds each order's `(gross − gst)` to 2 dp, while `SUM(net_amount)` sums line-level `net_amount` at full precision. Use `sum(round(sum(gross_amount) - sum(gst_amount), 2))` grouped per `(store_id, transaction_date, order_id)` to match portal exactly.
- `points-redeem` (Points Redemption, negative `net_amount`) is **included** — it reduces the sales figure used for bonus. `sys-points-01` (Points Claimed) is a separate code; neither is in `ops_bonus_exclusion`.
- `sur-card` (Card Surcharge) is **not** in `ops_bonus_exclusion`, so card surcharge is currently included in bonus-eligible sales. The stale `surcharge-01` entry in the exclusion list has no data.

## Hot Star Sales Integration

### Architecture
- `app.py` `calc_timesheets_n_billings()` splits bonus into **Gong Cha** (`Sxxx` Store ID) and **Hot Star** (`HSxxx` Store ID) after StoreReference merge
- `gc_bonus` uses `store_id` → `analytics_gongchaaus.txn_lines` (unified RedCat + ZiiCloud)
- `hs_bonus` uses `productStoreId` → `aupos_hotstaraus.d_txnlines` flow (new)

### Hot Star Data Sources
| Source | What | Where |
|---|---|---|
| Store mapping | `store_id` → `productStoreId` | `aupos_hotstaraus.r_stores` (ClickHouse) |
| POS sales | Daily `SUM(total)` grouped by `(productStoreId, Date)` | `aupos_hotstaraus.d_txnlines` |
| Manual sales | Override/additional entries by `store_id` + `date` | `storeSalesHS` sheet (Google Sheet `1rqOeBjA9drmTnjlENvr57RqL5-oxSqe_KGdbdL2MKhM`) |
| Targets | Tiered `Target Sales` + `Bonus Rate` per `(Store ID, Date)` | `TargetHS` sheet (same Google Sheet) |

### Sales Combination
Both POS and manual sales are **summed** per `(Store ID, Date)`:
```python
hs_bonus['Sales'] = pos_sales.fillna(0) + manual_sales.fillna(0)
```

### Hot Star Store IDs
| Roster Store Name | Store ID | productStoreId |
|---|---|---|
| Hotstar Burwood | HS007 | 4 |
| Hotstar HV | HS034 | 29 |
| Hotstar Waterloo | HS035 | 30 |
| Hotstar The Star | HS039 | 32 |
| Hotstar Haymarket | HS041 | NULL (manual only) |

### Bonus Logic (same as Gong Cha)
- Tiered targets: each `(Store ID, Date)` row in `TargetHS` is a separate tier
- If `Sales >= Target Sales`, employee gets `Bonus Rate × Hours`
- Multiple tiers stack (both bonuses summed)
- Missing target entries → Bonus = 0

### Pandas Compatibility
- `rate` column explicitly set to `object` dtype before assigning mixed types
- Bonus `hours` set to `0` instead of `''` (newer pandas StringDtype strictness)

### Railway Deploy
- `requirements.txt`, `Procfile`, `.streamlit/config.toml`, `railway.toml` all configured
- **Blocker**: `datasource.py` creates DB engines at module import with hardcoded creds — will fail if those servers aren't reachable from Railway
