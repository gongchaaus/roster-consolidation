# Agent Memory

## Hot Star Sales Integration

### Architecture
- `app.py` `calc_timesheets_n_billings()` splits bonus into **Gong Cha** (`Sxxx` Store ID) and **Hot Star** (`HSxxx` Store ID) after StoreReference merge
- `gc_bonus` uses existing `recid_plo` → `gong_cha_redcat_db.d_txnlines` flow (unchanged)
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
