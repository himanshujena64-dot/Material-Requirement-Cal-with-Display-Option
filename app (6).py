"""
SAP MRP ENGINE — Full L1 to L4 with Phantom Handling
Streamlit Cloud deployment (GitHub-ready)

Fixes applied:
  1. Date formats preserved in output (26-Apr stays 26-Apr, Apr-26 stays Apr-26)
  2. AttributeError: duplicate month columns no longer crash the app
  3. Receipt Quantity file: adds to stock before shortage calculation
  4. Component search bar with ancestry tree (Graphviz)
  5. Shortage = positive, Excess = negative in output
"""

import io
import re
import pandas as pd
import streamlit as st

# ═══════════════════════════════════════════════════════════════
# PAGE CONFIG
# ═══════════════════════════════════════════════════════════════
st.set_page_config(page_title="SAP MRP Engine", page_icon="⚙️", layout="wide")
st.title("⚙️ SAP MRP Engine — L1 to L4")
st.caption("Phantom handling · Alt-aware · NET propagation · Dynamic date/month columns")

# ═══════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════
with st.sidebar:
    st.header("Configuration")
    PHANTOM   = st.text_input("Phantom Sp. Procurement code", value="50")
    VERIFY_L1 = st.text_input("Verify component L1",         value="0010748460")
    VERIFY_L2 = st.text_input("Verify component L2",         value="0010748458")
    VERIFY_L3 = st.text_input("Verify L3 (phantom)",         value="0010748814")
    VERIFY_L4 = st.text_input("Verify component L4",         value="0010300601DEL")
    st.divider()
    st.subheader("Upload files")
    bom_file      = st.file_uploader("BOM file (.xlsx)",              type=["xlsx","xls"], key="bom")
    req_file      = st.file_uploader("Req and Stock (.xlsx)",         type=["xlsx","xls"], key="req")
    prod_file     = st.file_uploader("Production Orders (.xlsx) — optional", type=["xlsx","xls"], key="prod")
    receipt_file  = st.file_uploader("Receipt Quantities (.xlsx) — optional",
                                     type=["xlsx","xls"], key="receipt",
                                     help="Components with GR/receipt qty to add to stock")
    run_btn = st.button("▶ Run MRP", type="primary", use_container_width=True)


# ═══════════════════════════════════════════════════════════════
# SHARED HELPERS
# ═══════════════════════════════════════════════════════════════

MONTH_ABBR = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12
}

def parse_col_to_date(col, default_year=2026):
    """
    Parse any date/month column header and return (pd.Timestamp, original_label).
    
    Handles ALL formats:
      • "26-Apr"       → date-first (day-Mon), year inferred
      • "26-Apr-26"    → date-first with year
      • "Apr-26"       → month-year (Mon-YY)
      • "Jan-2026"     → month-year (Mon-YYYY)
      • pd.Timestamp   → datetime object
      • "2026-04-26"   → ISO string

    The original_label is ALWAYS preserved so output columns match input exactly.
    Year for ambiguous "26-Apr" is inferred from other parsed columns if possible,
    defaulting to default_year.
    """
    orig_label = str(col).strip() if not isinstance(col, pd.Timestamp) else col.strftime("%d-%b-%y")

    # ── Already a Timestamp / date object ─────────────────────
    if isinstance(col, pd.Timestamp):
        return col.replace(day=1), col.strftime("%d-%b-%y")
    if hasattr(col, "year") and hasattr(col, "month"):
        ts = pd.Timestamp(col)
        return ts.replace(day=1), ts.strftime("%d-%b-%y")
    if pd.isna(col):
        return None, None

    s = str(col).strip()
    if not s:
        return None, None

    # ── Pattern 1: "26-Apr", "26-Apr-26", "26/Apr/2026" ───────
    # Day comes first, then 3-letter month abbreviation
    m = re.match(r'^(\d{1,2})[/\-]([A-Za-z]{3})(?:[/\-](\d{2,4}))?$', s)
    if m:
        day_s, mon_s, yr_s = m.group(1), m.group(2).lower(), m.group(3)
        mon_num = MONTH_ABBR.get(mon_s)
        if mon_num and 1 <= int(day_s) <= 31:
            yr = int(yr_s) + (2000 if yr_s and len(yr_s)==2 else 0) if yr_s else default_year
            try:
                ts = pd.Timestamp(year=yr, month=mon_num, day=int(day_s))
                return ts, s          # ← preserve original label exactly
            except Exception:
                pass

    # ── Pattern 2: "Apr-26", "Apr'26", "Jan-2026" ─────────────
    m = re.match(r'^([A-Za-z]{3})[-\'\s_](\d{2,4})$', s)
    if m:
        mon_s, yr_s = m.group(1).lower(), m.group(2)
        mon_num = MONTH_ABBR.get(mon_s)
        if mon_num:
            yr = int(yr_s) + (2000 if len(yr_s)==2 else 0)
            ts = pd.Timestamp(year=yr, month=mon_num, day=1)
            return ts, s             # ← preserve original label exactly

    # ── Pattern 3: ISO / general date string ──────────────────
    try:
        ts = pd.to_datetime(s, dayfirst=True, errors="raise")
        return ts, s
    except Exception:
        pass

    return None, None


def infer_year_from_parsed(parsed_list):
    """
    If date-only columns ("26-Apr") have no year, try to infer from
    any month-year columns in the same file. Returns the most common year found.
    """
    years = []
    for p in parsed_list:
        if p["ts"] is not None:
            years.append(p["ts"].year)
    return max(set(years), key=years.count) if years else 2026


def parse_all_month_cols(req_cols, non_month_set):
    """
    Parse all potential month columns from req header.
    Returns sorted list of dicts: {orig, ts, label}
    Two-pass: first pass to find year context, second to fill in date-only cols.
    """
    candidates = [c for c in req_cols if c not in non_month_set]

    # First pass — collect all parseable columns
    parsed = []
    for col in candidates:
        ts, label = parse_col_to_date(col)
        if ts is not None:
            parsed.append({"orig": col, "ts": ts, "label": label})

    # Infer year for any ambiguous date-only entries
    # (they already defaulted to 2026, but refine if other cols give better context)
    ref_year = infer_year_from_parsed(parsed)
    if ref_year != 2026:
        # Re-parse date-only columns with correct year
        parsed = []
        for col in candidates:
            ts, label = parse_col_to_date(col, default_year=ref_year)
            if ts is not None:
                parsed.append({"orig": col, "ts": ts, "label": label})

    # Sort chronologically
    parsed.sort(key=lambda x: x["ts"])

    # Remove exact timestamp duplicates (keep first occurrence)
    seen_ts, unique = set(), []
    for p in parsed:
        key = p["ts"]
        if key not in seen_ts:
            seen_ts.add(key)
            unique.append(p)

    return unique


def standardize_req_header(v):
    if pd.isna(v):
        return ""
    s = str(v).strip()
    mapping = {"alt.":"Alt","alternative":"Alt","bom header":"BOM Header"}
    return mapping.get(s.lower(), s)


def detect_requirement_header_row(file_obj, sheet_name="Requirement", scan_rows=20):
    raw = pd.read_excel(file_obj, sheet_name=sheet_name, header=None, nrows=scan_rows)
    best_row, best_score = 0, -1
    for i in range(len(raw)):
        cleaned = [standardize_req_header(x) for x in raw.iloc[i].tolist()]
        score = (10 if "BOM Header" in cleaned else 0) + \
                (5  if "Alt" in cleaned else 0) + \
                sum(1 for x in cleaned if parse_col_to_date(x)[0] is not None)
        if score > best_score:
            best_score, best_row = score, i
    if best_score < 10:
        raise ValueError("Could not detect Requirement header row reliably.")
    return best_row


def safe_series(df_or_series, col):
    """
    Safely extract a Series from a DataFrame or Series.
    If df[col] returns a DataFrame (duplicate columns), take the first column.
    """
    result = df_or_series[col]
    if isinstance(result, pd.DataFrame):
        result = result.iloc[:, 0]
    return result


def is_phantom(val):
    return str(val).strip() == PHANTOM


def empty_prod_summary():
    return pd.DataFrame(columns=["Component","Confirmed_Qty","Open_Production_Qty"])


# ═══════════════════════════════════════════════════════════════
# RECEIPT QUANTITY LOADER
# ═══════════════════════════════════════════════════════════════
def load_receipt_qty(receipt_file):
    """
    Load receipt / GR quantity file and return a Series: component → qty.
    Auto-detects material and quantity columns.
    Supports single-sheet Excel with any column names containing
    'material'/'component'/'part' and 'qty'/'quantity'/'gr'.
    """
    if receipt_file is None:
        return pd.Series(dtype=float)

    try:
        df = pd.read_excel(receipt_file)
        df.columns = df.columns.str.strip()

        mat_keywords = ["material","component","part number","part","mat"]
        qty_keywords = ["gr qty","gr quantity","receipt qty","receipt quantity",
                        "received qty","quantity","qty"]

        mat_col = next(
            (c for c in df.columns
             if any(k in c.lower() for k in mat_keywords)),
            df.columns[0]
        )
        qty_col = next(
            (c for c in df.columns
             if any(k in c.lower() for k in qty_keywords)
             and c != mat_col),
            None
        )

        if qty_col is None:
            st.warning("Receipt file: could not detect a quantity column — skipped.")
            return pd.Series(dtype=float)

        df[mat_col] = df[mat_col].astype(str).str.strip()
        df[qty_col] = pd.to_numeric(
            df[qty_col].astype(str).str.replace(",","",regex=False).str.strip(),
            errors="coerce"
        ).fillna(0)

        result = df.groupby(mat_col)[qty_col].sum()
        st.sidebar.success(
            f"Receipt file loaded: {len(result):,} components "
            f"(col: '{mat_col}' / '{qty_col}')"
        )
        return result

    except Exception as e:
        st.warning(f"Receipt file could not be loaded ({e}) — skipped.")
        return pd.Series(dtype=float)


# ═══════════════════════════════════════════════════════════════
# SEARCH + TREE HELPERS
# ═══════════════════════════════════════════════════════════════
def get_ancestry_paths(component, bom):
    comp_rows = bom[bom["Component"] == component][
        ["BOM Header","Alt","Level","Parent","Component",
         "Required Qty","Component descriptio","Special procurement"]
    ].drop_duplicates()

    paths = []
    for _, row in comp_rows.iterrows():
        path_comps = [row["Component"]]
        path_descs = [row["Component descriptio"]]
        path_qtys  = [float(row["Required Qty"])]
        path_sp    = [str(row["Special procurement"]).strip()]
        current    = row["Parent"]
        fg         = row["BOM Header"]
        alt        = row["Alt"]

        for _ in range(4):
            if current == fg:
                break
            pr_rows = bom[
                (bom["BOM Header"] == fg) &
                (bom["Alt"] == alt) &
                (bom["Component"] == current)
            ]
            if pr_rows.empty:
                break
            pr = pr_rows.iloc[0]
            path_comps.insert(0, pr["Component"])
            path_descs.insert(0, pr["Component descriptio"])
            path_qtys.insert(0,  float(pr["Required Qty"]))
            path_sp.insert(0,    str(pr["Special procurement"]).strip())
            current = pr["Parent"]

        paths.append({
            "fg": fg, "alt": str(alt), "level": int(row["Level"]),
            "path_comps": path_comps, "path_descs": path_descs,
            "path_qtys": path_qtys,   "path_sp": path_sp,
        })
    return paths


def build_dot_tree(component, paths, req_df, months, stock, prod_summary):
    fg_demand = {}
    for p in paths:
        rows = req_df[(req_df["BOM Header"]==p["fg"]) & (req_df["Alt"]==p["alt"])]
        total = rows[months].sum(numeric_only=True).sum() if not rows.empty else 0
        fg_demand[(p["fg"], p["alt"])] = float(total)

    from functools import reduce
    result_dfs = st.session_state.get("mrp_results", {})
    gross_map, shortage_map = {}, {}
    for key in ["result_l1","result_l2","result_l3","result_l4"]:
        df = result_dfs.get(key)
        if df is None or df.empty:
            continue
        agg = df.groupby("Component")[["Gross_Requirement","Demand_Shortage_Excess"]].sum()
        for comp, row in agg.iterrows():
            gross_map[comp]    = gross_map.get(comp, 0) + row["Gross_Requirement"]
            shortage_map[comp] = shortage_map.get(comp, 0) + row["Demand_Shortage_Excess"]

    def trunc(s, n=20):
        return (str(s)[:n]+"…") if len(str(s))>n else str(s)

    node_attrs, edges, seen_edges = {}, [], set()

    for path in paths:
        fg, alt = path["fg"], path["alt"]
        demand  = fg_demand.get((fg, alt), 0)
        fg_id   = f"FG_{fg}_A{alt}".replace("-","_").replace(".","_")
        node_attrs[fg_id] = (
            f'label="FG: {fg}\\nAlt: {alt}\\nTotal demand: {demand:,.0f}"'
            f' shape=box style="filled,rounded" fillcolor="#2e86c1"'
            f' fontcolor=white fontsize=11'
        )
        prev_id = fg_id

        for comp, desc, qty, sp in zip(
            path["path_comps"], path["path_descs"],
            path["path_qtys"],  path["path_sp"]
        ):
            is_tgt = (comp == component)
            is_ph  = (sp == PHANTOM)
            nid    = f"N_{comp}_FG_{fg}_A{alt}".replace("-","_").replace(".","_").replace("+","p")

            gross    = gross_map.get(comp, 0)
            shortage = shortage_map.get(comp, 0)
            stk      = float(stock.get(comp, 0))

            if is_tgt:
                prod_row = prod_summary[prod_summary["Component"]==comp]
                conf  = float(prod_row["Confirmed_Qty"].iloc[0])       if not prod_row.empty else 0
                oprod = float(prod_row["Open_Production_Qty"].iloc[0]) if not prod_row.empty else 0
                label = (f"{trunc(comp)}\\n{trunc(desc)}\\n"
                         f"Stock: {stk:,.0f} | Conf: {conf:,.0f}\\n"
                         f"Open PO: {oprod:,.0f}\\n"
                         f"Gross: {gross:,.0f} | Short/Excess: {shortage:,.0f}")
                node_attrs[nid] = (
                    f'label="{label}" shape=box style="filled,rounded"'
                    f' fillcolor="#1e8449" fontcolor=white fontsize=11 penwidth=2.5'
                )
            elif is_ph:
                label = (f"PHANTOM\\n{trunc(comp)}\\n{trunc(desc)}\\n"
                         f"qty={qty:g} (pass-through)")
                node_attrs[nid] = (
                    f'label="{label}" shape=box style="filled,dashed"'
                    f' fillcolor="#f39c12" fontcolor="#333" fontsize=10'
                )
            else:
                label = (f"{trunc(comp)}\\n{trunc(desc)}\\n"
                         f"Qty: {qty:g} | Stock: {stk:,.0f}\\n"
                         f"Gross: {gross:,.0f} | Short/Excess: {shortage:,.0f}")
                node_attrs[nid] = (
                    f'label="{label}" shape=box style="filled,rounded"'
                    f' fillcolor="#f9e79f" fontcolor="#333" fontsize=10'
                )

            ek = (prev_id, nid)
            if ek not in seen_edges:
                edges.append((prev_id, nid, f"×{qty:g}"))
                seen_edges.add(ek)
            prev_id = nid

    lines = [
        "digraph MRP {",
        "  rankdir=TB;",
        '  node [fontname="Arial"];',
        '  edge [fontname="Arial" fontsize=10];',
        "  graph [splines=ortho nodesep=0.6 ranksep=0.8];",
    ]
    for nid, attrs in node_attrs.items():
        lines.append(f'  "{nid}" [{attrs}];')
    for src, dst, lbl in edges:
        lines.append(f'  "{src}" -> "{dst}" [label="{lbl}"];')
    lines.append("}")
    return "\n".join(lines)


def show_search_section(bom, req_df, months, stock, prod_summary):
    st.divider()
    st.subheader("🔍 Component Search")
    st.caption("Enter any component code to see demand, shortage, production orders and BOM ancestry tree.")

    scol, _ = st.columns([2,3])
    with scol:
        comp = st.text_input("Component code", placeholder="e.g. 0010748458",
                             label_visibility="collapsed").strip()
    if not comp:
        return

    r = st.session_state.get("mrp_results", {})
    result_dfs = {k: r.get(k) for k in ["result_l1","result_l2","result_l3","result_l4"]}

    found_in = {}
    for lbl, df in result_dfs.items():
        if df is not None and not df.empty and comp in df["Component"].values:
            found_in[lbl] = df[df["Component"]==comp].copy()

    bom_in = bom[bom["Component"]==comp]
    if bom_in.empty and not found_in:
        st.warning(f"`{comp}` not found in BOM or MRP results.")
        return

    desc  = bom_in["Component descriptio"].iloc[0] if not bom_in.empty else "—"
    ptype = bom_in["Procurement type"].iloc[0]     if not bom_in.empty else "—"
    sp    = bom_in["Special procurement"].iloc[0]  if not bom_in.empty else "—"
    stk   = float(stock.get(comp, 0))
    prod_row = prod_summary[prod_summary["Component"]==comp]
    conf_qty = float(prod_row["Confirmed_Qty"].iloc[0])       if not prod_row.empty else 0
    open_qty = float(prod_row["Open_Production_Qty"].iloc[0]) if not prod_row.empty else 0

    ph_badge = " 🔶 PHANTOM" if str(sp).strip()==PHANTOM else ""
    st.markdown(f"### `{comp}` — {desc}{ph_badge}")

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Stock on hand",      f"{stk:,.3f}")
    c2.metric("Confirmed prod qty", f"{conf_qty:,.0f}")
    c3.metric("Open production qty",f"{open_qty:,.0f}")
    c4.metric("Procurement type",   ptype)
    c5.metric("Sp. procurement",    sp if sp not in ("","nan") else "—")

    if found_in:
        st.markdown("#### Monthly demand & shortage/excess")
        all_rows = pd.concat(found_in.values(), ignore_index=True)
        mo = {m:i for i,m in enumerate(months)}
        monthly = (all_rows.groupby("Month", as_index=False)
                   .agg(Gross_Requirement=("Gross_Requirement","sum"),
                        Stock_Used=("Stock_Used","sum"),
                        Demand_Shortage_Excess=("Demand_Shortage_Excess","sum"),
                        Stock_Remaining=("Stock_Remaining","last")))
        monthly["_ord"] = monthly["Month"].map(mo)
        monthly = monthly.sort_values("_ord").drop(columns="_ord")
        monthly["Cumulative"] = monthly["Demand_Shortage_Excess"].cumsum()

        def hl(row):
            # Red for shortage (>0), Green for excess (<0)
            if row["Demand_Shortage_Excess"] > 0:
                return ["background-color:#ffe0e0"] * len(row)  # shortage - red
            elif row["Demand_Shortage_Excess"] < 0:
                return ["background-color:#e0f7e0"] * len(row)  # excess - green
            return [""] * len(row)

        st.dataframe(
            monthly.style.apply(hl, axis=1).format({
                "Gross_Requirement":"{:,.2f}",
                "Stock_Used":"{:,.2f}",
                "Demand_Shortage_Excess":"{:,.2f}",
                "Stock_Remaining":"{:,.2f}",
                "Cumulative":"{:,.2f}"
            }),
            use_container_width=True, hide_index=True)

        s1,s2,s3,s4 = st.columns(4)
        total_shortage = monthly[monthly["Demand_Shortage_Excess"] > 0]["Demand_Shortage_Excess"].sum()
        total_excess = abs(monthly[monthly["Demand_Shortage_Excess"] < 0]["Demand_Shortage_Excess"].sum())
        s1.metric("Total gross req",     f"{monthly['Gross_Requirement'].sum():,.2f}")
        s2.metric("Total stock consumed",f"{monthly['Stock_Used'].sum():,.2f}")
        s3.metric("Total shortage",      f"{total_shortage:,.2f}")
        s4.metric("Total excess stock",  f"{total_excess:,.2f}")
    else:
        st.info("Component in BOM but not in MRP results (phantom or no demand).")

    st.markdown("#### BOM ancestry tree")
    st.caption("🔵 FG   🟡 Intermediate   🟠 Phantom (pass-through)   🟢 Searched component")
    paths = get_ancestry_paths(comp, bom)
    if not paths:
        st.info("No ancestry paths found.")
        return

    fg_rows = []
    for p in paths:
        rows = req_df[(req_df["BOM Header"]==p["fg"]) & (req_df["Alt"]==p["alt"])]
        total = rows[months].sum(numeric_only=True).sum() if not rows.empty else 0
        mv = {m: float(rows[m].sum()) if not rows.empty else 0 for m in months}
        fg_rows.append({"FG code":p["fg"],"Alt":p["alt"],"BOM level":p["level"],
                        "Total demand":f"{total:,.0f}", **{m:f"{mv[m]:,.0f}" for m in months}})

    fg_df = pd.DataFrame(fg_rows).drop_duplicates(subset=["FG code","Alt"])
    st.dataframe(fg_df, use_container_width=True, hide_index=True)

    MAX_PATHS = 12
    display_paths = paths[:MAX_PATHS]
    if len(paths) > MAX_PATHS:
        st.caption(f"⚠️ Showing {MAX_PATHS} of {len(paths)} ancestry paths.")

    dot = build_dot_tree(comp, display_paths, req_df, months, stock, prod_summary)
    try:
        st.graphviz_chart(dot, use_container_width=True)
    except Exception as e:
        st.error(f"Tree render error: {e}")
        with st.expander("DOT source"):
            st.code(dot, language="dot")


# ═══════════════════════════════════════════════════════════════
# MAIN MRP FUNCTION
# ═══════════════════════════════════════════════════════════════
def run_mrp(bom_file, req_file, prod_file, receipt_file):
    logs   = []
    log    = lambda msg: logs.append(msg)
    status = st.status("Running MRP engine ...", expanded=True)

    # ── SECTION 1: BOM ────────────────────────────────────────
    with status:
        st.write("► Building clean BOM ...")

    bom = pd.read_excel(bom_file)
    bom.columns = bom.columns.str.strip()
    if "Alt." in bom.columns:
        bom = bom.rename(columns={"Alt.":"Alt"})

    bom["Level"] = pd.to_numeric(bom["Level"], errors="coerce").fillna(0).astype(int)
    bom = bom.reset_index(drop=True)

    parents, stack = [], {}
    for i in range(len(bom)):
        lvl    = bom.loc[i,"Level"]
        parent = bom.loc[i,"BOM Header"] if lvl==1 else stack.get(lvl-1)
        stack  = {k:v for k,v in stack.items() if k<=lvl}
        stack[lvl] = bom.loc[i,"Component"]
        parents.append(parent)
    bom["Parent"] = parents

    drop_cols = ["Plant","Usage","Quantity","Unit","BOM L/T","BOM code","Item",
                 "Mat. Group","Mat. Group Desc.","Pur. Group","Pur. Group Desc.",
                 "MRP Controller","MRP Controller Desc."]
    bom = bom.drop(columns=[c for c in drop_cols if c in bom.columns], errors="ignore")

    for old,new in [("Component description","Component descriptio"),
                    ("BOM header description","BOM header descripti")]:
        if old in bom.columns:
            bom = bom.rename(columns={old:new})

    keep = ["BOM Header","BOM header descripti","Alt","Level","Path","Parent",
            "Component","Component descriptio","Required Qty","Base unit",
            "Procurement type","Special procurement"]

    missing_bom = [c for c in ["BOM Header","Level","Component","Required Qty"]
                   if c not in bom.columns]
    if missing_bom:
        st.error(f"Missing required BOM columns: {missing_bom}")
        return None

    bom = bom[[c for c in keep if c in bom.columns]].copy()
    for col,default in [("Alt","0"),("Special procurement",""),
                        ("Procurement type",""),("Component descriptio","")]:
        if col not in bom.columns:
            bom[col] = default

    bom["Component"]            = bom["Component"].astype(str).str.strip()
    bom["BOM Header"]           = bom["BOM Header"].astype(str).str.strip()
    bom["Special procurement"]  = bom["Special procurement"].astype(str).str.strip()
    bom["Procurement type"]     = bom["Procurement type"].astype(str).str.strip()
    bom["Component descriptio"] = bom["Component descriptio"].astype(str).str.strip()
    bom["Required Qty"]         = pd.to_numeric(bom["Required Qty"], errors="coerce").fillna(0)
    # FIX 1: normalise Alt to integer string to prevent join failures
    bom["Alt"] = pd.to_numeric(bom["Alt"], errors="coerce").fillna(0).astype(int).astype(str)

    log(f"BOM rows: {len(bom):,}  |  Unique headers: {bom['BOM Header'].nunique()}")

    # ── SECTION 2: REQUIREMENT & STOCK ────────────────────────
    with status:
        st.write("► Loading Requirement and Stock ...")

    req_header_row = detect_requirement_header_row(req_file, sheet_name="Requirement")
    req_file.seek(0)

    req = pd.read_excel(req_file, sheet_name="Requirement", header=None)
    raw_headers = req.iloc[req_header_row].tolist()
    req.columns = [standardize_req_header(x) for x in raw_headers]
    req = req.iloc[req_header_row+1:].reset_index(drop=True)

    # FIX 2a: strip blank column names
    req = req.loc[:, [str(c).strip()!="" for c in req.columns]]

    # FIX 2b: deduplicate columns BEFORE accessing any — this is what caused
    # "DataFrame has no attribute str": duplicate col names return DataFrame not Series
    req = req.loc[:, ~pd.Index(req.columns).duplicated(keep="first")]

    missing_req = [c for c in ["BOM Header","Alt"] if c not in req.columns]
    if missing_req:
        st.error(f"Missing Requirement columns: {missing_req}. Found: {req.columns.tolist()}")
        return None

    req["BOM Header"] = req["BOM Header"].astype(str).str.strip()
    # FIX 1 (continued): same Alt normalisation
    req["Alt"] = pd.to_numeric(req["Alt"], errors="coerce").fillna(0).astype(int).astype(str)

    # FIX 3: dynamic month/date detection preserving original labels
    NON_MONTH_COLS = {"BOM Header","Alt"}
    parsed = parse_all_month_cols(req.columns.tolist(), NON_MONTH_COLS)

    if not parsed:
        st.error(f"No date/month columns detected. Found: {req.columns.tolist()}")
        return None

    # Rename only if original label ≠ parsed label (usually they're the same now)
    rename_map = {p["orig"]: p["label"] for p in parsed if p["orig"] != p["label"]}
    if rename_map:
        req = req.rename(columns=rename_map)

    months      = [p["label"] for p in parsed]
    MONTH_ORDER = {m:i for i,m in enumerate(months)}

    # FIX 2c: coerce each month column safely via safe_series to handle any
    # remaining duplicates that could slip through
    for m in months:
        col_data = safe_series(req, m)
        req[m] = pd.to_numeric(
            col_data.astype(str).str.replace(",","",regex=False).str.strip(),
            errors="coerce"
        ).fillna(0)

    log(f"Column format: '{parsed[0]['label']}' (preserved from source)")
    log(f"Months detected ({len(months)}): {months}")

    # Stock
    req_file.seek(0)
    stock_raw = pd.read_excel(req_file, sheet_name="Stock",
                              usecols=[0,1], header=0,
                              names=["Component","Stock_Qty"])
    stock_raw = stock_raw.dropna(subset=["Component"]).copy()
    stock_raw["Component"] = stock_raw["Component"].astype(str).str.strip()
    stock_raw["Stock_Qty"] = pd.to_numeric(
        stock_raw["Stock_Qty"].astype(str).str.replace(",","",regex=False).str.strip(),
        errors="coerce"
    ).fillna(0)
    stock = stock_raw.groupby("Component")["Stock_Qty"].sum()

    # FIX 4: Receipt quantity — add to stock before MRP
    receipt_qty = load_receipt_qty(receipt_file)
    receipt_added = 0
    if not receipt_qty.empty:
        for comp, qty in receipt_qty.items():
            current = float(stock.get(comp, 0))
            stock[comp] = current + float(qty)
            receipt_added += 1
        log(f"Receipt quantities added for {receipt_added} components (stock updated before MRP)")

    req_long = req.melt(
        id_vars=["BOM Header","Alt"], value_vars=months,
        var_name="Month", value_name="FG_Demand"
    )
    req_long = req_long[req_long["FG_Demand"]>0].copy()

    log(f"Requirement rows (non-zero): {len(req_long):,}")
    log(f"Stock components (incl. receipts): {len(stock):,}")

    # ── SECTION 3: PRODUCTION ORDERS ──────────────────────────
    with status:
        st.write("► Loading Production Orders ...")

    prod_summary = empty_prod_summary()
    if prod_file is not None:
        try:
            coois = pd.read_excel(prod_file)
            coois.columns = coois.columns.str.strip()
            if not coois.empty:
                status_col = next((c for c in coois.columns if "status" in c.lower()), None)
                mat_col    = next((c for c in coois.columns if "material" in c.lower()
                                   and "description" not in c.lower()), None)
                ord_col    = next((c for c in coois.columns if "order" in c.lower()
                                   and ("qty" in c.lower() or "quantity" in c.lower())), None)
                del_col    = next((c for c in coois.columns
                                   if "deliver" in c.lower()
                                   or ("quantity" in c.lower() and "gr" in c.lower())), None)
                conf_col   = next((c for c in coois.columns
                                   if "confirm" in c.lower() and "quantity" in c.lower()), None)

                if all([status_col, mat_col, ord_col
