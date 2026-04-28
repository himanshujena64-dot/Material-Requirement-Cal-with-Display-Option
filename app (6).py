"""
SAP MRP ENGINE + SEGMENT PRODUCTION CAPACITY CALCULATOR
========================================================
Tab 1: Full L1-L4 MRP with phantom handling, NET propagation,
        date/month columns, receipt qty, component search + ancestry tree.
Tab 2: Segment-wise set capacity (IDU+ODU) limited by import parts.
        Upload Segment & Import Part file (optional) to activate.
"""

import io
import re
from collections import defaultdict

import numpy as np
import pandas as pd
import streamlit as st
from scipy.optimize import linprog

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

    st.subheader("MRP files")
    bom_file     = st.file_uploader("BOM file (.xlsx) ✱",              type=["xlsx","xls"], key="bom")
    req_file     = st.file_uploader("Req and Stock (.xlsx) ✱",         type=["xlsx","xls"], key="req")
    prod_file    = st.file_uploader("Production Orders (.xlsx) — optional", type=["xlsx","xls"], key="prod")
    receipt_file = st.file_uploader("Receipt Quantities (.xlsx) — optional",
                                    type=["xlsx","xls"], key="receipt",
                                    help="GR/receipt quantities added to stock before MRP")
    run_mrp_btn  = st.button("▶ Run MRP", type="primary", use_container_width=True)

    st.divider()
    st.subheader("Segment Capacity (optional)")
    seg_imp_file = st.file_uploader(
        "Segment & Import Part file (.xlsx)",
        type=["xlsx","xls"], key="seg",
        help="Sheet 1: Import Part List  |  Sheet 2: Segment (IDU / ODU codes)")
    run_seg_btn  = st.button("▶ Run Segment Capacity", use_container_width=True)


# ═══════════════════════════════════════════════════════════════
# SHARED HELPERS — date/month parsing
# ═══════════════════════════════════════════════════════════════
MONTH_ABBR = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12
}

def parse_col_to_date(col, default_year=2026):
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
    m = re.match(r'^(\d{1,2})[/\-]([A-Za-z]{3})(?:[/\-](\d{2,4}))?$', s)
    if m:
        day_s, mon_s, yr_s = m.group(1), m.group(2).lower(), m.group(3)
        mon_num = MONTH_ABBR.get(mon_s)
        if mon_num and 1 <= int(day_s) <= 31:
            yr = int(yr_s)+(2000 if yr_s and len(yr_s)==2 else 0) if yr_s else default_year
            try:
                ts = pd.Timestamp(year=yr, month=mon_num, day=int(day_s))
                return ts, s
            except Exception:
                pass
    m = re.match(r'^([A-Za-z]{3})[-\'\s_](\d{2,4})$', s)
    if m:
        mon_s, yr_s = m.group(1).lower(), m.group(2)
        mon_num = MONTH_ABBR.get(mon_s)
        if mon_num:
            yr = int(yr_s)+(2000 if len(yr_s)==2 else 0)
            ts = pd.Timestamp(year=yr, month=mon_num, day=1)
            return ts, s
    try:
        ts = pd.to_datetime(s, dayfirst=True, errors="raise")
        return ts, s
    except Exception:
        pass
    return None, None


def infer_year_from_parsed(parsed_list):
    years = [p["ts"].year for p in parsed_list if p["ts"] is not None]
    return max(set(years), key=years.count) if years else 2026


def parse_all_month_cols(req_cols, non_month_set):
    candidates = [c for c in req_cols if c not in non_month_set]
    parsed = []
    for col in candidates:
        ts, label = parse_col_to_date(col)
        if ts is not None:
            parsed.append({"orig": col, "ts": ts, "label": label})
    ref_year = infer_year_from_parsed(parsed)
    if ref_year != 2026:
        parsed = []
        for col in candidates:
            ts, label = parse_col_to_date(col, default_year=ref_year)
            if ts is not None:
                parsed.append({"orig": col, "ts": ts, "label": label})
    parsed.sort(key=lambda x: x["ts"])
    seen_ts, unique = set(), []
    for p in parsed:
        if p["ts"] not in seen_ts:
            seen_ts.add(p["ts"])
            unique.append(p)
    return unique


def standardize_req_header(v):
    if pd.isna(v):
        return ""
    s = str(v).strip()
    return {"alt.":"Alt","alternative":"Alt","bom header":"BOM Header"}.get(s.lower(), s)


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
    result = df_or_series[col]
    return result.iloc[:, 0] if isinstance(result, pd.DataFrame) else result


def is_phantom(val):
    return str(val).strip() == PHANTOM


def empty_prod_summary():
    return pd.DataFrame(columns=["Component","Confirmed_Qty","Open_Production_Qty"])


def load_receipt_qty(receipt_file):
    if receipt_file is None:
        return pd.Series(dtype=float)
    try:
        df = pd.read_excel(receipt_file)
        df.columns = df.columns.str.strip()
        mat_kw = ["material","component","part number","part","mat"]
        qty_kw = ["gr qty","gr quantity","receipt qty","receipt quantity","received qty","quantity","qty"]
        mat_col = next((c for c in df.columns if any(k in c.lower() for k in mat_kw)), df.columns[0])
        qty_col = next((c for c in df.columns if any(k in c.lower() for k in qty_kw) and c != mat_col), None)
        if qty_col is None:
            st.warning("Receipt file: quantity column not detected — skipped.")
            return pd.Series(dtype=float)
        df[mat_col] = df[mat_col].astype(str).str.strip()
        df[qty_col] = pd.to_numeric(
            df[qty_col].astype(str).str.replace(",","",regex=False).str.strip(),
            errors="coerce").fillna(0)
        result = df.groupby(mat_col)[qty_col].sum()
        st.sidebar.success(f"Receipt file: {len(result):,} components loaded.")
        return result
    except Exception as e:
        st.warning(f"Receipt file error ({e}) — skipped.")
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
        current, fg, alt = row["Parent"], row["BOM Header"], row["Alt"]
        for _ in range(4):
            if current == fg:
                break
            pr_rows = bom[(bom["BOM Header"]==fg)&(bom["Alt"]==alt)&(bom["Component"]==current)]
            if pr_rows.empty:
                break
            pr = pr_rows.iloc[0]
            path_comps.insert(0, pr["Component"])
            path_descs.insert(0, pr["Component descriptio"])
            path_qtys.insert(0,  float(pr["Required Qty"]))
            path_sp.insert(0,    str(pr["Special procurement"]).strip())
            current = pr["Parent"]
        paths.append({"fg":fg,"alt":str(alt),"level":int(row["Level"]),
                      "path_comps":path_comps,"path_descs":path_descs,
                      "path_qtys":path_qtys,"path_sp":path_sp})
    return paths


def build_dot_tree(component, paths, req_df, months, stock, prod_summary):
    fg_demand = {}
    for p in paths:
        rows = req_df[(req_df["BOM Header"]==p["fg"])&(req_df["Alt"]==p["alt"])]
        fg_demand[(p["fg"],p["alt"])] = float(rows[months].sum(numeric_only=True).sum()) if not rows.empty else 0

    result_dfs = st.session_state.get("mrp_results", {})
    gross_map, shortage_map = {}, {}
    for key in ["result_l1","result_l2","result_l3","result_l4"]:
        df = result_dfs.get(key)
        if df is None or df.empty:
            continue
        agg = df.groupby("Component")[["Gross_Requirement","Shortage"]].sum()
        for comp, row in agg.iterrows():
            gross_map[comp]    = gross_map.get(comp,0) + row["Gross_Requirement"]
            shortage_map[comp] = shortage_map.get(comp,0) + row["Shortage"]

    def trunc(s, n=20):
        return (str(s)[:n]+"…") if len(str(s))>n else str(s)

    node_attrs, edges, seen_edges = {}, [], set()
    for path in paths:
        fg, alt = path["fg"], path["alt"]
        fg_id = f"FG_{fg}_A{alt}".replace("-","_").replace(".","_")
        node_attrs[fg_id] = (
            f'label="FG: {fg}\\nAlt: {alt}\\nTotal demand: {fg_demand.get((fg,alt),0):,.0f}"'
            f' shape=box style="filled,rounded" fillcolor="#2e86c1" fontcolor=white fontsize=11'
        )
        prev_id = fg_id
        for comp, desc, qty, sp in zip(path["path_comps"],path["path_descs"],path["path_qtys"],path["path_sp"]):
            is_tgt = (comp == component)
            is_ph  = (sp == PHANTOM)
            nid = f"N_{comp}_FG_{fg}_A{alt}".replace("-","_").replace(".","_").replace("+","p")
            gross = gross_map.get(comp,0); shortage = shortage_map.get(comp,0); stk = float(stock.get(comp,0))
            if is_tgt:
                prod_row = prod_summary[prod_summary["Component"]==comp]
                conf  = float(prod_row["Confirmed_Qty"].iloc[0])       if not prod_row.empty else 0
                oprod = float(prod_row["Open_Production_Qty"].iloc[0]) if not prod_row.empty else 0
                label = (f"{trunc(comp)}\\n{trunc(desc)}\\nStock: {stk:,.0f} | Conf: {conf:,.0f}\\n"
                         f"Open PO: {oprod:,.0f}\\nGross: {gross:,.0f} | Short: {shortage:,.0f}")
                node_attrs[nid] = (f'label="{label}" shape=box style="filled,rounded"'
                                   f' fillcolor="#1e8449" fontcolor=white fontsize=11 penwidth=2.5')
            elif is_ph:
                label = f"PHANTOM\\n{trunc(comp)}\\n{trunc(desc)}\\nqty={qty:g} (pass-through)"
                node_attrs[nid] = (f'label="{label}" shape=box style="filled,dashed"'
                                   f' fillcolor="#f39c12" fontcolor="#333" fontsize=10')
            else:
                label = (f"{trunc(comp)}\\n{trunc(desc)}\\nQty: {qty:g} | Stock: {stk:,.0f}\\n"
                         f"Gross: {gross:,.0f} | Short: {shortage:,.0f}")
                node_attrs[nid] = (f'label="{label}" shape=box style="filled,rounded"'
                                   f' fillcolor="#f9e79f" fontcolor="#333" fontsize=10')
            ek = (prev_id, nid)
            if ek not in seen_edges:
                edges.append((prev_id, nid, f"×{qty:g}"))
                seen_edges.add(ek)
            prev_id = nid

    lines = ["digraph MRP {","  rankdir=TB;",
             '  node [fontname="Arial"];','  edge [fontname="Arial" fontsize=10];',
             "  graph [splines=ortho nodesep=0.6 ranksep=0.8];"]
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
    found_in = {}
    for lbl in ["result_l1","result_l2","result_l3","result_l4"]:
        df = r.get(lbl)
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
        st.markdown("#### Monthly demand & shortage")
        all_rows = pd.concat(found_in.values(), ignore_index=True)
        mo = {m:i for i,m in enumerate(months)}
        monthly = (all_rows.groupby("Month", as_index=False)
                   .agg(Gross_Requirement=("Gross_Requirement","sum"),
                        Stock_Used=("Stock_Used","sum"),
                        Shortage=("Shortage","sum"),
                        Stock_Remaining=("Stock_Remaining","last")))
        monthly["_ord"] = monthly["Month"].map(mo)
        monthly = monthly.sort_values("_ord").drop(columns="_ord")

        # Net Position = Stock − cumulative gross demand
        monthly["Cumul_Gross"] = monthly["Gross_Requirement"].cumsum()
        monthly["Net_Position"] = stk - monthly["Cumul_Gross"]

        def hl(row):
            if row["Net_Position"] < 0:
                return ["background-color:#ffe0e0"]*len(row)
            elif row["Net_Position"] > 0:
                return ["background-color:#e8f8e8"]*len(row)
            return [""]*len(row)

        display_cols = ["Month","Gross_Requirement","Stock_Used","Stock_Remaining","Net_Position"]
        st.dataframe(
            monthly[display_cols].style.apply(hl, axis=1).format({
                "Gross_Requirement":"{:,.2f}","Stock_Used":"{:,.2f}",
                "Stock_Remaining":"{:,.2f}","Net_Position":"{:,.2f}"}),
            use_container_width=True, hide_index=True)
        st.caption("Net Position = Stock − cumulative gross demand | 🟢 positive = surplus · 🔴 negative = shortage")

        s1,s2,s3,s4 = st.columns(4)
        s1.metric("Total gross req",  f"{monthly['Gross_Requirement'].sum():,.2f}")
        s2.metric("Opening stock",    f"{stk:,.2f}")
        final_net = monthly["Net_Position"].iloc[-1]
        s3.metric("Final net position", f"{final_net:,.2f}",
                  delta="surplus" if final_net>=0 else "shortage",
                  delta_color="normal" if final_net>=0 else "inverse")
        s4.metric("Months in shortage", f"{(monthly['Net_Position']<0).sum()} / {len(monthly)}")
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
        rows = req_df[(req_df["BOM Header"]==p["fg"])&(req_df["Alt"]==p["alt"])]
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
# SEGMENT CAPACITY HELPERS
# ═══════════════════════════════════════════════════════════════
def load_segment_import(seg_imp_file):
    xl     = pd.ExcelFile(seg_imp_file)
    sheets = xl.sheet_names

    imp_sheet = next((s for s in sheets if "import" in s.lower()), sheets[0])
    imp_df    = pd.read_excel(seg_imp_file, sheet_name=imp_sheet, header=0)
    import_parts = sorted(imp_df.iloc[:,0].dropna().astype(str).str.strip().unique())

    seg_sheet = next((s for s in sheets if "seg" in s.lower()), sheets[min(1, len(sheets)-1)])
    seg_df    = pd.read_excel(seg_imp_file, sheet_name=seg_sheet, header=0)
    seg_df.columns = [str(c).strip() for c in seg_df.columns]
    seg_df = seg_df.rename(columns={seg_df.columns[0]:"Code", seg_df.columns[1]:"Segment"})
    seg_df["Code"]    = seg_df["Code"].astype(str).str.strip()
    seg_df["Segment"] = seg_df["Segment"].astype(str).str.strip()

    seg_df["Unit_Type"] = "OTHER"
    seg_df.loc[seg_df["Segment"].str.contains("IDU", case=False), "Unit_Type"] = "IDU"
    seg_df.loc[seg_df["Segment"].str.contains("ODU", case=False), "Unit_Type"] = "ODU"
    seg_df["PairKey"] = (seg_df["Segment"]
        .str.replace("-IDU","",regex=False).str.replace("-ODU","",regex=False)
        .str.replace(" IDU","",regex=False).str.replace(" ODU","",regex=False).str.strip())

    return import_parts, seg_df


def explode_bom_for_seg(bom_header, bom_df, target_set, phantom=None):
    """BOM explosion returning {import_part: qty_per_unit}. Uses primary Alt."""
    if phantom is None:
        phantom = PHANTOM
    alts = sorted(bom_df[bom_df["BOM Header"]==bom_header]["Alt"].unique())
    if not alts:
        return {}
    alt = alts[0]
    sub = bom_df[(bom_df["BOM Header"]==bom_header)&(bom_df["Alt"]==alt)]

    children_map = defaultdict(list)
    for _, r in sub.iterrows():
        children_map[r["Parent"]].append(r)

    result = {}

    def dfs(node, accum, depth=0):
        if depth > 12:
            return
        for r in children_map.get(node, []):
            comp = r["Component"]
            qty  = float(r["Required Qty"])
            sp   = r["Special procurement"]
            eff  = accum * (1.0 if sp == phantom else qty)
            if comp in target_set:
                result[comp] = result.get(comp, 0) + eff
            dfs(comp, eff, depth+1)

    dfs(bom_header, 1.0)
    return result


def run_segment_capacity(bom, stock, seg_imp_file):
    """
    Run segment production capacity calculation.
    Uses BOM and stock already loaded from the MRP run.
    Returns result dict.
    """
    status = st.status("Running Segment Capacity calculation ...", expanded=True)

    with status:
        st.write("► Loading Segment and Import Part data ...")
    import_parts, seg_df = load_segment_import(seg_imp_file)
    target_set  = set(import_parts)
    bom_headers = set(bom["BOM Header"].unique())

    with status:
        st.write("► Exploding BOM for each IDU / ODU model ...")
    models_df  = seg_df[seg_df["Unit_Type"].isin(["IDU","ODU"])]
    model_reqs = {}
    not_in_bom = []
    for _, row in models_df.iterrows():
        code = row["Code"]
        if code in bom_headers:
            model_reqs[code] = explode_bom_for_seg(code, bom, target_set)
        else:
            not_in_bom.append(code)

    if not_in_bom:
        st.warning(f"Models not in BOM (excluded): {', '.join(not_in_bom)}")

    with status:
        st.write("► Building segment requirements ...")
    segments_data, skipped_segs = {}, []

    for segment, grp in models_df.groupby("PairKey"):
        idu_codes = grp[grp["Unit_Type"]=="IDU"]["Code"].tolist()
        odu_codes = grp[grp["Unit_Type"]=="ODU"]["Code"].tolist()
        if not idu_codes or not odu_codes:
            skipped_segs.append(f"{segment} (missing {'ODU' if not odu_codes else 'IDU'})")
            continue
        idu_reqs = [model_reqs[c] for c in idu_codes if c in model_reqs and model_reqs[c]]
        odu_reqs = [model_reqs[c] for c in odu_codes if c in model_reqs and model_reqs[c]]
        if not idu_reqs or not odu_reqs:
            skipped_segs.append(f"{segment} (no BOM data)")
            continue
        all_parts = set()
        for r in idu_reqs + odu_reqs:
            all_parts.update(r.keys())
        combined = {}
        for p in all_parts:
            total = (max((r.get(p,0) for r in idu_reqs), default=0) +
                     max((r.get(p,0) for r in odu_reqs), default=0))
            if total > 0:
                combined[p] = total
        segments_data[segment] = {
            "combined_req": combined,
            "idu_codes":    idu_codes,
            "odu_codes":    odu_codes,
            "idu_count":    len(idu_codes),
            "odu_count":    len(odu_codes),
        }

    if not segments_data:
        st.error("No segments with valid IDU+ODU BOM data found.")
        return None

    with status:
        st.write("► Running LP optimisation (maximise total sets) ...")
    segs  = list(segments_data.keys())
    n_seg = len(segs)

    constrained_parts, A_rows, b_rows = [], [], []
    for p in import_parts:
        row = [segments_data[s]["combined_req"].get(p,0) for s in segs]
        if any(v>0 for v in row):
            constrained_parts.append(p)
            A_rows.append(row)
            b_rows.append(float(stock.get(p,0)))

    A   = np.array(A_rows, dtype=float)
    b   = np.array(b_rows, dtype=float)
    c   = -np.ones(n_seg)
    res = linprog(c, A_ub=A, b_ub=b, bounds=[(0,None)]*n_seg, method="highs")

    if res.status not in (0, 1):
        st.error(f"LP did not converge: {res.message}")
        return None

    alloc_int  = np.floor(res.x).astype(int)
    total_sets = int(alloc_int.sum())

    part_usage = {}
    for p, row, avail in zip(constrained_parts, A_rows, b_rows):
        used = sum(row[j]*alloc_int[j] for j in range(n_seg))
        part_usage[p] = {"stock":avail,"used":used,
                         "remain":max(0,avail-used),
                         "pct":round(100*used/avail,1) if avail>0 else 0}

    with status:
        st.write("► Done.")
    status.update(label="Segment Capacity complete ✅", state="complete", expanded=False)

    return dict(segs=segs, alloc_int=alloc_int, total_sets=total_sets,
                segments_data=segments_data, import_parts=import_parts,
                constrained_parts=constrained_parts, part_usage=part_usage,
                stock=stock, skipped_segs=skipped_segs)


def display_segment_results(r):
    segs          = r["segs"]
    alloc_int     = r["alloc_int"]
    total_sets    = r["total_sets"]
    segments_data = r["segments_data"]
    part_usage    = r["part_usage"]
    stock         = r["stock"]
    import_parts  = r["import_parts"]

    st.divider()
    m1,m2,m3 = st.columns(3)
    m1.metric("Total sets (all segments)", f"{total_sets:,}")
    m2.metric("Segments with production",  f"{(alloc_int>0).sum()} / {len(segs)}")
    m3.metric("Import parts constrained",  f"{len(r['constrained_parts'])}")

    if r["skipped_segs"]:
        with st.expander(f"⚠️ {len(r['skipped_segs'])} segments skipped"):
            for s in r["skipped_segs"]:
                st.text(f"  • {s}")

    # ── Sets per segment table ─────────────────────────────────
    st.subheader("📊 Sets producible per segment")
    rows = []
    for s, qty in zip(segs, alloc_int):
        sd = segments_data[s]
        limiting_part, min_ratio = "—", float("inf")
        for p, req in sd["combined_req"].items():
            if req > 0:
                ratio = float(stock.get(p,0)) / req
                if ratio < min_ratio:
                    min_ratio, limiting_part = ratio, p
        rows.append({
            "Segment":           s,
            "Max Sets":          qty,
            "IDU models":        sd["idu_count"],
            "ODU models":        sd["odu_count"],
            "Import parts used": len(sd["combined_req"]),
            "Limiting part":     limiting_part,
            "Limiting stock":    int(stock.get(limiting_part,0)) if limiting_part!="—" else 0,
        })

    results_df = pd.DataFrame(rows).sort_values("Max Sets", ascending=False)

    def hl_sets(row):
        if row["Max Sets"] > 0:
            return ["background-color:#e8f8e8"]*len(row)
        return ["background-color:#f5f5f5;color:#999"]*len(row)

    st.dataframe(
        results_df.style.apply(hl_sets, axis=1)
                  .format({"Max Sets":"{:,}","Limiting stock":"{:,}"}),
        use_container_width=True, hide_index=True)

    # ── Import part utilisation ────────────────────────────────
    st.subheader("🔩 Import part stock utilisation")
    pu_rows = []
    for p in import_parts:
        pu = part_usage.get(p, {})
        pu_rows.append({
            "Import Part":  p,
            "Stock":        int(stock.get(p,0)),
            "Used":         int(pu.get("used",0)),
            "Remaining":    int(pu.get("remain", stock.get(p,0))),
            "Utilisation%": pu.get("pct",0),
        })
    pu_df = pd.DataFrame(pu_rows).sort_values("Utilisation%", ascending=False)

    def hl_util(row):
        pct = row["Utilisation%"]
        if pct >= 90: return ["background-color:#ffe0e0"]*len(row)
        elif pct >= 50: return ["background-color:#fff3cd"]*len(row)
        elif pct > 0: return ["background-color:#e8f8e8"]*len(row)
        return [""]*len(row)

    tab_all, tab_const = st.tabs(["All import parts","Constrained only"])
    with tab_all:
        st.dataframe(
            pu_df.style.apply(hl_util, axis=1)
                 .format({"Stock":"{:,}","Used":"{:,}","Remaining":"{:,}","Utilisation%":"{:.1f}"}),
            use_container_width=True, hide_index=True)
    with tab_const:
        constrained = pu_df[pu_df["Import Part"].isin(r["constrained_parts"])]
        st.dataframe(
            constrained.style.apply(hl_util, axis=1)
                       .format({"Stock":"{:,}","Used":"{:,}","Remaining":"{:,}","Utilisation%":"{:.1f}"}),
            use_container_width=True, hide_index=True)

    # ── Segment detail ─────────────────────────────────────────
    st.subheader("🔍 Segment detail")
    selected = st.selectbox(
        "Select segment", options=sorted(segs, key=lambda s:-alloc_int[segs.index(s)]),
        key="seg_detail_select")
    if selected:
        sd   = segments_data[selected]
        sets = alloc_int[segs.index(selected)]
        st.markdown(f"**{selected}** — `{sets:,}` sets")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**IDU models:**")
            for code in sd["idu_codes"]: st.text(f"  {code}")
        with c2:
            st.markdown("**ODU models:**")
            for code in sd["odu_codes"]: st.text(f"  {code}")
        st.markdown("**Import part requirements per set:**")
        imp_rows = []
        for p, req in sorted(sd["combined_req"].items(), key=lambda x:-x[1]):
            avail = float(stock.get(p,0))
            max_s = int(avail/req) if req>0 else 0
            imp_rows.append({"Part":p,"Qty per set":round(req,4),
                             "Stock available":int(avail),"Max sets (alone)":max_s})
        imp_df = pd.DataFrame(imp_rows)
        def hl_imp(row):
            if row["Max sets (alone)"] == sets and sets > 0:
                return ["background-color:#ffe0e0"]*len(row)
            return [""]*len(row)
        st.dataframe(
            imp_df.style.apply(hl_imp, axis=1)
                  .format({"Qty per set":"{:.4f}","Stock available":"{:,}","Max sets (alone)":"{:,}"}),
            use_container_width=True, hide_index=True)
        st.caption("🔴 Highlighted = binding constraint for this segment")

    # ── Excel download ─────────────────────────────────────────
    st.divider()
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        results_df.to_excel(writer, sheet_name="Sets per Segment", index=False)
        pu_df.to_excel(writer, sheet_name="Import Part Utilisation", index=False)
    buf.seek(0)
    st.download_button(
        "⬇️ Download Segment Capacity (.xlsx)", data=buf,
        file_name="segment_production_capacity.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True, type="primary")


# ═══════════════════════════════════════════════════════════════
# MAIN MRP FUNCTION
# ═══════════════════════════════════════════════════════════════
def run_mrp(bom_file, req_file, prod_file, receipt_file):
    logs   = []
    log    = lambda msg: logs.append(msg)
    status = st.status("Running MRP engine ...", expanded=True)

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
    missing_bom = [c for c in ["BOM Header","Level","Component","Required Qty"] if c not in bom.columns]
    if missing_bom:
        st.error(f"Missing required BOM columns: {missing_bom}")
        return None
    bom = bom[[c for c in keep if c in bom.columns]].copy()
    for col,default in [("Alt","0"),("Special procurement",""),("Procurement type",""),("Component descriptio","")]:
        if col not in bom.columns:
            bom[col] = default
    bom["Component"]            = bom["Component"].astype(str).str.strip()
    bom["BOM Header"]           = bom["BOM Header"].astype(str).str.strip()
    bom["Special procurement"]  = bom["Special procurement"].astype(str).str.strip()
    bom["Procurement type"]     = bom["Procurement type"].astype(str).str.strip()
    bom["Component descriptio"] = bom["Component descriptio"].astype(str).str.strip()
    bom["Required Qty"]         = pd.to_numeric(bom["Required Qty"], errors="coerce").fillna(0)
    bom["Alt"] = pd.to_numeric(bom["Alt"], errors="coerce").fillna(0).astype(int).astype(str)
    log(f"BOM rows: {len(bom):,}  |  Unique headers: {bom['BOM Header'].nunique()}")

    with status:
        st.write("► Loading Requirement and Stock ...")
    req_header_row = detect_requirement_header_row(req_file, sheet_name="Requirement")
    req_file.seek(0)
    req = pd.read_excel(req_file, sheet_name="Requirement", header=None)
    raw_headers = req.iloc[req_header_row].tolist()
    req.columns = [standardize_req_header(x) for x in raw_headers]
    req = req.iloc[req_header_row+1:].reset_index(drop=True)
    req = req.loc[:, [str(c).strip()!="" for c in req.columns]]
    req = req.loc[:, ~pd.Index(req.columns).duplicated(keep="first")]
    missing_req = [c for c in ["BOM Header","Alt"] if c not in req.columns]
    if missing_req:
        st.error(f"Missing Requirement columns: {missing_req}. Found: {req.columns.tolist()}")
        return None
    req["BOM Header"] = req["BOM Header"].astype(str).str.strip()
    req["Alt"] = pd.to_numeric(req["Alt"], errors="coerce").fillna(0).astype(int).astype(str)
    NON_MONTH_COLS = {"BOM Header","Alt"}
    parsed = parse_all_month_cols(req.columns.tolist(), NON_MONTH_COLS)
    if not parsed:
        st.error(f"No date/month columns detected. Found: {req.columns.tolist()}")
        return None
    rename_map = {p["orig"]: p["label"] for p in parsed if p["orig"] != p["label"]}
    if rename_map:
        req = req.rename(columns=rename_map)
    months      = [p["label"] for p in parsed]
    MONTH_ORDER = {m:i for i,m in enumerate(months)}
    for m in months:
        col_data = safe_series(req, m)
        req[m] = pd.to_numeric(
            col_data.astype(str).str.replace(",","",regex=False).str.strip(),
            errors="coerce").fillna(0)
    log(f"Months detected ({len(months)}): {months}")
    req_file.seek(0)
    stock_raw = pd.read_excel(req_file, sheet_name="Stock",
                              usecols=[0,1], header=0, names=["Component","Stock_Qty"])
    stock_raw = stock_raw.dropna(subset=["Component"]).copy()
    stock_raw["Component"] = stock_raw["Component"].astype(str).str.strip()
    stock_raw["Stock_Qty"] = pd.to_numeric(
        stock_raw["Stock_Qty"].astype(str).str.replace(",","",regex=False).str.strip(),
        errors="coerce").fillna(0)
    stock = stock_raw.groupby("Component")["Stock_Qty"].sum()
    receipt_qty = load_receipt_qty(receipt_file)
    receipt_added = 0
    if not receipt_qty.empty:
        for comp, qty in receipt_qty.items():
            stock[comp] = float(stock.get(comp,0)) + float(qty)
            receipt_added += 1
        log(f"Receipt quantities added for {receipt_added} components")
    req_long = req.melt(id_vars=["BOM Header","Alt"], value_vars=months,
                        var_name="Month", value_name="FG_Demand")
    req_long = req_long[req_long["FG_Demand"]>0].copy()
    log(f"Requirement rows (non-zero): {len(req_long):,}")
    log(f"Stock components: {len(stock):,}")

    with status:
        st.write("► Loading Production Orders ...")
    prod_summary = empty_prod_summary()
    if prod_file is not None:
        try:
            coois = pd.read_excel(prod_file)
            coois.columns = coois.columns.str.strip()
            if not coois.empty:
                status_col = next((c for c in coois.columns if "status" in c.lower()), None)
                mat_col    = next((c for c in coois.columns if "material" in c.lower() and "description" not in c.lower()), None)
                ord_col    = next((c for c in coois.columns if "order" in c.lower() and ("qty" in c.lower() or "quantity" in c.lower())), None)
                del_col    = next((c for c in coois.columns if "deliver" in c.lower() or ("quantity" in c.lower() and "gr" in c.lower())), None)
                conf_col   = next((c for c in coois.columns if "confirm" in c.lower() and "quantity" in c.lower()), None)
                if all([status_col, mat_col, ord_col, del_col, conf_col]):
                    coois = coois[~coois[status_col].astype(str).str.contains("TECO", case=False, na=False)].copy()
                    coois[mat_col]  = coois[mat_col].astype(str).str.strip()
                    coois[ord_col]  = pd.to_numeric(coois[ord_col],  errors="coerce").fillna(0)
                    coois[del_col]  = pd.to_numeric(coois[del_col],  errors="coerce").fillna(0)
                    coois[conf_col] = pd.to_numeric(coois[conf_col], errors="coerce").fillna(0)
                    coois["Open_Qty"] = (coois[ord_col]-coois[del_col]).clip(lower=0)
                    prod_summary = (coois.groupby(mat_col, as_index=False)
                                        .agg(Confirmed_Qty=(conf_col,"sum"), Open_Production_Qty=("Open_Qty","sum"))
                                        .rename(columns={mat_col:"Component"}))
                    prod_summary["Component"] = prod_summary["Component"].astype(str).str.strip()
                    log(f"Production order rows (non-TECO): {len(coois):,}")
        except Exception as e:
            log(f"Production order error ({e}) — defaulting to 0.")

    def get_sfrac(rows, comp_col, gross_col):
        agg = rows.groupby([comp_col,"Month","Month_Order"], as_index=False)[gross_col].sum()
        sfrac = {}
        for comp, grp in agg.groupby(comp_col):
            avail = float(stock.get(comp,0))
            for _, row in grp.sort_values("Month_Order").iterrows():
                g = float(row[gross_col])
                sfrac[(comp,row["Month"])] = max(0.0,g-avail)/g if g>0 else 0.0
                avail = max(0.0, avail-g)
        return sfrac

    def make_report(gross_agg_df, comp_col):
        BASE = ["Component","Description","Month","Gross_Requirement","Stock_Used","Shortage","Stock_Remaining"]
        if gross_agg_df.empty:
            return pd.DataFrame(columns=BASE)
        results = []
        for comp, grp in gross_agg_df.groupby(comp_col):
            avail = float(stock.get(comp,0))
            desc  = grp["Desc"].iloc[0]
            for _, row in grp.sort_values("Month_Order").iterrows():
                gr       = float(row["Gross"])
                consumed = min(avail, gr)
                shortage = max(0.0, gr-avail)
                avail    = max(0.0, avail-gr)
                results.append({"Component":comp,"Description":desc,"Month":row["Month"],
                                 "Gross_Requirement":gr,"Stock_Used":consumed,
                                 "Shortage":shortage,"Stock_Remaining":avail})
        return pd.DataFrame(results, columns=BASE)

    def apply_sfrac(df, gross_col, ph_col, sfrac_dict, comp_col):
        return df.apply(
            lambda r: r[gross_col] if is_phantom(r[ph_col])
                      else r[gross_col]*sfrac_dict.get((r[comp_col],r["Month"]),1.0), axis=1)

    with status:
        st.write("► Running MRP explosion ...")

    bom_l1 = (bom[bom["Level"]==1][["BOM Header","Alt","Component","Component descriptio","Required Qty","Special procurement"]].copy()
              .rename(columns={"Component":"L1_Comp","Component descriptio":"L1_Desc","Required Qty":"L1_Qty","Special procurement":"L1_Ph"}))
    l1 = req_long.merge(bom_l1, on=["BOM Header","Alt"], how="inner")
    l1["L1_Gross"] = l1["FG_Demand"]*l1["L1_Qty"]
    l1["Month_Order"] = l1["Month"].map(MONTH_ORDER)
    l1_norm = l1[~l1["L1_Ph"].apply(is_phantom)].copy()
    l1_sfrac = get_sfrac(l1_norm,"L1_Comp","L1_Gross")
    l1["L1_Eff"] = apply_sfrac(l1,"L1_Gross","L1_Ph",l1_sfrac,"L1_Comp")
    l1_agg = (l1_norm.groupby(["L1_Comp","L1_Desc","Month","Month_Order"],as_index=False)["L1_Gross"].sum()
              .rename(columns={"L1_Comp":"Component","L1_Desc":"Desc","L1_Gross":"Gross"}))
    result_l1 = make_report(l1_agg,"Component")

    bom_l2 = (bom[bom["Level"]==2][["BOM Header","Alt","Parent","Component","Component descriptio","Required Qty","Special procurement"]].copy()
              .rename(columns={"Parent":"L1_Comp","Component":"L2_Comp","Component descriptio":"L2_Desc","Required Qty":"L2_Qty","Special procurement":"L2_Ph"}))
    l2 = l1.merge(bom_l2,on=["BOM Header","Alt","L1_Comp"],how="inner")
    l2["L2_Gross"] = l2["L1_Eff"]*l2["L2_Qty"]
    l2_norm = l2[~l2["L2_Ph"].apply(is_phantom)].copy()
    l2_sfrac = get_sfrac(l2_norm,"L2_Comp","L2_Gross")
    l2["L2_Eff"] = apply_sfrac(l2,"L2_Gross","L2_Ph",l2_sfrac,"L2_Comp")
    l2_agg = (l2_norm.groupby(["L2_Comp","L2_Desc","Month","Month_Order"],as_index=False)["L2_Gross"].sum()
              .rename(columns={"L2_Comp":"Component","L2_Desc":"Desc","L2_Gross":"Gross"}))
    result_l2 = make_report(l2_agg,"Component")

    bom_l3 = (bom[bom["Level"]==3][["BOM Header","Alt","Parent","Component","Component descriptio","Required Qty","Special procurement"]].copy()
              .rename(columns={"Parent":"L2_Comp","Component":"L3_Comp","Component descriptio":"L3_Desc","Required Qty":"L3_Qty","Special procurement":"L3_Ph"}))
    l3 = l2.merge(bom_l3,on=["BOM Header","Alt","L2_Comp"],how="inner")
    l3["L3_Gross"] = l3.apply(lambda r: r["L2_Eff"] if is_phantom(r["L3_Ph"]) else r["L2_Eff"]*r["L3_Qty"],axis=1)
    l3_norm = l3[~l3["L3_Ph"].apply(is_phantom)].copy()
    l3_sfrac = get_sfrac(l3_norm,"L3_Comp","L3_Gross")
    l3["L3_Eff"] = apply_sfrac(l3,"L3_Gross","L3_Ph",l3_sfrac,"L3_Comp")
    l3_agg = (l3_norm.groupby(["L3_Comp","L3_Desc","Month","Month_Order"],as_index=False)["L3_Gross"].sum()
              .rename(columns={"L3_Comp":"Component","L3_Desc":"Desc","L3_Gross":"Gross"}))
    result_l3 = make_report(l3_agg,"Component")

    bom_l4 = (bom[bom["Level"]==4][["BOM Header","Alt","Parent","Component","Component descriptio","Required Qty","Special procurement"]].copy()
              .rename(columns={"Parent":"L3_Comp","Component":"L4_Comp","Component descriptio":"L4_Desc","Required Qty":"L4_Qty","Special procurement":"L4_Ph"}))
    l4 = l3.merge(bom_l4,on=["BOM Header","Alt","L3_Comp"],how="inner")
    l4["L4_Gross"] = l4["L3_Eff"]*l4["L4_Qty"]
    l4_agg = (l4.groupby(["L4_Comp","L4_Desc","Month","Month_Order"],as_index=False)["L4_Gross"].sum()
              .rename(columns={"L4_Comp":"Component","L4_Desc":"Desc","L4_Gross":"Gross"}))
    result_l4 = make_report(l4_agg,"Component")

    with status:
        st.write("► Building output ...")
    status.update(label="MRP complete ✅", state="complete", expanded=False)

    # ── Summary ────────────────────────────────────────────────
    st.divider()
    st.subheader("📊 Summary")
    if not receipt_qty.empty:
        st.info(f"ℹ️ Receipt: {receipt_added} components had stock increased before MRP.")
    c1,c2,c3,c4 = st.columns(4)
    for col_ui,lbl,df in zip([c1,c2,c3,c4],["L1","L2","L3","L4"],
                              [result_l1,result_l2,result_l3,result_l4]):
        short = df[df["Shortage"]>0]["Component"].nunique() if not df.empty else 0
        with col_ui:
            st.metric(f"L{lbl[-1]} components", df["Component"].nunique() if not df.empty else 0)
            st.metric("With shortage", short)

    # ── Verification ───────────────────────────────────────────
    st.divider()
    st.subheader("🔍 Verification")
    tab1,tab2,tab3,tab4 = st.tabs(["L1","L2","L3 (phantom)","L4"])
    def show_verify(tab, result_df, target, level_label):
        with tab:
            st.markdown(f"**{level_label} — `{target}`**")
            if result_df is None or result_df.empty:
                st.warning("No rows at this level."); return
            t = result_df[result_df["Component"]==target]
            if t.empty:
                st.info("Not found — phantom or no demand."); return
            st.caption(f"Description: {t['Description'].iloc[0]} | "
                       f"Opening Stock: {stock.get(target,0):,.3f}")
            st.dataframe(t[["Month","Gross_Requirement","Stock_Used","Shortage","Stock_Remaining"]]
                          .reset_index(drop=True), use_container_width=True)
    show_verify(tab1, result_l1, VERIFY_L1, "LEVEL 1")
    show_verify(tab2, result_l2, VERIFY_L2, "LEVEL 2")
    with tab3:
        ph_found = (not result_l3.empty) and (VERIFY_L3 in result_l3["Component"].values)
        if ph_found:
            st.error(f"ERROR: {VERIFY_L3} found — phantom logic broken!")
        else:
            st.success(f"✅ {VERIFY_L3} correctly SKIPPED.")
        st.caption(f"Phantom rows passed through: {len(l3[l3['L3_Ph'].apply(is_phantom)]):,}")
    show_verify(tab4, result_l4, VERIFY_L4, "LEVEL 4")
    if months:
        l4_jan = l4[(l4["L4_Comp"]==VERIFY_L4)&(l4["Month"]==months[0])]
        if not l4_jan.empty:
            with tab4:
                bd = (l4_jan.groupby("BOM Header")["L4_Gross"].sum()
                      .sort_values(ascending=False).reset_index())
                bd.columns = ["BOM Header", f"Gross ({months[0]})"]
                total = bd[f"Gross ({months[0]})"].sum()
                stk2  = stock.get(VERIFY_L4, 0)
                st.caption(f"Total: {total:,.3f} | Stock: {stk2:,.3f} | Shortage: {max(0,total-stk2):,.3f}")
                st.dataframe(bd, use_container_width=True)

    # ── Export ─────────────────────────────────────────────────
    final_output = pd.concat([result_l1,result_l2,result_l3,result_l4], ignore_index=True)
    all_comps = final_output[["Component","Description"]].drop_duplicates(subset="Component").copy()

    # Pivot Gross_Requirement → cumsum → Net Position = Stock - cumulative demand
    pivot_gross = (final_output
                   .pivot_table(index=["Component","Description"], columns="Month",
                                values="Gross_Requirement", aggfunc="sum", fill_value=0)
                   .reset_index())
    pivot = all_comps.merge(pivot_gross, on=["Component","Description"], how="left").fillna(0)
    month_cols = [m for m in months if m in pivot.columns]
    if month_cols:
        pivot[month_cols] = pivot[month_cols].cumsum(axis=1)

    bom_master = bom[["Component","Procurement type","Special procurement"]].drop_duplicates(subset="Component")
    stock_df   = stock.reset_index().rename(columns={"Stock_Qty":"Stock"})
    pivot = (pivot.merge(bom_master, on="Component", how="left")
                  .merge(stock_df,   on="Component", how="left")
                  .merge(prod_summary, on="Component", how="left"))

    pivot["Procurement type"]    = pivot["Procurement type"].fillna("")
    pivot["Special procurement"] = pivot["Special procurement"].fillna("")
    pivot["Stock"]               = pivot["Stock"].fillna(0)
    pivot["Confirmed_Qty"]       = pivot["Confirmed_Qty"].fillna(0)
    pivot["Open_Production_Qty"] = pivot["Open_Production_Qty"].fillna(0)

    # Net Position = Stock - cumulative gross demand per month
    for m in month_cols:
        pivot[m] = pivot["Stock"] - pivot[m]

    if not receipt_qty.empty:
        rq_df = receipt_qty.reset_index(); rq_df.columns = ["Component","Receipt_Qty"]
        pivot = pivot.merge(rq_df, on="Component", how="left")
        pivot["Receipt_Qty"] = pivot["Receipt_Qty"].fillna(0)
        extra_cols = ["Receipt_Qty"]
    else:
        extra_cols = []

    pivot = pivot.rename(columns={"Description":"Component descri"})
    final_cols = (["Component","Component descri","Procurement type","Special procurement",
                   "Confirmed_Qty","Open_Production_Qty","Stock"] + extra_cols + month_cols)
    for c in final_cols:
        if c not in pivot.columns:
            pivot[c] = 0 if c in month_cols+["Confirmed_Qty","Open_Production_Qty","Stock","Receipt_Qty"] else ""
    pivot = pivot[final_cols].sort_values("Component").reset_index(drop=True)

    st.divider()
    st.subheader("📋 Output preview")
    st.dataframe(pivot.head(200), use_container_width=True)
    st.caption(f"{len(pivot):,} rows · {len(month_cols)} month columns · "
               f"positive = surplus stock · negative = shortage")

    buf = io.BytesIO()
    pivot.to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    st.download_button("⬇️ Download mrp_final.xlsx", data=buf, file_name="mrp_final.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       use_container_width=True, type="primary")

    with st.expander("Run log"):
        for line in logs: st.text(line)

    return dict(bom=bom, req=req, months=months, stock=stock,
                prod_summary=prod_summary,
                result_l1=result_l1, result_l2=result_l2,
                result_l3=result_l3, result_l4=result_l4)


# ═══════════════════════════════════════════════════════════════
# SESSION STATE + ENTRY POINT
# ═══════════════════════════════════════════════════════════════
if "mrp_results"  not in st.session_state: st.session_state["mrp_results"]  = None
if "seg_results"  not in st.session_state: st.session_state["seg_results"]  = None

# ── MRP run ───────────────────────────────────────────────────
if not run_mrp_btn and bom_file is None:
    st.info("Upload your files in the sidebar, then click **▶ Run MRP**.")
elif run_mrp_btn:
    if bom_file is None or req_file is None:
        st.warning("Please upload at least the BOM file and the Req & Stock file.")
    else:
        try:
            results = run_mrp(bom_file, req_file, prod_file, receipt_file)
            if results is not None:
                st.session_state["mrp_results"] = results
        except Exception as e:
            st.exception(e)

# Show MRP search if results available
if st.session_state["mrp_results"] is not None:
    r = st.session_state["mrp_results"]
    try:
        show_search_section(bom=r["bom"], req_df=r["req"], months=r["months"],
                            stock=r["stock"], prod_summary=r["prod_summary"])
    except Exception as e:
        st.error(f"Search error: {e}")

# ── Segment Capacity run ───────────────────────────────────────
if run_seg_btn:
    if seg_imp_file is None:
        st.warning("Please upload the Segment & Import Part file to run Segment Capacity.")
    elif st.session_state["mrp_results"] is None:
        st.warning("Please run MRP first — Segment Capacity uses the same BOM and Stock data.")
    else:
        try:
            r = st.session_state["mrp_results"]
            seg_result = run_segment_capacity(r["bom"], r["stock"], seg_imp_file)
            if seg_result is not None:
                st.session_state["seg_results"] = seg_result
        except Exception as e:
            st.exception(e)

# Show segment results if available
if st.session_state["seg_results"] is not None:
    st.divider()
    st.header("🏭 Segment Production Capacity")
    st.caption("IDU + ODU sets per segment · LP-optimised across all segments · import parts as constraint")
    try:
        display_segment_results(st.session_state["seg_results"])
    except Exception as e:
        st.error(f"Segment display error: {e}")
