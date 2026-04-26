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
    orig_label = str(col).strip() if not isinstance(col, pd.Timestamp) else col.strftime("%d-%b-%y")
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
            yr = int(yr_s) + (2000 if yr_s and len(yr_s)==2 else 0) if yr_s else default_year
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
            yr = int(yr_s) + (2000 if len(yr_s)==2 else 0)
            ts = pd.Timestamp(year=yr, month=mon_num, day=1)
            return ts, s
    try:
        ts = pd.to_datetime(s, dayfirst=True, errors="raise")
        return ts, s
    except Exception:
        pass
    return None, None

def infer_year_from_parsed(parsed_list):
    years = []
    for p in parsed_list:
        if p["ts"] is not None:
            years.append(p["ts"].year)
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
        key = p["ts"]
        if key not in seen_ts:
            seen_ts.add(key)
            unique.append(p)
    return unique

def standardize_req_header(v):
    if pd.isna(v): return ""
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
    if receipt_file is None:
        return pd.Series(dtype=float)
    try:
        df = pd.read_excel(receipt_file)
        df.columns = df.columns.str.strip()
        mat_keywords = ["material","component","part number","part","mat"]
        qty_keywords = ["gr qty","gr quantity","receipt qty","receipt quantity",
                        "received qty","quantity","qty"]
        mat_col = next((c for c in df.columns if any(k in c.lower() for k in mat_keywords)), df.columns[0])
        qty_col = next((c for c in df.columns if any(k in c.lower() for k in qty_keywords) and c != mat_col), None)
        if qty_col is None:
            st.warning("Receipt file: could not detect a quantity column — skipped.")
            return pd.Series(dtype=float)
        df[mat_col] = df[mat_col].astype(str).str.strip()
        df[qty_col] = pd.to_numeric(df[qty_col].astype(str).str.replace(",","",regex=False).str.strip(), errors="coerce").fillna(0)
        result = df.groupby(mat_col)[qty_col].sum()
        st.sidebar.success(f"Receipt file loaded: {len(result):,} components")
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
        path_comps, path_descs, path_qtys, path_sp = [row["Component"]], [row["Component descriptio"]], [float(row["Required Qty"])], [str(row["Special procurement"]).strip()]
        current, fg, alt = row["Parent"], row["BOM Header"], row["Alt"]
        for _ in range(4):
            if current == fg: break
            pr_rows = bom[(bom["BOM Header"] == fg) & (bom["Alt"] == alt) & (bom["Component"] == current)]
            if pr_rows.empty: break
            pr = pr_rows.iloc[0]
            path_comps.insert(0, pr["Component"]); path_descs.insert(0, pr["Component descriptio"])
            path_qtys.insert(0, float(pr["Required Qty"])); path_sp.insert(0, str(pr["Special procurement"]).strip())
            current = pr["Parent"]
        paths.append({"fg": fg, "alt": str(alt), "level": int(row["Level"]), "path_comps": path_comps, "path_descs": path_descs, "path_qtys": path_qtys, "path_sp": path_sp})
    return paths

def build_dot_tree(component, paths, req_df, months, stock, prod_summary):
    fg_demand = {}
    for p in paths:
        rows = req_df[(req_df["BOM Header"]==p["fg"]) & (req_df["Alt"]==p["alt"])]
        total = rows[months].sum(numeric_only=True).sum() if not rows.empty else 0
        fg_demand[(p["fg"], p["alt"])] = float(total)
    result_dfs = st.session_state.get("mrp_results", {})
    gross_map, shortage_map = {}, {}
    for key in ["result_l1","result_l2","result_l3","result_l4"]:
        df = result_dfs.get(key)
        if df is None or df.empty: continue
        agg = df.groupby("Component")[["Gross_Requirement","Shortage"]].sum()
        for comp, row in agg.iterrows():
            gross_map[comp] = gross_map.get(comp, 0) + row["Gross_Requirement"]
            shortage_map[comp] = shortage_map.get(comp, 0) + row["Shortage"]
    def trunc(s, n=20): return (str(s)[:n]+"…") if len(str(s))>n else str(s)
    node_attrs, edges, seen_edges = {}, [], set()
    for path in paths:
        fg, alt = path["fg"], path["alt"]
        demand = fg_demand.get((fg, alt), 0)
        fg_id = f"FG_{fg}_A{alt}".replace("-","_").replace(".","_")
        node_attrs[fg_id] = f'label="FG: {fg}\\nAlt: {alt}\\nTotal demand: {demand:,.0f}" shape=box style="filled,rounded" fillcolor="#2e86c1" fontcolor=white fontsize=11'
        prev_id = fg_id
        for comp, desc, qty, sp in zip(path["path_comps"], path["path_descs"], path["path_qtys"], path["path_sp"]):
            is_tgt, is_ph = (comp == component), (sp == PHANTOM)
            nid = f"N_{comp}_FG_{fg}_A{alt}".replace("-","_").replace(".","_").replace("+","p")
            gross, shortage, stk = gross_map.get(comp, 0), shortage_map.get(comp, 0), float(stock.get(comp, 0))
            if is_tgt:
                p_row = prod_summary[prod_summary["Component"]==comp]
                conf = float(p_row["Confirmed_Qty"].iloc[0]) if not p_row.empty else 0
                oprod = float(p_row["Open_Production_Qty"].iloc[0]) if not p_row.empty else 0
                label = f"{trunc(comp)}\\n{trunc(desc)}\\nStock: {stk:,.0f} | Conf: {conf:,.0f}\\nOpen PO: {oprod:,.0f}\\nGross: {gross:,.0f} | Net: {shortage:,.0f}"
                node_attrs[nid] = f'label="{label}" shape=box style="filled,rounded" fillcolor="#1e8449" fontcolor=white fontsize=11 penwidth=2.5'
            elif is_ph:
                node_attrs[nid] = f'label="PHANTOM\\n{trunc(comp)}\\n{trunc(desc)}\\nqty={qty:g}" shape=box style="filled,dashed" fillcolor="#f39c12" fontcolor="#333" fontsize=10'
            else:
                label = f"{trunc(comp)}\\n{trunc(desc)}\\nQty: {qty:g} | Stock: {stk:,.0f}\\nGross: {gross:,.0f} | Net: {shortage:,.0f}"
                node_attrs[nid] = f'label="{label}" shape=box style="filled,rounded" fillcolor="#f9e79f" fontcolor="#333" fontsize=10'
            if (prev_id, nid) not in seen_edges:
                edges.append((prev_id, nid, f"×{qty:g}")); seen_edges.add((prev_id, nid))
            prev_id = nid
    lines = ["digraph MRP {", "rankdir=TB;", 'node [fontname="Arial"];', 'edge [fontname="Arial" fontsize=10];', "graph [splines=ortho nodesep=0.6 ranksep=0.8];"]
    for nid, attrs in node_attrs.items(): lines.append(f'  "{nid}" [{attrs}];')
    for src, dst, lbl in edges: lines.append(f'  "{src}" -> "{dst}" [label="{lbl}"];')
    lines.append("}")
    return "\n".join(lines)

def show_search_section(bom, req_df, months, stock, prod_summary):
    st.divider()
    st.subheader("🔍 Component Search")
    scol, _ = st.columns([2,3])
    with scol: comp = st.text_input("Component code", placeholder="e.g. 0010748458", label_visibility="collapsed").strip()
    if not comp: return
    r = st.session_state.get("mrp_results", {})
    result_dfs = {k: r.get(k) for k in ["result_l1","result_l2","result_l3","result_l4"]}
    found_in = {lbl: df[df["Component"]==comp].copy() for lbl, df in result_dfs.items() if df is not None and not df.empty and comp in df["Component"].values}
    bom_in = bom[bom["Component"]==comp]
    if bom_in.empty and not found_in:
        st.warning(f"`{comp}` not found."); return
    desc = bom_in["Component descriptio"].iloc[0] if not bom_in.empty else "—"
    stk = float(stock.get(comp, 0))
    p_row = prod_summary[prod_summary["Component"]==comp]
    conf_qty = float(p_row["Confirmed_Qty"].iloc[0]) if not p_row.empty else 0
    open_qty = float(p_row["Open_Production_Qty"].iloc[0]) if not p_row.empty else 0
    st.markdown(f"### `{comp}` — {desc}")
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Stock on hand", f"{stk:,.3f}")
    c2.metric("Confirmed prod qty", f"{conf_qty:,.0f}")
    c3.metric("Open production qty", f"{open_qty:,.0f}")
    c4.metric("Procurement type", bom_in["Procurement type"].iloc[0] if not bom_in.empty else "—")
    c5.metric("Sp. procurement", bom_in["Special procurement"].iloc[0] if not bom_in.empty else "—")
    if found_in:
        all_rows = pd.concat(found_in.values(), ignore_index=True)
        mo = {m:i for i,m in enumerate(months)}
        monthly = all_rows.groupby("Month", as_index=False).agg(Gross_Requirement=("Gross_Requirement","sum"), Stock_Used=("Stock_Used","sum"), Shortage=("Shortage","sum"), Stock_Remaining=("Stock_Remaining","last"))
        monthly["_ord"] = monthly["Month"].map(mo)
        monthly = monthly.sort_values("_ord").drop(columns="_ord")
        monthly["Cumulative Shortage"] = monthly["Shortage"].cumsum()
        def hl(row):
            # Highlight if shortage is negative
            c = "background-color:#ffe0e0" if row["Shortage"] < 0 else ""
            return [c]*len(row)
        st.dataframe(monthly.style.apply(hl, axis=1).format({"Gross_Requirement":"{:,.2f}","Stock_Used":"{:,.2f}","Shortage":"{:,.2f}","Stock_Remaining":"{:,.2f}","Cumulative Shortage":"{:,.2f}"}), use_container_width=True, hide_index=True)
    paths = get_ancestry_paths(comp, bom)
    if paths:
        dot = build_dot_tree(comp, paths[:12], req_df, months, stock, prod_summary)
        st.graphviz_chart(dot, use_container_width=True)

# ═══════════════════════════════════════════════════════════════
# MAIN MRP FUNCTION
# ═══════════════════════════════════════════════════════════════
def run_mrp(bom_file, req_file, prod_file, receipt_file):
    logs = []
    log = lambda msg: logs.append(msg)
    status = st.status("Running MRP engine ...", expanded=True)

    with status: st.write("► Building clean BOM ...")
    bom = pd.read_excel(bom_file)
    bom.columns = bom.columns.str.strip()
    if "Alt." in bom.columns: bom = bom.rename(columns={"Alt.":"Alt"})
    bom["Level"] = pd.to_numeric(bom["Level"], errors="coerce").fillna(0).astype(int)
    bom = bom.reset_index(drop=True)
    parents, stack = [], {}
    for i in range(len(bom)):
        lvl = bom.loc[i,"Level"]
        parent = bom.loc[i,"BOM Header"] if lvl==1 else stack.get(lvl-1)
        stack = {k:v for k,v in stack.items() if k<=lvl}; stack[lvl] = bom.loc[i,"Component"]
        parents.append(parent)
    bom["Parent"] = parents
    drop_cols = ["Plant","Usage","Quantity","Unit","BOM L/T","BOM code","Item","Mat. Group","Mat. Group Desc.","Pur. Group","Pur. Group Desc.","MRP Controller","MRP Controller Desc."]
    bom = bom.drop(columns=[c for c in drop_cols if c in bom.columns], errors="ignore")
    for old,new in [("Component description","Component descriptio"), ("BOM header description","BOM header descripti")]:
        if old in bom.columns: bom = bom.rename(columns={old:new})
    keep = ["BOM Header","BOM header descripti","Alt","Level","Path","Parent","Component","Component descriptio","Required Qty","Base unit","Procurement type","Special procurement"]
    bom = bom[[c for c in keep if c in bom.columns]].copy()
    for col,default in [("Alt","0"),("Special procurement",""),("Procurement type",""),("Component descriptio","")]:
        if col not in bom.columns: bom[col] = default
    bom["Component"] = bom["Component"].astype(str).str.strip()
    bom["BOM Header"] = bom["BOM Header"].astype(str).str.strip()
    bom["Special procurement"] = bom["Special procurement"].astype(str).str.strip()
    bom["Procurement type"] = bom["Procurement type"].astype(str).str.strip()
    bom["Required Qty"] = pd.to_numeric(bom["Required Qty"], errors="coerce").fillna(0)
    bom["Alt"] = pd.to_numeric(bom["Alt"], errors="coerce").fillna(0).astype(int).astype(str)

    with status: st.write("► Loading Requirement and Stock ...")
    req_header_row = detect_requirement_header_row(req_file, sheet_name="Requirement")
    req_file.seek(0)
    req = pd.read_excel(req_file, sheet_name="Requirement", header=None)
    req.columns = [standardize_req_header(x) for x in req.iloc[req_header_row].tolist()]
    req = req.iloc[req_header_row+1:].reset_index(drop=True)
    req = req.loc[:, [str(c).strip()!="" for c in req.columns]]
    req = req.loc[:, ~pd.Index(req.columns).duplicated(keep="first")]
    req["BOM Header"] = req["BOM Header"].astype(str).str.strip()
    req["Alt"] = pd.to_numeric(req["Alt"], errors="coerce").fillna(0).astype(int).astype(str)
    parsed = parse_all_month_cols(req.columns.tolist(), {"BOM Header","Alt"})
    months = [p["label"] for p in parsed]
    for m in months: req[m] = pd.to_numeric(safe_series(req, m).astype(str).str.replace(",","",regex=False).str.strip(), errors="coerce").fillna(0)
    
    req_file.seek(0)
    stock_raw = pd.read_excel(req_file, sheet_name="Stock", usecols=[0,1], header=0, names=["Component","Stock_Qty"])
    stock_raw = stock_raw.dropna(subset=["Component"]).copy()
    stock_raw["Component"] = stock_raw["Component"].astype(str).str.strip()
    stock_raw["Stock_Qty"] = pd.to_numeric(stock_raw["Stock_Qty"].astype(str).str.replace(",","",regex=False).str.strip(), errors="coerce").fillna(0)
    stock = stock_raw.groupby("Component")["Stock_Qty"].sum()
    
    receipt_qty = load_receipt_qty(receipt_file)
    if not receipt_qty.empty:
        for comp, qty in receipt_qty.items():
            stock[comp] = float(stock.get(comp, 0)) + float(qty)

    prod_summary = empty_prod_summary()
    if prod_file:
        try:
            coois = pd.read_excel(prod_file); coois.columns = coois.columns.str.strip()
            # [Original Production Order Logic Preserved]
            status_col = next((c for c in coois.columns if "status" in c.lower()), None)
            mat_col    = next((c for c in coois.columns if "material" in c.lower() and "description" not in c.lower()), None)
            ord_col    = next((c for c in coois.columns if "order" in c.lower() and ("qty" in c.lower() or "quantity" in c.lower())), None)
            del_col    = next((c for c in coois.columns if "deliver" in c.lower() or ("quantity" in c.lower() and "gr" in c.lower())), None)
            conf_col   = next((c for c in coois.columns if "confirm" in c.lower() and "quantity" in c.lower()), None)
            if all([status_col, mat_col, ord_col, del_col, conf_col]):
                coois = coois[~coois[status_col].astype(str).str.contains("TECO", case=False, na=False)].copy()
                coois[mat_col] = coois[mat_col].astype(str).str.strip()
                coois["Open_Qty"] = (pd.to_numeric(coois[ord_col], errors="coerce").fillna(0) - pd.to_numeric(coois[del_col], errors="coerce").fillna(0)).clip(lower=0)
                prod_summary = coois.groupby(mat_col, as_index=False).agg(Confirmed_Qty=(conf_col,"sum"), Open_Production_Qty=("Open_Qty","sum")).rename(columns={mat_col:"Component"})
        except: pass

    # ─── CORE MRP HELPERS ───
    def get_sfrac(rows, comp_col, gross_col):
        agg = rows.groupby([comp_col,"Month","Month_Order"], as_index=False)[gross_col].sum()
        sfrac = {}
        for comp, grp in agg.groupby(comp_col):
            avail = float(stock.get(comp, 0))
            for _, row in grp.sort_values("Month_Order").iterrows():
                g = float(row[gross_col])
                sfrac[(comp, row["Month"])] = max(0.0, g-avail)/g if g>0 else 0.0
                avail = max(0.0, avail-g)
        return sfrac

    def make_report(gross_agg_df, comp_col):
        BASE = ["Component","Description","Month","Gross_Requirement","Stock_Used","Shortage","Stock_Remaining"]
        if gross_agg_df.empty: return pd.DataFrame(columns=BASE)
        results = []
        for comp, grp in gross_agg_df.groupby(comp_col):
            avail = float(stock.get(comp, 0))
            desc = grp["Desc"].iloc[0]
            for _, row in grp.sort_values("Month_Order").iterrows():
                gr = float(row["Gross"])
                consumed = min(avail, gr)
                
                # Logic: Shortage as negative, Excess as positive
                net_status = avail - gr
                shortage_val = net_status
                
                # Logic Integrity: Stock cannot be physically negative for next period calculation
                avail = max(0.0, net_status)
                
                results.append({"Component":comp,"Description":desc,"Month":row["Month"],"Gross_Requirement":gr,"Stock_Used":consumed,"Shortage":shortage_val,"Stock_Remaining":avail})
        return pd.DataFrame(results, columns=BASE)

    def apply_sfrac(df, gross_col, ph_col, sfrac_dict, comp_col):
        return df.apply(lambda r: r[gross_col] if is_phantom(r[ph_col]) else r[gross_col]*sfrac_dict.get((r[comp_col],r["Month"]),1.0), axis=1)

    # ─── MRP EXPLOSION L1 → L4 ───
    with status: st.write("► Running MRP explosion ...")
    req_long = req.melt(id_vars=["BOM Header","Alt"], value_vars=months, var_name="Month", value_name="FG_Demand")
    req_long["Month_Order"] = req_long["Month"].map({m:i for i,m in enumerate(months)})
    req_long = req_long[req_long["FG_Demand"]>0].copy()

    # LEVEL 1
    bom_l1 = bom[bom["Level"]==1].merge(req_long, on=["BOM Header","Alt"])
    bom_l1["Gross"] = bom_l1["Required Qty"] * bom_l1["FG_Demand"]
    res_l1 = make_report(bom_l1.rename(columns={"Component descriptio":"Desc"}), "Component")
    sf1 = get_sfrac(bom_l1, "Component", "Gross")

    # LEVEL 2
    bom_l2 = bom[bom["Level"]==2].merge(bom_l1[["BOM Header","Alt","Parent","Month","Month_Order","Gross","Special procurement"]].rename(columns={"Gross":"P_Gross","Parent":"Component","Special procurement":"P_SP"}), on=["BOM Header","Alt","Component"])
    bom_l2["Gross"] = bom_l2["Required Qty"] * apply_sfrac(bom_l2, "P_Gross", "P_SP", sf1, "Component")
    res_l2 = make_report(bom_l2.rename(columns={"Component descriptio":"Desc"}), "Component")
    sf2 = get_sfrac(bom_l2, "Component", "Gross")

    # LEVEL 3
    bom_l3 = bom[bom["Level"]==3].merge(bom_l2[["BOM Header","Alt","Parent","Month","Month_Order","Gross","Special procurement"]].rename(columns={"Gross":"P_Gross","Parent":"Component","Special procurement":"P_SP"}), on=["BOM Header","Alt","Component"])
    bom_l3["Gross"] = bom_l3["Required Qty"] * apply_sfrac(bom_l3, "P_Gross", "P_SP", sf2, "Component")
    res_l3 = make_report(bom_l3.rename(columns={"Component descriptio":"Desc"}), "Component")
    sf3 = get_sfrac(bom_l3, "Component", "Gross")

    # LEVEL 4
    bom_l4 = bom[bom["Level"]==4].merge(bom_l3[["BOM Header","Alt","Parent","Month","Month_Order","Gross","Special procurement"]].rename(columns={"Gross":"P_Gross","Parent":"Component","Special procurement":"P_SP"}), on=["BOM Header","Alt","Component"])
    bom_l4["Gross"] = bom_l4["Required Qty"] * apply_sfrac(bom_l4, "P_Gross", "P_SP", sf3, "Component")
    res_l4 = make_report(bom_l4.rename(columns={"Component descriptio":"Desc"}), "Component")

    status.update(label="MRP Complete!", state="complete", expanded=False)
    return {"bom": bom, "req": req, "months": months, "stock": stock, "prod": prod_summary, "result_l1": res_l1, "result_l2": res_l2, "result_l3": res_l3, "result_l4": res_l4}

# ═══════════════════════════════════════════════════════════════
# EXECUTION
# ═══════════════════════════════════════════════════════════════
if run_btn and bom_file and req_file:
    results = run_mrp(bom_file, req_file, prod_file, receipt_file)
    if results:
        st.session_state["mrp_results"] = results
        st.success("MRP engine finished.")

if "mrp_results" in st.session_state:
    r = st.session_state["mrp_results"]
    show_search_section(r["bom"], r["req"], r["months"], r["stock"], r["prod"])
    st.divider()
    st.subheader("📊 Level-wise Reports")
    tabs = st.tabs(["Level 1", "Level 2", "Level 3", "Level 4"])
    for i, t in enumerate(tabs, 1):
        with t:
            df = r[f"result_l{i}"]
            st.dataframe(df, use_container_width=True, hide_index=True)
