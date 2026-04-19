"""StmtForge - Interactive Streamlit Dashboard."""

import io
import os
import sys
import tempfile
from pathlib import Path

import pikepdf
import pdfplumber
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from uuid import uuid4

from stmtforge.database.db import Database
from stmtforge.hybrid_pipeline import HybridPipeline
from stmtforge.parsers.registry import list_available_parsers
from stmtforge.utils.config import load_config, get_all_passwords
from stmtforge.utils.hashing import file_hash as compute_file_hash
from stmtforge.utils.privacy_logging import PrivacyEventLogger, pseudonymize_value

# ── Page config ──────────────────────────────────────────────────
st.set_page_config(
    page_title="StmtForge - Credit Card Dashboard",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ───────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 12px;
        color: white;
        text-align: center;
    }
    .metric-card h3 {
        margin: 0;
        font-size: 14px;
        opacity: 0.9;
    }
    .metric-card h1 {
        margin: 5px 0;
        font-size: 28px;
    }
    .stMetric > div {
        background-color: #f8f9fa;
        padding: 12px;
        border-radius: 8px;
        border-left: 4px solid #667eea;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_db():
    return Database()


@st.cache_resource
def get_client_event_logger():
    return PrivacyEventLogger(channel="client")


@st.cache_data(ttl=300)
def load_transactions(_db, filters_key):
    """Load transactions with caching.

    filters_key is a tuple-of-2-tuples (hashable); multi-value entries are
    stored as tuples and must be converted back to lists for get_transactions.
    """
    filters = {k: list(v) if isinstance(v, tuple) else v for k, v in filters_key}
    return _db.get_transactions(filters)


def format_inr(amount):
    """Format amount in INR."""
    if amount >= 10_000_000:
        return f"₹{amount/10_000_000:.2f} Cr"
    elif amount >= 100_000:
        return f"₹{amount/100_000:.2f} L"
    elif amount >= 1000:
        return f"₹{amount/1000:.1f}K"
    return f"₹{amount:,.2f}"


def main():
    db = get_db()
    client_logger = get_client_event_logger()

    if "client_session_id" not in st.session_state:
        st.session_state["client_session_id"] = uuid4().hex

    session_hash = pseudonymize_value(st.session_state["client_session_id"])

    if not st.session_state.get("client_page_view_logged"):
        client_logger.log_event(
            "dashboard_page_loaded",
            {
                "session_id": st.session_state["client_session_id"],
                "session_hash": session_hash,
            },
            source="dashboard",
        )
        st.session_state["client_page_view_logged"] = True

    # ── Sidebar ──────────────────────────────────────────────────
    st.sidebar.title("💳 CCAnalyser")
    st.sidebar.markdown("---")

    summary = db.get_summary()

    # ── Always show tabs (Parse PDF works even with empty DB) ────
    st.title("💳 Credit Card Analytics Dashboard")
    tab_analytics, tab_statements, tab_parse = st.tabs(
        ["📊 Analytics", "📁 Statements", "📤 Parse PDF"]
    )

    with tab_parse:
        _render_parse_pdf(db)

    # No-data state: analytics/statements show a prompt
    if summary["total_transactions"] == 0:
        with tab_analytics:
            st.warning(
                "No transactions found. Run the pipeline first:\n\n"
                "```\npython run_pipeline.py\n```"
            )
            st.info(
                "**Setup steps:**\n"
                "1. Place your `credentials.json` from Google Cloud Console in the project root\n"
                "2. Fill in `.env` with your DOB and PAN\n"
                "3. Run `python run_pipeline.py`\n"
                "4. Refresh this dashboard — or use **Parse PDF** above to import a statement now"
            )
        return

    if hasattr(db, "get_date_anchor_options"):
        date_anchors = db.get_date_anchor_options()
    else:
        fallback_end = summary["date_range"].get("end") if summary.get("date_range") else None
        fallback_start = summary["date_range"].get("start") if summary.get("date_range") else None
        date_anchors = {
            "latest_statement_end_date": None,
            "latest_statement_received_date": fallback_end,
            "current_date": datetime.now().strftime("%Y-%m-%d"),
            "transaction_min_date": fallback_start,
        }
    banks = db.get_banks()
    categories = db.get_categories()
    cards = db.get_cards()
    card_names = db.get_card_names()

    # Date range
    st.sidebar.subheader("📅 Date Range")
    date_min = date_anchors["transaction_min_date"] or summary["date_range"]["start"] or "2024-06-01"

    date_to_options = {
        "Latest statement end date": (
            date_anchors["latest_statement_end_date"]
            or date_anchors["latest_statement_received_date"]
            or date_anchors["current_date"]
        ),
        "Latest statement received date": (
            date_anchors["latest_statement_received_date"]
            or date_anchors["current_date"]
        ),
        "Current date": date_anchors["current_date"],
    }
    selected_to_anchor = st.sidebar.selectbox(
        "To date basis",
        options=list(date_to_options.keys()),
        index=0,
    )
    date_max = date_to_options[selected_to_anchor]

    # Default "from" to 2 years before the selected end date so we don't
    # show decade-old rows caused by bad date parsing.
    date_max_dt = datetime.strptime(date_max, "%Y-%m-%d")
    date_default_from = max(
        date_max_dt - timedelta(days=730),
        datetime.strptime(date_min, "%Y-%m-%d"),
    )

    col1, col2 = st.sidebar.columns(2)
    date_from = col1.date_input(
        "From",
        value=date_default_from,
        min_value=datetime.strptime(date_min, "%Y-%m-%d"),
        max_value=date_max_dt,
    )
    date_to = col2.date_input(
        "To",
        value=date_max_dt,
        min_value=datetime.strptime(date_min, "%Y-%m-%d"),
        max_value=date_max_dt,
    )

    # Bank filter
    st.sidebar.subheader("🏦 Bank")
    selected_banks = st.sidebar.multiselect("Select banks", banks, default=banks)

    # Card name filter
    if card_names:
        st.sidebar.subheader("💳 Card")
        selected_card_names = st.sidebar.multiselect("Select cards", card_names, default=card_names)
    else:
        selected_card_names = []

    # Category filter
    st.sidebar.subheader("🏷️ Category")
    selected_categories = st.sidebar.multiselect("Select categories", categories, default=categories)

    # Amount range
    st.sidebar.subheader("💰 Amount Range")
    amount_col1, amount_col2 = st.sidebar.columns(2)
    amount_min = amount_col1.number_input("Min", min_value=0.0, value=0.0, step=100.0)
    amount_max = amount_col2.number_input("Max", min_value=0.0, value=0.0, step=100.0)

    # Search
    st.sidebar.subheader("🔍 Search")
    search_query = st.sidebar.text_input("Search merchant/description")

    # Card last-4 filter
    if cards:
        st.sidebar.subheader("🔢 Card Last 4")
        selected_card = st.sidebar.selectbox("Card (last 4 digits)", ["All"] + cards)
    else:
        selected_card = "All"

    # Transaction type
    st.sidebar.subheader("📊 Type")
    txn_type = st.sidebar.radio("Transaction type", ["All", "Debit", "Credit"], horizontal=True)

    # Build filters
    filters = {
        "date_from": date_from.strftime("%Y-%m-%d"),
        "date_to": date_to.strftime("%Y-%m-%d"),
    }
    if selected_banks and len(selected_banks) < len(banks):
        filters["bank"] = selected_banks
    if selected_card_names and len(selected_card_names) < len(card_names):
        filters["card_name"] = selected_card_names
    if selected_categories and len(selected_categories) < len(categories):
        filters["category"] = selected_categories
    if amount_min > 0:
        filters["amount_min"] = amount_min
    if amount_max > 0:
        filters["amount_max"] = amount_max
    if search_query:
        filters["search"] = search_query
    if selected_card != "All":
        filters["card_last4"] = selected_card
    if txn_type != "All":
        filters["type"] = txn_type.lower()

    # Load data
    # Convert any list values to tuples so the cache key is fully hashable.
    hashable_filters = {
        k: tuple(v) if isinstance(v, list) else v
        for k, v in filters.items()
    }
    df = load_transactions(db, tuple(sorted(hashable_filters.items())))

    current_filter_signature = str({
        "date_from": filters.get("date_from"),
        "date_to": filters.get("date_to"),
        "to_basis": selected_to_anchor,
        "banks_count": len(selected_banks or []),
        "cards_count": len(selected_card_names or []),
        "categories_count": len(selected_categories or []),
        "has_search": bool(search_query),
        "search_len": len(search_query or ""),
        "txn_type": txn_type,
    })

    if st.session_state.get("last_filter_signature") != current_filter_signature:
        st.session_state["last_filter_signature"] = current_filter_signature
        client_logger.log_event(
            "dashboard_filters_applied",
            {
                "session_id": st.session_state["client_session_id"],
                "session_hash": session_hash,
                "to_date_basis": selected_to_anchor,
                "date_from": filters.get("date_from"),
                "date_to": filters.get("date_to"),
                "banks_count": len(selected_banks or []),
                "cards_count": len(selected_card_names or []),
                "categories_count": len(selected_categories or []),
                "has_search": bool(search_query),
                "search_len": len(search_query or ""),
                "txn_type": txn_type,
                "result_count": int(len(df)),
            },
            source="dashboard",
        )

    # ── Main Content ─────────────────────────────────────────────
    with tab_analytics:
        _render_analytics(df)

    with tab_statements:
        _render_statements(db)


# ── Analytics tab renderer ────────────────────────────────────────────────────

def _render_analytics(df: "pd.DataFrame") -> None:
    if df.empty:
        st.info("No transactions match the selected filters.")
        return

    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.to_period("M").astype(str)
    df["day_of_week"] = df["date"].dt.day_name()
    df["week"] = df["date"].dt.isocalendar().week.astype(int)

    debits = df[df["type"] == "debit"]
    credits = df[df["type"] == "credit"]

    # ── Summary Cards ─────────────────────────────────────────────
    st.markdown("### 📊 Overview")
    m1, m2, m3, m4, m5 = st.columns(5)

    total_spend = debits["amount"].sum()
    total_credit = credits["amount"].sum()
    num_months = max(debits["month"].nunique(), 1)
    avg_monthly = total_spend / num_months
    top_category = debits.groupby("category")["amount"].sum().idxmax() if not debits.empty else "N/A"
    num_txns = len(df)

    m1.metric("Total Spend", format_inr(total_spend))
    m2.metric("Total Credits", format_inr(total_credit))
    m3.metric("Avg Monthly Spend", format_inr(avg_monthly))
    m4.metric("Top Category", top_category)
    m5.metric("Transactions", f"{num_txns:,}")

    st.markdown("---")

    # ── Charts Row 1 ─────────────────────────────────────────────
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.subheader("📈 Monthly Spend Trend")
        monthly = debits.groupby("month").agg(
            total=("amount", "sum"),
            count=("amount", "count"),
        ).reset_index()

        if not monthly.empty:
            fig = px.line(
                monthly, x="month", y="total",
                markers=True,
                labels={"month": "Month", "total": "Amount (₹)"},
            )
            fig.update_traces(
                line=dict(color="#667eea", width=3),
                marker=dict(size=8),
            )
            fig.update_layout(
                hovermode="x unified",
                yaxis_tickprefix="₹",
                margin=dict(l=0, r=0, t=10, b=0),
            )
            st.plotly_chart(fig, use_container_width=True)

    with chart_col2:
        st.subheader("🎯 Category Breakdown")
        cat_spend = debits.groupby("category")["amount"].sum().reset_index()
        cat_spend = cat_spend.sort_values("amount", ascending=False)

        if not cat_spend.empty:
            fig = px.pie(
                cat_spend, values="amount", names="category",
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3,
            )
            fig.update_traces(
                textposition="inside",
                textinfo="percent+label",
                hovertemplate="%{label}: ₹%{value:,.0f}<extra></extra>",
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, use_container_width=True)

    # ── Charts Row 2 ─────────────────────────────────────────────
    chart_col3, chart_col4 = st.columns(2)

    with chart_col3:
        st.subheader("🏪 Top Merchants by Spend")
        merchant_spend = debits.groupby("description")["amount"].sum().nlargest(15).reset_index()

        if not merchant_spend.empty:
            fig = px.bar(
                merchant_spend, x="amount", y="description",
                orientation="h",
                labels={"amount": "Amount (₹)", "description": "Merchant"},
                color="amount",
                color_continuous_scale="Viridis",
            )
            fig.update_layout(
                yaxis=dict(autorange="reversed"),
                xaxis_tickprefix="₹",
                showlegend=False,
                coloraxis_showscale=False,
                margin=dict(l=0, r=0, t=10, b=0),
            )
            st.plotly_chart(fig, use_container_width=True)

    with chart_col4:
        st.subheader("🏦 Spend by Bank & Card")
        if "card_name" in debits.columns and debits["card_name"].notna().any():
            debits_with_card = debits.copy()
            debits_with_card["bank_card"] = debits_with_card.apply(
                lambda r: f"{r['bank']} - {r['card_name']}" if pd.notna(r.get("card_name")) else r["bank"],
                axis=1,
            )
            card_spend = debits_with_card.groupby("bank_card")["amount"].sum().reset_index()
            card_spend = card_spend.sort_values("amount", ascending=False)
            if not card_spend.empty:
                fig = px.bar(
                    card_spend, x="bank_card", y="amount",
                    labels={"amount": "Amount (₹)", "bank_card": "Bank - Card"},
                    color="bank_card",
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                )
                fig.update_layout(
                    yaxis_tickprefix="₹",
                    showlegend=False,
                    margin=dict(l=0, r=0, t=10, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            bank_spend = debits.groupby("bank")["amount"].sum().reset_index()
            bank_spend = bank_spend.sort_values("amount", ascending=False)
            if not bank_spend.empty:
                fig = px.bar(
                    bank_spend, x="bank", y="amount",
                    labels={"amount": "Amount (₹)", "bank": "Bank"},
                    color="bank",
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                )
                fig.update_layout(
                    yaxis_tickprefix="₹",
                    showlegend=False,
                    margin=dict(l=0, r=0, t=10, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)

    # ── Daily Heatmap ─────────────────────────────────────────────
    st.markdown("---")
    st.subheader("🗓️ Daily Spend Heatmap")

    if not debits.empty:
        daily = debits.groupby("date")["amount"].sum().reset_index()
        daily["day_of_week"] = daily["date"].dt.day_name()
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        fig = px.scatter(
            daily, x="date", y="day_of_week",
            size="amount", color="amount",
            color_continuous_scale="YlOrRd",
            size_max=20,
            labels={"amount": "Spend (₹)", "day_of_week": "Day", "date": "Date"},
            category_orders={"day_of_week": day_order},
        )
        fig.update_layout(
            yaxis=dict(categoryorder="array", categoryarray=day_order[::-1]),
            margin=dict(l=0, r=0, t=10, b=0),
            height=300,
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Category Drill-down ───────────────────────────────────────
    st.markdown("---")
    st.subheader("🔍 Category Drill-down")

    if not debits.empty:
        selected_drill_category = st.selectbox(
            "Select category to drill down",
            options=sorted(debits["category"].unique()),
        )

        cat_df = debits[debits["category"] == selected_drill_category]

        drill_col1, drill_col2 = st.columns(2)

        with drill_col1:
            cat_monthly = cat_df.groupby("month")["amount"].sum().reset_index()
            if not cat_monthly.empty:
                fig = px.bar(
                    cat_monthly, x="month", y="amount",
                    labels={"month": "Month", "amount": "Amount (₹)"},
                    color_discrete_sequence=["#667eea"],
                )
                fig.update_layout(
                    yaxis_tickprefix="₹",
                    margin=dict(l=0, r=0, t=10, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)

        with drill_col2:
            cat_merchants = cat_df.groupby("description")["amount"].sum().nlargest(10).reset_index()
            if not cat_merchants.empty:
                fig = px.bar(
                    cat_merchants, x="amount", y="description",
                    orientation="h",
                    labels={"amount": "Amount (₹)", "description": "Merchant"},
                    color_discrete_sequence=["#764ba2"],
                )
                fig.update_layout(
                    yaxis=dict(autorange="reversed"),
                    xaxis_tickprefix="₹",
                    margin=dict(l=0, r=0, t=10, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)

    # ── Transaction Table ─────────────────────────────────────────
    st.markdown("---")
    st.subheader("📋 Transaction Details")

    sort_col1, sort_col2 = st.columns([3, 1])
    sort_by = sort_col1.selectbox(
        "Sort by",
        ["date", "amount", "description", "category", "bank"],
        index=0,
    )
    sort_order = sort_col2.radio("Order", ["Descending", "Ascending"], horizontal=True)

    display_df = df.copy()
    display_df = display_df.sort_values(sort_by, ascending=(sort_order == "Ascending"))

    display_cols = ["date", "description", "amount", "type", "category", "bank", "card_name", "card_last4"]
    display_cols = [c for c in display_cols if c in display_df.columns]
    display_df = display_df[display_cols].copy()
    display_df["date"] = display_df["date"].dt.strftime("%Y-%m-%d")
    display_df["amount"] = display_df["amount"].apply(lambda x: f"₹{x:,.2f}")

    page_size = st.selectbox("Rows per page", [25, 50, 100, 200], index=0)
    total_pages = max(1, (len(display_df) - 1) // page_size + 1)
    page = st.number_input("Page", min_value=1, max_value=total_pages, value=1)

    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size

    st.dataframe(
        display_df.iloc[start_idx:end_idx],
        use_container_width=True,
        hide_index=True,
    )
    st.caption(f"Showing {start_idx + 1}-{min(end_idx, len(display_df))} of {len(display_df)} transactions")

    # ── Download ─────────────────────────────────────────────────
    st.markdown("---")
    export_cols = ["date", "description", "amount", "type", "category", "bank", "card_name", "card_last4"]
    export_cols = [c for c in export_cols if c in df.columns]
    csv_data = df[export_cols].copy()
    csv_data["date"] = csv_data["date"].dt.strftime("%Y-%m-%d")

    st.download_button(
        label="📥 Download Filtered Data as CSV",
        data=csv_data.to_csv(index=False).encode("utf-8"),
        file_name=f"ccanalyser_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )


# ── Statements tab renderer ───────────────────────────────────────────────────

@st.cache_data(ttl=300)
def _load_statements(_db):
    """Load all statement metadata rows."""
    import sqlite3
    with sqlite3.connect(str(_db.db_path)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT bank, card_name, filename, status, transaction_count, "
            "email_date, statement_period_start, statement_period_end, "
            "error_message, created_at "
            "FROM statements_metadata ORDER BY created_at DESC"
        ).fetchall()
    return pd.DataFrame([dict(r) for r in rows])


def _render_statements(db) -> None:
    st.subheader("📁 Processed Statements")

    sm = _load_statements(db)

    if sm.empty:
        st.info("No statements recorded yet. Run the pipeline first.")
        return

    # ── Summary metrics ───────────────────────────────────────────
    total = len(sm)
    completed = (sm["status"] == "completed").sum()
    no_data = (sm["status"] == "no_data").sum()
    errored = (sm["status"] == "error").sum()
    txn_total = sm["transaction_count"].sum()

    mc1, mc2, mc3, mc4, mc5 = st.columns(5)
    mc1.metric("Total Statements", total)
    mc2.metric("✅ Parsed", completed)
    mc3.metric("🔒 No Data / Locked", no_data)
    mc4.metric("❌ Errors", errored)
    mc5.metric("Transactions Imported", int(txn_total))

    st.markdown("---")

    # ── Status breakdown by bank ──────────────────────────────────
    if not sm.empty:
        bank_status = sm.groupby(["bank", "status"]).size().reset_index(name="count")
        if not bank_status.empty:
            fig = px.bar(
                bank_status, x="bank", y="count", color="status",
                barmode="stack",
                labels={"bank": "Bank", "count": "Statements", "status": "Status"},
                color_discrete_map={
                    "completed": "#4caf50",
                    "no_data": "#ff9800",
                    "error": "#f44336",
                    "pending": "#9e9e9e",
                },
            )
            fig.update_layout(
                margin=dict(l=0, r=0, t=10, b=0),
                showlegend=True,
            )
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Filter + table ────────────────────────────────────────────
    status_filter = st.multiselect(
        "Filter by status",
        options=sorted(sm["status"].unique()),
        default=sorted(sm["status"].unique()),
    )
    bank_filter = st.multiselect(
        "Filter by bank",
        options=sorted(sm["bank"].dropna().unique()),
        default=sorted(sm["bank"].dropna().unique()),
    )

    filtered_sm = sm[
        sm["status"].isin(status_filter) & sm["bank"].isin(bank_filter)
    ].copy()

    # Friendly column names
    filtered_sm = filtered_sm.rename(columns={
        "bank": "Bank",
        "card_name": "Card",
        "filename": "File",
        "status": "Status",
        "transaction_count": "Txns",
        "email_date": "Email Date",
        "statement_period_start": "Period Start",
        "statement_period_end": "Period End",
        "error_message": "Error",
    })
    filtered_sm = filtered_sm.drop(columns=["created_at"], errors="ignore")

    st.dataframe(filtered_sm, use_container_width=True, hide_index=True)
    st.caption(f"{len(filtered_sm)} of {len(sm)} statements shown")


# ── Parse PDF helpers ─────────────────────────────────────────────────────────

def _detect_bank_from_pdf_text(text: str) -> str | None:
    """Heuristic bank detection from first-page PDF text."""
    t = text.lower()
    if "csb bank" in t or "edge csb" in t:
        return "csb"
    if "federal bank" in t or "federalbank" in t:
        return "federal"
    if "idfc first" in t or "idfcfirstbank" in t:
        return "idfc_first"
    if "axis bank" in t or "axisbank" in t:
        return "axis"
    if "hdfc bank" in t:
        return "hdfc"
    if "sbi card" in t or "sbicard" in t:
        return "sbi"
    if "icici" in t:
        return "icici"
    if "yes bank" in t or "yesbank" in t:
        return "yes"
    if "kotak" in t:
        return "kotak"
    return None


def _unlock_pdf_bytes(pdf_bytes: bytes, extra_password: str | None) -> tuple[bytes | None, str]:
    """
    Try to produce an unlocked PDF from raw bytes.

    Priority:
      1. Open without password (already unlocked)
      2. User-supplied password
      3. Configured bank passwords

    Returns (unlocked_bytes, status_message).
    """
    # 1. Try as-is
    try:
        with pikepdf.open(io.BytesIO(pdf_bytes)) as pdf:
            buf = io.BytesIO()
            pdf.save(buf)
            return buf.getvalue(), "not_encrypted"
    except pikepdf.PasswordError:
        pass
    except Exception as e:
        return None, f"open_error:{e}"

    # 2. Build password candidate list
    cfg = load_config()
    known_pwds = get_all_passwords(cfg)
    candidates: list[str] = []
    if extra_password:
        candidates.append(extra_password)
    candidates.extend(known_pwds)

    for pwd in candidates:
        try:
            with pikepdf.open(io.BytesIO(pdf_bytes), password=pwd) as pdf:
                buf = io.BytesIO()
                pdf.save(buf)
                return buf.getvalue(), "unlocked"
        except pikepdf.PasswordError:
            continue
        except Exception:
            continue

    return None, "unlock_failed"


def _do_parse(
    pdf_bytes: bytes,
    filename: str,
    bank_ui: str,
    password: str | None,
    db: "Database",
) -> tuple[dict, str | None]:
    """
    Unlock, detect bank, and run the hybrid pipeline on the uploaded PDF.

    Returns (result_dict, error_message).  error_message is None on success.
    """
    # ── 1. Unlock ────────────────────────────────────────────────
    unlocked_bytes, unlock_status = _unlock_pdf_bytes(pdf_bytes, password)
    if unlocked_bytes is None:
        return {}, (
            "Could not unlock the PDF. "
            "Please enter the correct password in the 'PDF Password' field."
        )

    # ── 2. Extract first-page text for bank detection ─────────────
    first_page_text = ""
    try:
        with pdfplumber.open(io.BytesIO(unlocked_bytes)) as _pdf:
            if _pdf.pages:
                first_page_text = _pdf.pages[0].extract_text() or ""
    except Exception:
        pass

    # ── 3. Resolve bank ──────────────────────────────────────────
    if bank_ui == "Auto-detect":
        bank = _detect_bank_from_pdf_text(first_page_text) or "unknown"
    else:
        bank = bank_ui

    # ── 4. Write unlocked bytes to temp file ──────────────────────
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".pdf")
    try:
        with os.fdopen(tmp_fd, "wb") as fh:
            fh.write(unlocked_bytes)

        fhash = compute_file_hash(tmp_path)

        # Ensure statement row exists so update_statement_status works
        db.record_statement(
            file_hash=fhash,
            original_path=filename,
            bank=bank,
            filename=filename,
        )

        # ── 5. Run pipeline ───────────────────────────────────────
        pipeline = HybridPipeline(db=db)
        result = pipeline.process_pdf(
            tmp_path,
            bank=bank,
            fhash=fhash,
            path=tmp_path,
        )
        result["detected_bank"] = bank
        result["filename"] = filename
        result["unlock_status"] = unlock_status
        return result, None

    except Exception as exc:
        return {}, f"Parsing failed: {exc}"

    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


# ── Parse PDF UI ──────────────────────────────────────────────────────────────

def _render_parse_pdf(db: "Database") -> None:
    """Render the Parse PDF tab — full pipeline without Gmail fetch."""
    st.subheader("📤 Parse PDF Statement")
    st.caption(
        "Upload any credit card statement PDF to extract transactions using the same "
        "pipeline as the main flow. No Gmail fetch required — results are saved to the database."
    )

    # ── Upload + options row ──────────────────────────────────────
    upload_col, opts_col = st.columns([3, 2])

    with upload_col:
        uploaded_file = st.file_uploader(
            "Drop or browse a credit card statement PDF",
            type=["pdf"],
            help=(
                "Encrypted PDFs will be auto-unlocked using known bank passwords. "
                "Supply a custom password below if the file uses a different one."
            ),
        )

    with opts_col:
        bank_choices = ["Auto-detect"] + sorted(list_available_parsers()) + ["unknown"]
        selected_bank_ui = st.selectbox(
            "Bank",
            bank_choices,
            index=0,
            help="'Auto-detect' reads the first page to identify the bank.",
        )
        pdf_password = st.text_input(
            "PDF Password (optional)",
            type="password",
            help="Only needed if the PDF password is not in the standard bank list.",
        )

    st.markdown("---")

    parse_clicked = st.button(
        "🔍 Parse PDF",
        type="primary",
        disabled=uploaded_file is None,
    )

    if uploaded_file is None:
        st.info("⬆️ Upload a PDF above to get started.")
        return

    # ── Trigger or re-display ────────────────────────────────────
    result_key = f"_parse_result_{uploaded_file.file_id}"

    if parse_clicked:
        # Clear any previous result for this file
        st.session_state.pop(result_key, None)

        with st.spinner("🔓 Unlocking & parsing PDF — this may take a moment…"):
            result, err = _do_parse(
                uploaded_file.getvalue(),
                uploaded_file.name,
                selected_bank_ui,
                pdf_password.strip() or None,
                db,
            )

        if err:
            st.error(f"❌ {err}")
            return

        st.session_state[result_key] = result
        # Invalidate transaction caches so analytics reflects new data
        load_transactions.clear()

    result = st.session_state.get(result_key)
    if result:
        _display_parse_result(result, uploaded_file.name)


def _display_parse_result(result: dict, filename: str) -> None:
    """Render metrics + transaction table for a parse result."""
    method = result.get("method", "unknown")
    confidence = result.get("confidence", 0.0)
    txn_count = result.get("transaction_count", 0)
    inserted = result.get("inserted", 0)
    card_name = result.get("card_name")
    bank = result.get("detected_bank", "unknown")
    unlock_status = result.get("unlock_status", "")

    st.subheader("📊 Parse Results")

    # ── Top metrics ───────────────────────────────────────────────
    mc1, mc2, mc3, mc4, mc5 = st.columns(5)
    mc1.metric("Bank Detected", bank.upper())
    mc2.metric("Method", method.split("(")[0])
    mc3.metric("Confidence", f"{confidence:.0%}")
    mc4.metric("Transactions", txn_count)
    mc5.metric("New to DB", inserted)

    if card_name:
        st.success(f"Card identified: **{card_name}**")
    if unlock_status == "unlocked":
        st.info("🔓 PDF was password-protected and successfully unlocked.")

    if inserted < txn_count:
        st.caption(
            f"ℹ️ {txn_count - inserted} transaction(s) already existed in the database "
            "(duplicate hashes — no double-counting)."
        )

    if txn_count == 0:
        st.warning(
            "No transactions could be extracted from this PDF. "
            "Try selecting the correct bank or ensure the file is a credit card statement."
        )
        return

    # ── Transaction breakdown ─────────────────────────────────────
    txns = result.get("transactions", [])
    if not txns:
        return

    df = pd.DataFrame(txns)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    debits = df[df["type"] == "debit"] if "type" in df.columns else df
    credits = df[df["type"] == "credit"] if "type" in df.columns else pd.DataFrame()

    sc1, sc2, sc3 = st.columns(3)
    sc1.metric("Total Debit", format_inr(debits["amount"].sum() if not debits.empty else 0))
    sc2.metric("Total Credit", format_inr(credits["amount"].sum() if not credits.empty else 0))
    if not debits.empty and "category" in debits.columns and debits["category"].notna().any():
        top_cat = debits.groupby("category")["amount"].sum().idxmax()
        sc3.metric("Top Category", top_cat)

    # ── Transaction table ─────────────────────────────────────────
    st.markdown("#### Extracted Transactions")
    display_cols = ["date", "description", "amount", "type", "category", "card_name", "card_last4"]
    display_cols = [c for c in display_cols if c in df.columns]

    disp = df[display_cols].copy()
    if "date" in disp.columns:
        disp["date"] = disp["date"].dt.strftime("%Y-%m-%d").fillna("")
    if "amount" in disp.columns:
        disp["amount"] = disp["amount"].apply(lambda x: f"₹{x:,.2f}")

    st.dataframe(disp, use_container_width=True, hide_index=True)

    # ── Download ──────────────────────────────────────────────────
    export_df = df[display_cols].copy()
    if "date" in export_df.columns:
        export_df["date"] = export_df["date"].dt.strftime("%Y-%m-%d").fillna("")
    stem = Path(filename).stem
    st.download_button(
        label="📥 Download as CSV",
        data=export_df.to_csv(index=False).encode("utf-8"),
        file_name=f"parse_{stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
