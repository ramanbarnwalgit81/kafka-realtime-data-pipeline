import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh

# =====================================================
# Streamlit setup
# =====================================================
st.set_page_config(
    page_title="Real-Time Interaction Analytics",
    layout="wide"
)

st.title("📊 Real-Time Interaction Analytics")

# =====================================================
# Sidebar controls
# =====================================================
st.sidebar.header("Dashboard Controls")

refresh_rate = st.sidebar.slider(
    "Auto-refresh (seconds)", 2, 30, 10
)

TOP_N_USERS = st.sidebar.slider(
    "Top users to display", 5, 50, 20
)

TOP_N_ITEMS = st.sidebar.slider(
    "Top items to display", 5, 50, 20
)

show_raw = st.sidebar.checkbox(
    "Show Raw Aggregated Data", True
)

# =====================================================
# Auto refresh (safe)
# =====================================================
st_autorefresh(
    interval=refresh_rate * 1000,
    key="auto_refresh"
)

# =====================================================
# MongoDB connection
# =====================================================
MONGO_URI = "mongodb://admin:password@localhost:27017/?authSource=admin"
DB_NAME = "kpi_db"

@st.cache_resource
def get_client():
    return MongoClient(MONGO_URI)

client = get_client()
db = client[DB_NAME]

user_col = db["user_interactions"]
item_col = db["item_interactions"]

# =====================================================
# Load aggregated data
# =====================================================
user_df = pd.DataFrame(list(user_col.find({}, {"_id": 0})))
item_df = pd.DataFrame(list(item_col.find({}, {"_id": 0})))

# =====================================================
# Debug (safe to remove later)
# =====================================================
st.write("DEBUG → user_interactions rows:", len(user_df))
st.write("DEBUG → item_interactions rows:", len(item_df))

# =====================================================
# Data cleaning
# =====================================================
if not user_df.empty:
    user_df["window_end"] = pd.to_datetime(user_df["window_end"])
    user_df["total_interactions"] = pd.to_numeric(
        user_df["total_interactions"], errors="coerce"
    )
    user_df = user_df.dropna(subset=["total_interactions"])

if not item_df.empty:
    item_df["window_end"] = pd.to_datetime(item_df["window_end"])
    item_df["total_interactions"] = pd.to_numeric(
        item_df["total_interactions"], errors="coerce"
    )
    item_df = item_df.dropna(subset=["total_interactions"])

# =====================================================
# KPIs
# =====================================================
k1, k2, k3 = st.columns(3)

if user_df.empty or item_df.empty:
    k1.metric("Avg interactions / User", 0)
    k2.metric("Max interactions / Item", 0)
    k3.metric("Min interactions / Item", 0)
else:
    k1.metric(
        "Avg interactions / User",
        round(user_df["total_interactions"].mean(), 2)
    )
    k2.metric(
        "Max interactions / Item",
        int(item_df["total_interactions"].max())
    )
    k3.metric(
        "Min interactions / Item",
        int(item_df["total_interactions"].min())
    )

st.divider()

# =====================================================
# Raw aggregated data (always visible)
# =====================================================
if show_raw:
    st.subheader("📋 Raw Aggregated Data")

    with st.expander("User Interactions (Aggregated)"):
        st.dataframe(
            user_df.sort_values("window_end", ascending=False).head(100),
            width="stretch"
        )

    with st.expander("Item Interactions (Aggregated)"):
        st.dataframe(
            item_df.sort_values("window_end", ascending=False).head(100),
            width="stretch"
        )

st.divider()

# =====================================================
# Layout
# =====================================================
c1, c2 = st.columns(2)

# =====================================================
# Plot 1: Top-N Users by Interactions
# =====================================================
with c1:
    st.subheader("Total Interactions by User")

    if user_df.empty:
        st.info("No user interaction data available.")
    else:
        top_users_df = (
            user_df
            .sort_values("total_interactions", ascending=False)
            .head(TOP_N_USERS)
        )

        fig_user = px.bar(
            top_users_df,
            x="user_id",
            y="total_interactions",
            template="plotly_dark",
            title=f"Top {TOP_N_USERS} Users by Interactions"
        )

        st.plotly_chart(fig_user, width="stretch")

# =====================================================
# Plot 2: Item Interaction SNAPSHOT (LATEST WINDOW)
# =====================================================
with c2:
    st.subheader("Item Interaction Snapshot")

    if item_df.empty:
        st.info("No item interaction data available.")
    else:
        latest_window = item_df["window_end"].max()

        latest_items_df = (
            item_df[item_df["window_end"] == latest_window]
            .sort_values("total_interactions", ascending=False)
            .head(TOP_N_ITEMS)
        )

        fig_item = px.bar(
            latest_items_df,
            x="item_id",
            y="total_interactions",
            template="plotly_dark",
            title=f"Top {TOP_N_ITEMS} Items @ {latest_window}"
        )

        st.plotly_chart(fig_item, width="stretch")

# =====================================================
# End marker
# =====================================================
st.success("✅ Dashboard rendered successfully")
