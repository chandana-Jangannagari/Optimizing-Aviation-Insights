import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px

# ================================
# PAGE CONFIG
# ================================
st.set_page_config(page_title="Aviation Data Hub", page_icon="✈️", layout="wide")

st.markdown("""
<style>

.block-container {
    padding-top: 0.8rem;
    padding-bottom: 0rem;
    padding-left: 2rem;
    padding-right: 2rem;
}

h1 {
    margin-top: 0rem;
}
   .stPlotlyChart {
        margin-top: -20px;
        margin-bottom: -20px;
    }             
/* Reduce space above and below divider */
hr {
    margin-top: 5px !important;
    margin-bottom: 5px !important;
}
</style>
""", unsafe_allow_html=True)

st.title("✈️ Optimizing Aviation Insights")

# ================================
# SNOWFLAKE CONNECTION
# ================================
def get_connection():
    conn = snowflake.connector.connect(
        user='USERNAME',
        password='PASSWORD',
        account='SF ADMIN',
        warehouse="COMPUTE_WH",
        database="AVIATION_DB",
        schema="PUBLIC"
    )
    return conn


# ================================
# LOAD DATA
# ================================
@st.cache_data
def load_data():
    conn = get_connection()

    query = """
    SELECT *
    FROM GOLD_AVIATION_PERFORMANCE
    LIMIT 50000
    """

    df = pd.read_sql(query, conn)

    conn.close()

    df["DEPARTURE_DELAY"] = df["DEPARTURE_DELAY"].fillna(0)
    df["ARRIVAL_DELAY"] = df["ARRIVAL_DELAY"].fillna(0)

    return df


df = load_data()

# ================================
# SIDEBAR FILTERS
# ================================
st.sidebar.header("Filters")

# Convert FLIGHT_DATE to datetime
df["FLIGHT_DATE"] = pd.to_datetime(df["FLIGHT_DATE"])

# -------------------------------
# Airline Filter (Radio Buttons)
# -------------------------------
airlines = sorted(df["AIRLINE_ID"].unique())

# Add Select All option
airline_options = ["All"] + airlines

selected_airline = st.sidebar.radio(
    "Select Airline",
    airline_options
)

# -------------------------------
# Date Range Filter
# -------------------------------
date_range = st.sidebar.date_input(
    "Select Date Range",
    [df["FLIGHT_DATE"].min(), df["FLIGHT_DATE"].max()]
)

# -------------------------------
# Apply Filters
# -------------------------------

# Airline filter
if selected_airline == "All":
    filtered_df = df.copy()
else:
    filtered_df = df[df["AIRLINE_ID"] == selected_airline]

# Date filter
filtered_df = filtered_df[
    (filtered_df["FLIGHT_DATE"] >= pd.Timestamp(date_range[0])) &
    (filtered_df["FLIGHT_DATE"] <= pd.Timestamp(date_range[1]))
]




# ================================
# KPI CALCULATIONS
# ================================
base_total_flights = 4_800_000
base_cancelled_flights = 78860

ratio = len(filtered_df) / len(df)

total_flights = int(base_total_flights * ratio)
cancelled_flights = int(base_cancelled_flights * ratio)

# On-Time Flights
on_time_flights = len(filtered_df[filtered_df["DEPARTURE_DELAY"] <= 0])

# Delayed Flights (ADD THIS LINE)
delayed_flights = len(filtered_df[filtered_df["DEPARTURE_DELAY"] > 0])

# Average Delay
avg_departure_delay = filtered_df["DEPARTURE_DELAY"].mean()

# Total Distance
total_distance = filtered_df["DISTANCE"].sum()


# ================================
# KPI DISPLAY
# ================================
# ================================
# KPI DISPLAY (ONE LINE)
# ================================
k1, k2, k3, k4, k5, k6 = st.columns(6)

k1.metric("Total Flights", f"{total_flights:,}")
k2.metric("Cancelled Flights", f"{cancelled_flights:,}")
k3.metric("Delayed Flights", f"{delayed_flights:,}")
k4.metric("On-Time Flights", f"{on_time_flights:,}")
k5.metric("Avg Departure Delay", f"{avg_departure_delay:.2f} min")
k6.metric("Total Distance Flown", f"{total_distance:,.0f} miles")


# ================================
# CHART ROW 1
# ================================
c1, c2 = st.columns(2)

with c1:
    st.subheader("Average Delay by Airline")

    delay_airline = filtered_df.groupby("AIRLINE_ID")["DEPARTURE_DELAY"].mean().reset_index()

    fig1 = px.bar(
        delay_airline,
        x="AIRLINE_ID",
        y="DEPARTURE_DELAY",
        color="AIRLINE_ID",
        template="plotly_dark",
        height=300
    )

    st.plotly_chart(fig1, use_container_width=True)

with c2:
    st.subheader("Flight Volume Share")

    fig2 = px.pie(
        filtered_df,
        names="AIRLINE_ID",
        hole=0.4,
        template="plotly_dark",
        height=300
    )

    st.plotly_chart(fig2, use_container_width=True)


# ================================
# CHART ROW 2
# ================================
# --- ROW 3 (Side by Side Charts) ---

col1, col2 = st.columns(2)

with col1:
    st.subheader("Top Origin Airports")

    top_hubs = filtered_df['ORIGIN_AIRPORT_ID'].value_counts().head(10).reset_index()
    top_hubs.columns = ['ORIGIN_AIRPORT_ID', 'count']

    fig3 = px.bar(
        top_hubs,
        x='count',
        y='ORIGIN_AIRPORT_ID',
        orientation='h',
        color='ORIGIN_AIRPORT_ID',
        height = 260
    )

    st.plotly_chart(fig3, width="stretch")


with col2:
    st.subheader("Daily Delay Trend")

    trend = filtered_df.groupby('FLIGHT_DATE')['DEPARTURE_DELAY'].mean().reset_index()

    fig4 = px.line(
        trend,
        x='FLIGHT_DATE',
        y='DEPARTURE_DELAY',
        height = 260,
        color_discrete_sequence=['orange']
    )

    st.plotly_chart(fig4, width="stretch")