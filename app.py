import os
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh

FORECAST_PARQUET_PATH = Path("data/weather_forecast_clean.parquet")
CURRENT_PARQUET_PATH = Path("data/weather_current_clean.parquet")
DAILY_PARQUET_PATH = Path("data/weather_daily_clean.parquet")
HISTORICAL_PARQUET_PATH = Path("data/weather_historical_hourly.parquet")

# Load environment variables so local runs can pick up .env settings too.
load_dotenv()

THEME = {
    "page_bg": "#f8f4ef",
    "surface_bg": "#fffdf9",
    "card_bg": "#fffdf9",
    "card_border": "#e7ddd1",
    "soft_border": "#ddd4c8",
    "shadow": "0 12px 28px rgba(15, 23, 42, 0.06)",
    "title_color": "#183b6b",
    "section_title_color": "#294f7e",
    "body_color": "#334155",
    "muted_color": "#7b8794",
    "temp_color": "#f97316",
    "heading_font": "'Poppins', 'Roboto', sans-serif",
    "body_font": "'Roboto', 'Poppins', sans-serif",
}


@st.cache_data
def load_parquet_data():
    """Load the parquet files used by the dashboard."""
    forecast_df = pd.read_parquet(get_data_source("forecast"))
    current_df = pd.read_parquet(get_data_source("current"))
    daily_df = pd.read_parquet(get_data_source("daily"))
    historical_df = pd.read_parquet(get_data_source("historical"))
    forecast_df["forecast_time_dhaka"] = pd.to_datetime(
        forecast_df["forecast_time_dhaka"]
    )
    current_df["extracted_at"] = pd.to_datetime(current_df["extracted_at"])
    daily_df["forecast_date"] = pd.to_datetime(daily_df["forecast_date"])
    historical_df["observed_time_dhaka"] = pd.to_datetime(
        historical_df["observed_time_dhaka"]
    )
    return forecast_df, current_df, daily_df, historical_df


def get_data_source(dataset_name):
    """Return either a local parquet path or an S3 parquet path for the dashboard."""

    # Read optional S3 settings so hosted dashboards can read the latest files directly.
    bucket_name = os.getenv("AWS_S3_BUCKET")
    use_s3_data = os.getenv("STREAMLIT_USE_S3", "false").lower() == "true"

    # Map dataset names to their local files and S3 object keys.
    local_sources = {
        "forecast": str(FORECAST_PARQUET_PATH),
        "current": str(CURRENT_PARQUET_PATH),
        "daily": str(DAILY_PARQUET_PATH),
        "historical": str(HISTORICAL_PARQUET_PATH),
    }
    s3_keys = {
        "forecast": os.getenv(
            "AWS_S3_FORECAST_KEY", "weather/weather_forecast_clean.parquet"
        ),
        "current": os.getenv(
            "AWS_S3_CURRENT_KEY", "weather/weather_current_clean.parquet"
        ),
        "daily": os.getenv("AWS_S3_DAILY_KEY", "weather/weather_daily_clean.parquet"),
        "historical": os.getenv(
            "AWS_S3_HISTORICAL_KEY", "weather/weather_historical_hourly.parquet"
        ),
    }

    # Use S3 only when explicitly enabled and a bucket name is present.
    if use_s3_data and bucket_name:
        return f"s3://{bucket_name}/{s3_keys[dataset_name]}"

    # Otherwise fall back to the local parquet files.
    return local_sources[dataset_name]


def inject_global_styles():
    """Inject global CSS so the theme is controlled in one place."""
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;600;700&family=Roboto:wght@400;500;700&display=swap');
        .stApp {{
            background:
                radial-gradient(circle at top left, rgba(37, 99, 235, 0.04) 0%, transparent 22%),
                linear-gradient(180deg, {THEME["page_bg"]} 0%, #f5efe7 100%);
            color: {THEME["body_color"]};
            font-family: {THEME["body_font"]};
        }}

        .block-container {{ padding-top: 1.55rem; padding-bottom: 2.35rem; }}
        h2 {{
            font-family: {THEME["heading_font"]};
            color: {THEME["section_title_color"]};
            font-weight: 500;
            letter-spacing: -0.02em;
        }}
        [data-testid="stDataFrame"] {{
            border: 1px solid {THEME["soft_border"]};
            border-radius: 18px;
            overflow: hidden;
            background: {THEME["surface_bg"]};
        }}
        .loading-shell {{
            margin-top: 0.6rem;
            margin-bottom: 1.4rem;
            max-width: 430px;
            background: rgba(255, 253, 249, 0.96);
            border: 1px solid {THEME["soft_border"]};
            border-radius: 22px;
            box-shadow: {THEME["shadow"]};
            padding: 1rem 1.1rem;
        }}
        .loading-title {{
            font-family: {THEME["heading_font"]};
            color: {THEME["title_color"]};
            font-size: 1rem;
            font-weight: 600;
            letter-spacing: -0.01em;
            margin-bottom: 0.3rem;
        }}
        .loading-copy {{
            color: {THEME["muted_color"]};
            font-size: 0.92rem;
            line-height: 1.55;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_loading_state():
    """Render a cleaner loading card while the dashboard fetches data."""

    st.markdown(
        """
        <div class="loading-shell">
            <div class="loading-title">Refreshing the latest weather snapshot</div>
            <div class="loading-copy">
                Pulling the newest parquet files so the dashboard opens with the most recent Dhaka update.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_intro(extracted_at_text):
    """Render page subtitle and update timestamp."""
    st.markdown(
        f"""
        <div style="font-family:{THEME['heading_font']};color:{THEME['title_color']};font-weight:700;letter-spacing:-0.03em;font-size:3rem;line-height:1.04;margin:0 0 0.38rem 0;">
            Dhaka Weather Dashboard
        </div>
        <div style="color:{THEME['muted_color']};margin-top:0;margin-bottom:0.78rem;font-size:0.98rem;line-height:1.45;">
            Dhaka's weather at a glance with current conditions, daily outlook and historical insight.
        </div>
        <div style="
            display:inline-flex;gap:0.45rem;align-items:center;margin-bottom:0.95rem;
            background:rgba(255,253,249,0.88);border:1px solid {THEME['soft_border']};
            border-radius:20px;padding:0.55rem 0.95rem;font-size:0.93rem;color:{THEME['body_color']};
        ">
            <span style="font-weight:500;">Last updated</span>
            <span style="color:{THEME['muted_color']};">{extracted_at_text}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_section_title(title, caption):
    """Render one section heading and caption."""
    st.markdown(
        f"""
        <div style="margin-top:1.45rem;margin-bottom:0.7rem;">
            <div style="font-family:{THEME['heading_font']};color:{THEME['section_title_color']};font-size:1.75rem;font-weight:500;letter-spacing:-0.02em;line-height:1.04;margin:0 0 0.8rem 0;">
                {title}
            </div>
            <div style="color:{THEME['muted_color']};font-size:0.92rem;line-height:1.34;margin:0;">
                {caption}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_current_weather_card(current_row):
    """Render the hero card for current conditions."""

    # Show the current Dhaka time in the card header so it behaves like a live
    # local clock, independent of when the weather data was extracted.
    current_dhaka_time = pd.Timestamp.now(tz="Asia/Dhaka").strftime("%I:%M %p")

    st.markdown(
        f"""
        <div style="background:{THEME['card_bg']};border:1px solid {THEME['card_border']};border-radius:22px;box-shadow:{THEME['shadow']};overflow:hidden;">
            <div style="display:flex;justify-content:space-between;align-items:center;padding:1rem 1.35rem 0.9rem 1.35rem;border-bottom:1px solid {THEME['soft_border']};color:{THEME['muted_color']};font-size:0.92rem;font-weight:600;text-transform:uppercase;letter-spacing:0.04em;">
                <span>Current Weather</span>
                <span>{current_dhaka_time}</span>
            </div>
            <div style="display:grid;grid-template-columns:1.2fr 1fr;gap:1.25rem;padding:1.35rem;">
                <div>
                    <div style="display:flex;align-items:center;gap:1rem;">
                        <div style="font-size:3rem;">&#9728;</div>
                        <div style="font-family:{THEME['heading_font']};color:{THEME['title_color']};font-size:4rem;font-weight:600;line-height:0.95;">
                            {current_row['temperature_2m']:.1f}<span style="font-size:2rem;color:{THEME['muted_color']};">&deg;C</span>
                        </div>
                    </div>
                    <div style="margin-top:0.7rem;color:{THEME['body_color']};font-size:1.05rem;font-weight:500;">{current_row['weather_summary']}</div>
                    <div style="margin-top:0.3rem;color:{THEME['muted_color']};font-size:0.96rem;">Dhaka, Bangladesh</div>
                </div>
                <div>
                    <div style="display:flex;justify-content:space-between;padding:0.45rem 0;border-bottom:1px solid {THEME['soft_border']};"><span style="color:{THEME['muted_color']};">Humidity</span><span style="color:{THEME['title_color']};font-weight:600;">{current_row['relative_humidity_2m']}%</span></div>
                    <div style="display:flex;justify-content:space-between;padding:0.75rem 0;border-bottom:1px solid {THEME['soft_border']};"><span style="color:{THEME['muted_color']};">Wind</span><span style="color:{THEME['title_color']};font-weight:600;">{current_row['wind_speed_10m']:.1f} km/h</span></div>
                    <div style="display:flex;justify-content:space-between;padding:0.75rem 0;"><span style="color:{THEME['muted_color']};">Condition</span><span style="color:{THEME['title_color']};font-weight:600;">{current_row['weather_summary']}</span></div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_daily_outlook_card(daily_df):
    """Render a 3-day outlook card with max and min temperatures."""
    rows = []
    for row in daily_df.head(3).itertuples(index=False):
        rows.append(
            f'<div style="display:grid;grid-template-columns:88px 120px 1fr;align-items:center;gap:0.9rem;padding:1rem 1.35rem;border-top:1px solid {THEME["soft_border"]};">'
            f'<div><div style="color:{THEME["title_color"]};font-weight:700;">{row.forecast_date.strftime("%a").upper()}</div><div style="color:{THEME["muted_color"]};font-size:0.9rem;">{row.forecast_date.strftime("%m/%d")}</div></div>'
            f'<div style="display:flex;align-items:baseline;gap:0.5rem;font-family:{THEME["heading_font"]};color:{THEME["title_color"]};"><span style="font-size:2rem;font-weight:600;">{row.temperature_2m_max:.0f}&deg;</span><span style="font-size:1.35rem;color:{THEME["muted_color"]};">{row.temperature_2m_min:.0f}&deg;</span></div>'
            f'<div><div style="color:{THEME["body_color"]};font-size:1rem;font-weight:500;">{row.weather_summary}</div><div style="color:{THEME["muted_color"]};font-size:0.92rem;margin-top:0.15rem;">Max {row.temperature_2m_max:.0f}&deg; / Min {row.temperature_2m_min:.0f}&deg;</div></div>'
            "</div>"
        )
    st.markdown(
        f'<div style="background:{THEME["card_bg"]};border:1px solid {THEME["card_border"]};border-radius:22px;box-shadow:{THEME["shadow"]};overflow:hidden;"><div style="padding:1rem 1.35rem 0.9rem 1.35rem;color:{THEME["muted_color"]};font-size:0.92rem;font-weight:600;text-transform:uppercase;letter-spacing:0.04em;">Next 3 Days Outlook</div>{"".join(rows)}</div>',
        unsafe_allow_html=True,
    )


def render_story_card(title, primary, text):
    """Render a compact narrative insight card."""
    st.markdown(
        f"""
        <div style="background:{THEME['card_bg']};border:1px solid {THEME['card_border']};border-radius:20px;box-shadow:{THEME['shadow']};padding:1.2rem 1.25rem 1.15rem 1.25rem;min-height:158px;">
            <div style="color:{THEME['muted_color']};font-size:0.8rem;font-weight:600;text-transform:uppercase;letter-spacing:0.07em;margin-bottom:0.8rem;">{title}</div>
            <div style="color:{THEME['title_color']};font-family:{THEME['heading_font']};font-size:1.55rem;font-weight:600;line-height:1.15;letter-spacing:-0.025em;margin-bottom:0.65rem;">{primary}</div>
            <div style="color:{THEME['body_color']};font-size:0.9rem;line-height:1.62;">{text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def prepare_chart_labels(chart_df, value_column):
    """Label every hourly point on the hourly temperature chart."""
    label_df = chart_df.copy()
    label_df["value_label"] = label_df[value_column].round(1).astype(str)
    return label_df


def render_temperature_forecast_chart(chart_df):
    """Render the only remaining hourly chart: temperature forecast."""
    label_df = prepare_chart_labels(chart_df, "temperature_2m")
    x_axis_start = chart_df["forecast_time_dhaka"].min() - pd.Timedelta(minutes=30)
    x_axis_end = chart_df["forecast_time_dhaka"].max() + pd.Timedelta(minutes=30)
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=chart_df["forecast_time_dhaka"],
            y=chart_df["temperature_2m"],
            mode="lines+markers",
            line={"color": THEME["temp_color"], "width": 3},
            marker={"size": 5, "color": THEME["temp_color"]},
            hovertemplate="%{x|%I:%M %p}<br>%{y:.1f}<extra></extra>",
            showlegend=False,
        )
    )
    figure.add_trace(
        go.Scatter(
            x=label_df["forecast_time_dhaka"],
            y=label_df["temperature_2m"],
            mode="markers+text",
            marker={"size": 8, "color": THEME["temp_color"]},
            text=label_df["value_label"],
            textposition="top center",
            textfont={
                "size": 12,
                "color": THEME["body_color"],
                "family": THEME["body_font"],
            },
            cliponaxis=False,
            hoverinfo="skip",
            showlegend=False,
        )
    )
    figure.update_layout(
        title={
            "text": "Temperature Forecast",
            "x": 0.5,
            "xanchor": "center",
            "font": {
                "size": 17,
                "color": THEME["section_title_color"],
                "family": "Poppins, Roboto, sans-serif",
            },
        },
        height=310,
        margin={"l": 8, "r": 8, "t": 60, "b": 8},
        paper_bgcolor=THEME["surface_bg"],
        plot_bgcolor=THEME["surface_bg"],
    )
    figure.update_xaxes(
        range=[x_axis_start, x_axis_end],
        tickformat="%I %p",
        dtick=3600000,
        showgrid=False,
        showline=True,
        linewidth=1,
        linecolor=THEME["soft_border"],
        tickfont={"size": 12, "color": THEME["muted_color"]},
        zeroline=False,
    )
    figure.update_yaxes(
        showgrid=False,
        showticklabels=False,
        ticks="",
        showline=False,
        zeroline=False,
    )
    st.markdown(
        f'<div style="background:{THEME["surface_bg"]};border:1px solid {THEME["soft_border"]};border-radius:22px;box-shadow:{THEME["shadow"]};padding:0.55rem 0.65rem 0.35rem 0.65rem;">',
        unsafe_allow_html=True,
    )
    st.plotly_chart(
        figure, use_container_width=True, config={"displayModeBar": False}
    )
    st.markdown("</div>", unsafe_allow_html=True)


def build_current_vs_yesterday_story(current_row, historical_df):
    """Compare current weather against the same hour yesterday."""
    historical_df = historical_df[
        historical_df["observed_time_dhaka"] <= current_row["extracted_at"]
    ].copy()
    target_hour = (current_row["extracted_at"] - pd.Timedelta(days=1)).replace(
        minute=0, second=0, microsecond=0
    )
    yesterday_df = historical_df[historical_df["observed_time_dhaka"] == target_hour]
    if yesterday_df.empty:
        return None
    yesterday_row = yesterday_df.iloc[-1]
    return {
        "hour": target_hour.strftime("%I:%M %p"),
        "temp_diff": current_row["temperature_2m"] - yesterday_row["temperature_2m"],
        "humidity_diff": current_row["relative_humidity_2m"]
        - yesterday_row["relative_humidity_2m"],
        "wind_diff": current_row["wind_speed_10m"] - yesterday_row["wind_speed_10m"],
    }


def build_seven_day_temperature_trend(historical_df, current_timestamp):
    """Aggregate the last 7 days into daily avg, min, and max temperatures."""
    historical_df = historical_df[
        historical_df["observed_time_dhaka"] <= current_timestamp
    ].copy()
    latest_timestamp = historical_df["observed_time_dhaka"].max()
    start_date = (latest_timestamp.normalize() - pd.Timedelta(days=6)).date()
    recent_df = historical_df[
        historical_df["observed_time_dhaka"].dt.date >= start_date
    ].copy()
    return (
        recent_df.assign(day=recent_df["observed_time_dhaka"].dt.floor("D"))
        .groupby("day", as_index=False)
        .agg(
            avg_temp=("temperature_2m", "mean"),
            min_temp=("temperature_2m", "min"),
            max_temp=("temperature_2m", "max"),
        )
        .sort_values("day")
    )


def build_seven_day_summary(historical_df, current_timestamp):
    """Find the hottest day, coolest day, and windiest observed hour from the last 7 days."""
    historical_df = historical_df[
        historical_df["observed_time_dhaka"] <= current_timestamp
    ].copy()
    latest_timestamp = historical_df["observed_time_dhaka"].max()
    start_date = (latest_timestamp.normalize() - pd.Timedelta(days=6)).date()
    recent_df = historical_df[
        historical_df["observed_time_dhaka"].dt.date >= start_date
    ].copy()
    daily_avg_df = (
        recent_df.assign(day=recent_df["observed_time_dhaka"].dt.floor("D"))
        .groupby("day", as_index=False)
        .agg(avg_temp=("temperature_2m", "mean"))
        .sort_values("day")
    )
    return {
        "hottest_day": daily_avg_df.loc[daily_avg_df["avg_temp"].idxmax()],
        "coolest_day": daily_avg_df.loc[daily_avg_df["avg_temp"].idxmin()],
        "windiest_hour": recent_df.loc[recent_df["wind_speed_10m"].idxmax()],
    }


def render_temperature_trend_chart(trend_df):
    """Render a 7-day temperature trend with avg line and min/max band."""
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=trend_df["day"],
            y=trend_df["max_temp"],
            mode="lines",
            line={"width": 0},
            hoverinfo="skip",
            showlegend=False,
        )
    )
    figure.add_trace(
        go.Scatter(
            x=trend_df["day"],
            y=trend_df["min_temp"],
            mode="lines",
            fill="tonexty",
            fillcolor="rgba(249, 115, 22, 0.14)",
            line={"width": 0},
            hoverinfo="skip",
            showlegend=False,
        )
    )
    figure.add_trace(
        go.Scatter(
            x=trend_df["day"],
            y=trend_df["avg_temp"],
            mode="lines+markers+text",
            line={"color": THEME["temp_color"], "width": 3},
            marker={"size": 8, "color": THEME["temp_color"]},
            text=trend_df["avg_temp"].round(1).astype(str),
            textposition="top center",
            textfont={"size": 12, "color": THEME["body_color"]},
            hovertemplate="%{x|%b %d}<br>Avg: %{y:.1f}<extra></extra>",
            showlegend=False,
        )
    )
    figure.update_layout(
        title={
            "text": "Last 7 Days Temperature Trend",
            "x": 0.5,
            "xanchor": "center",
            "font": {
                "size": 16,
                "color": THEME["section_title_color"],
                "family": "Poppins, Roboto, sans-serif",
            },
        },
        height=330,
        margin={"l": 10, "r": 10, "t": 56, "b": 10},
        paper_bgcolor=THEME["surface_bg"],
        plot_bgcolor=THEME["surface_bg"],
    )
    figure.update_xaxes(
        tickformat="%b %d",
        showgrid=False,
        showline=True,
        linewidth=1,
        linecolor=THEME["soft_border"],
        tickfont={"size": 12, "color": THEME["muted_color"]},
        zeroline=False,
    )
    figure.update_yaxes(
        showgrid=False,
        tickfont={"size": 12, "color": THEME["muted_color"]},
        zeroline=False,
    )
    st.markdown(
        f'<div style="margin-top:1.4rem;background:{THEME["surface_bg"]};border:1px solid {THEME["soft_border"]};border-radius:22px;box-shadow:{THEME["shadow"]};padding:0.8rem 0.8rem 0.45rem 0.8rem;">',
        unsafe_allow_html=True,
    )
    st.plotly_chart(
        figure, use_container_width=True, config={"displayModeBar": False}
    )
    st.markdown("</div>", unsafe_allow_html=True)


st.set_page_config(page_title="Dhaka Weather Dashboard", layout="wide")

# Refresh the dashboard every minute so the current-time clock stays current
# without requiring the user to manually reload the page.
st_autorefresh(interval=60 * 1000, key="dhaka_dashboard_refresh")

inject_global_styles()

if (
    not FORECAST_PARQUET_PATH.exists()
    or not CURRENT_PARQUET_PATH.exists()
    or not DAILY_PARQUET_PATH.exists()
    or not HISTORICAL_PARQUET_PATH.exists()
):
    st.warning(
        "Required parquet files not found. Run `python scraper.py` first to generate clean data."
    )
    st.stop()

loading_placeholder = st.empty()
with loading_placeholder.container():
    render_loading_state()
with st.spinner("Loading the latest dashboard data..."):
    forecast_df, current_df, daily_df, historical_df = load_parquet_data()
loading_placeholder.empty()
latest_current = current_df.sort_values("extracted_at", ascending=False).iloc[0]
upcoming_df = forecast_df[forecast_df["is_upcoming"]].copy().head(12)
next_three_days_df = daily_df.sort_values("forecast_date").head(3)
comparison_story = build_current_vs_yesterday_story(latest_current, historical_df)
temperature_trend_df = build_seven_day_temperature_trend(
    historical_df, latest_current["extracted_at"]
)
seven_day_summary = build_seven_day_summary(
    historical_df, latest_current["extracted_at"]
)

render_intro(latest_current["extracted_at"].strftime("%Y-%m-%d %I:%M:%S %p %Z"))

top_card_col_1, top_card_col_2 = st.columns([1.25, 1])
with top_card_col_1:
    render_current_weather_card(latest_current)
with top_card_col_2:
    render_daily_outlook_card(next_three_days_df)

render_section_title(
    "Hourly Weather",
    "The dashboard keeps one clean short-term forecast view with 1-hour intervals.",
)
render_temperature_forecast_chart(upcoming_df)

render_section_title(
    "Weather Trends & Insights",
    "A quick look at how today compares, what's the trend and when the weather peaked.",
)

insight_col_1, insight_col_2 = st.columns(2)
with insight_col_1:
    if comparison_story is None:
        render_story_card(
            "Current vs Yesterday Same Hour",
            "Not enough history yet",
            "Run the pipeline over a longer period so the dashboard can compare the current hour against yesterday's matching record.",
        )
    else:
        temp_word = "warmer" if comparison_story["temp_diff"] >= 0 else "cooler"
        humidity_word = (
            "higher" if comparison_story["humidity_diff"] >= 0 else "lower"
        )
        wind_word = "stronger" if comparison_story["wind_diff"] >= 0 else "lighter"
        render_story_card(
            "Current vs Yesterday Same Hour",
            f"{abs(comparison_story['temp_diff']):.1f}°c warmer"
            if comparison_story["temp_diff"] >= 0
            else f"{abs(comparison_story['temp_diff']):.1f}°c cooler",
            f"As of {comparison_story['hour']}, temperature is {abs(comparison_story['temp_diff']):.1f}°c {temp_word}, humidity is {abs(comparison_story['humidity_diff']):.0f}% {humidity_word}, and wind is {abs(comparison_story['wind_diff']):.1f} km/h {wind_word} than yesterday.",
        )
with insight_col_2:
    hottest_day_date = seven_day_summary["hottest_day"]["day"].date()
    current_date = latest_current["extracted_at"].date()
    if hottest_day_date == current_date:
        hottest_text = (
            f"Today is currently the hottest day so far at "
            f"{seven_day_summary['hottest_day']['avg_temp']:.1f}°c avg"
        )
    else:
        hottest_text = (
            f"{seven_day_summary['hottest_day']['day'].strftime('%b %d')} was the hottest day at "
            f"{seven_day_summary['hottest_day']['avg_temp']:.1f}°c avg"
        )
    render_story_card(
        "This Week in Weather",
        f"Hottest day: {seven_day_summary['hottest_day']['day'].strftime('%b %d')}",
        f"{hottest_text}, while {seven_day_summary['coolest_day']['day'].strftime('%b %d')} offered the coolest relief at {seven_day_summary['coolest_day']['avg_temp']:.1f}°c. The windiest observed hour hit {seven_day_summary['windiest_hour']['wind_speed_10m']:.1f} km/h on {seven_day_summary['windiest_hour']['observed_time_dhaka'].strftime('%b %d')} at {seven_day_summary['windiest_hour']['observed_time_dhaka'].strftime('%I %p')}.",
    )

render_temperature_trend_chart(temperature_trend_df)
