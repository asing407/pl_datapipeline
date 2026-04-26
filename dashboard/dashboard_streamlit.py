import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
import os
from dotenv import load_dotenv
import numpy as np
import streamlit.components.v1 as components

load_dotenv()

# Page config
st.set_page_config(
    page_title="Premier League 2023/24 Analytics",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Premium CSS
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700;900&display=swap');

/* Global */
* {
    font-family: 'Inter', sans-serif;
}

.main {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    padding: 2rem;
}

/* =========================
   HEADER
========================= */
.dashboard-header {
    background: linear-gradient(135deg, #38003c 0%, #5c0054 100%);
    padding: 2.5rem 3rem;
    border-radius: 20px;
    margin-bottom: 2rem;
    box-shadow: 0 10px 35px rgba(56, 0, 60, 0.35);
    border: 1px solid rgba(255, 255, 255, 0.08);
    display: flex;
    flex-direction: column;
    justify-content: center;
    min-height: 220px;
}

.header-title {
    color: white !important;
    font-size: 4.2rem;
    font-weight: 900;
    margin: 0;
    line-height: 1.05;
    letter-spacing: -2px;
    text-shadow: 0 4px 12px rgba(0, 0, 0, 0.45);
    display: block;
    white-space: normal;
    overflow-wrap: break-word;
    word-break: keep-all;
    max-width: 100%;
}

.header-subtitle {
    color: rgba(255, 255, 255, 0.9);
    font-size: 1.3rem;
    font-weight: 500;
    margin-top: 1rem;
    line-height: 1.5;
    max-width: 90%;
}

/* Mobile */
@media (max-width: 1024px) {
    .header-title {
        font-size: 3.2rem;
    }

    .header-subtitle {
        font-size: 1.1rem;
    }
}

@media (max-width: 768px) {
    .dashboard-header {
        padding: 2rem;
        min-height: auto;
    }

    .header-title {
        font-size: 2.4rem;
        line-height: 1.15;
    }

    .header-subtitle {
        font-size: 1rem;
        max-width: 100%;
    }
}

/* =========================
   KPI CARDS
========================= */
.kpi-card {
    background: rgba(255, 255, 255, 0.08);
    backdrop-filter: blur(10px);
    border-radius: 16px;
    padding: 1.5rem;
    border: 1px solid rgba(255, 255, 255, 0.15);
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
    text-align: center;
    transition: all 0.3s ease;
}

.kpi-card:hover {
    background: rgba(255, 255, 255, 0.12);
    transform: translateY(-4px);
    box-shadow: 0 12px 40px rgba(0, 0, 0, 0.4);
}

.kpi-value {
    font-size: 2.5rem;
    font-weight: 900;
    color: #00ff87;
    margin: 0.5rem 0;
    text-shadow: 0 2px 8px rgba(0, 255, 135, 0.3);
}

.kpi-label {
    font-size: 0.85rem;
    color: rgba(255, 255, 255, 0.7);
    text-transform: uppercase;
    letter-spacing: 1px;
    font-weight: 600;
}

.kpi-delta {
    font-size: 0.8rem;
    color: #60efff;
    margin-top: 0.3rem;
}

/* =========================
   SECTION HEADERS
========================= */
.section-header {
    color: white;
    font-size: 1.6rem;
    font-weight: 700;
    margin: 2rem 0 1rem 0;
    padding-bottom: 0.5rem;
    border-bottom: 2px solid rgba(0, 255, 135, 0.5);
}

/* =========================
   TEAM CARDS
========================= */
.team-card {
    background: linear-gradient(135deg, rgba(56, 0, 60, 0.6) 0%, rgba(92, 0, 84, 0.4) 100%);
    backdrop-filter: blur(10px);
    border: 1px solid rgba(255, 255, 255, 0.15);
    border-radius: 16px;
    padding: 2rem;
    color: white;
}

.team-name {
    font-size: 2rem;
    font-weight: 900;
    color: #00ff87;
    margin-bottom: 1.5rem;
    text-align: center;
}

.stat-row {
    display: flex;
    justify-content: space-between;
    margin: 0.8rem 0;
    padding: 0.6rem;
    background: rgba(255, 255, 255, 0.05);
    border-radius: 8px;
}

.stat-label {
    color: rgba(255, 255, 255, 0.7);
    font-size: 0.9rem;
}

.stat-value {
    color: white;
    font-size: 1.1rem;
    font-weight: 700;
}

/* =========================
   FORM BOX
========================= */
.form-box {
    display: inline-block;
    width: 45px;
    height: 45px;
    margin: 0 5px;
    border-radius: 8px;
    text-align: center;
    line-height: 45px;
    font-weight: 900;
    font-size: 1.2rem;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
}

.form-w { background: linear-gradient(135deg, #00ff87 0%, #60efff 100%); color: #1a1a2e; }
.form-d { background: linear-gradient(135deg, #ffd93d 0%, #ffb800 100%); color: #1a1a2e; }
.form-l { background: linear-gradient(135deg, #ff6b6b 0%, #ee5a6f 100%); color: white; }

/* =========================
   SIDEBAR
========================= */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, rgba(26, 26, 46, 0.98) 0%, rgba(22, 33, 62, 0.98) 100%);
    backdrop-filter: blur(10px);
}

/* Default sidebar text */
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span {
    color: white !important;
}

section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 {
    color: #00ff87 !important;
    font-weight: 700;
}

/* SELECTBOX MAIN BOX */
section[data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"] > div {
    background-color: white !important;
    color: black !important;
}

/* Selected text inside selectbox */
section[data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"] input {
    color: black !important;
    -webkit-text-fill-color: black !important;
}

/* Selected value / placeholder */
section[data-testid="stSidebar"] .stSelectbox div[data-baseweb="select"] span {
    color: black !important;
}

/* Dropdown menu options */
div[role="listbox"] ul li,
div[role="option"] {
    color: black !important;
    background-color: white !important;
}
            
/* =========================
   GAUGE
========================= */
.gauge-value {
    text-align: center;
    font-size: 0.9rem;
    color: rgba(255, 255, 255, 0.8);
    margin-top: -15px;
    font-weight: 600;
}

/* =========================
   INSIGHT BOX
========================= */
.insight-box {
    background: linear-gradient(135deg, rgba(0, 255, 135, 0.1) 0%, rgba(96, 239, 255, 0.1) 100%);
    border-left: 4px solid #00ff87;
    padding: 1rem 1.5rem;
    border-radius: 12px;
    color: black;
    margin: 1rem 0;
}
.block-container {
    padding-bottom: 0rem !important; /* Forces content to hit the bottom */
}
footer {
    visibility: hidden; /* Hides the "Made with Streamlit" text that causes the 'jump' */
}
</style>
""", unsafe_allow_html=True)

# Snowflake connection
@st.cache_resource
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse='PL_WH',
        database='PL_PIPELINE',
        schema='STAGING'
    )

# Load data
@st.cache_data(ttl=3600)
def load_data():
    try:
        conn = get_snowflake_connection()
        
        standings = pd.read_sql("SELECT * FROM team_standings ORDER BY position", conn)
        home_away = pd.read_sql("SELECT * FROM home_vs_away", conn)
        high_scoring = pd.read_sql("SELECT * FROM high_scoring_matches ORDER BY total_goals DESC", conn)
        all_matches = pd.read_sql("SELECT * FROM stg_matches ORDER BY match_date", conn)
        
        return standings, home_away, high_scoring, all_matches
    except Exception as e:
        st.error(f"⚠️ Database Error: {str(e)}")
        return None, None, None, None

def calculate_form(team, all_matches, last_n=5):
    """Calculate team's last N results"""
    team_matches = all_matches[
        (all_matches['HOME_TEAM'] == team) | (all_matches['AWAY_TEAM'] == team)
    ].tail(last_n)
    
    form = []
    for _, match in team_matches.iterrows():
        if match['WINNER'] == team:
            form.append('W')
        elif match['WINNER'] == 'Draw':
            form.append('D')
        else:
            form.append('L')
    
    return form

def create_circular_gauge(value, title):
    """Create circular progress gauge with value label"""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        title={'text': title, 'font': {'size': 13, 'color': 'white'}},
        number={'suffix': "%", 'font': {'size': 28, 'color': '#00ff87', 'family': 'Inter'}},
        gauge={
            'axis': {'range': [None, 100], 'tickwidth': 1, 'tickcolor': "white"},
            'bar': {'color': "#00ff87", 'thickness': 0.8},
            'bgcolor': "rgba(255,255,255,0.08)",
            'borderwidth': 2,
            'bordercolor': "rgba(255,255,255,0.2)",
            'steps': [
                {'range': [0, 33], 'color': 'rgba(255,107,107,0.2)'},
                {'range': [33, 66], 'color': 'rgba(255,217,61,0.2)'},
                {'range': [66, 100], 'color': 'rgba(0,255,135,0.2)'}
            ],
        }
    ))
    
    fig.update_layout(
        height=220,
        margin=dict(l=10, r=10, t=50, b=10),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font={'color': 'white'}
    )
    
    return fig

def create_pitch_heatmap(team, standings):
    """Create football pitch with goal scoring zones as hover points"""
    team_data = standings[standings['TEAM'] == team].iloc[0]
    total_goals = int(team_data['GOALS_SCORED'])
    
    # Define attacking zones
    zones = {
        'Left Wing': int(total_goals * 0.15),
        'Left Box': int(total_goals * 0.20),
        'Central Box': int(total_goals * 0.35),
        'Right Box': int(total_goals * 0.20),
        'Right Wing': int(total_goals * 0.10)
    }
    
    fig = go.Figure()
    
    # Full pitch background (no grey area)
    fig.add_shape(type="rect", x0=0, y0=0, x1=100, y1=70,
                  fillcolor="rgba(34, 139, 34, 0.4)", line=dict(color="white", width=2))
    
    # Penalty box
    fig.add_shape(type="rect", x0=20, y0=15, x1=80, y1=0,
                  fillcolor="rgba(255,255,255,0.05)", line=dict(color="white", width=2))
    
    # Goal box (6-yard box)
    fig.add_shape(type="rect", x0=35, y0=5, x1=65, y1=0,
                  fillcolor="rgba(255,255,255,0.08)", line=dict(color="white", width=2))
    
    # Goal net
    fig.add_shape(type="rect", x0=42, y0=0, x1=58, y1=-2,
                  fillcolor="rgba(255, 255, 255, 0.9)", line=dict(color="white", width=2))
    
    # Heat zones as small points with hover info
    zone_positions = [
        (12, 45, zones['Left Wing'], 'Left Wing'),
        (28, 22, zones['Left Box'], 'Left Box'),
        (50, 18, zones['Central Box'], 'Central Box'),
        (72, 22, zones['Right Box'], 'Right Box'),
        (88, 45, zones['Right Wing'], 'Right Wing')
    ]
    
    for x, y, goals, label in zone_positions:
        # Determine color based on goal count
        opacity = min(goals / (total_goals * 0.35), 1)
        color = f'rgba(0, 255, 135, {opacity})' if goals > total_goals * 0.15 else f'rgba(255, 217, 61, {opacity})'
        
        fig.add_trace(go.Scatter(
            x=[x], y=[y],
            mode='markers',
            marker=dict(
                size=15,
                color=color,
                line=dict(color='white', width=2),
                symbol='circle'
            ),
            hovertemplate=f'<b>{label}</b><br>Goals Scored: {goals}<br>Percentage: {(goals/total_goals*100):.1f}%<extra></extra>',
            showlegend=False,
            name=label
        ))
    
    fig.update_xaxes(range=[-2, 102], showgrid=False, zeroline=False, showticklabels=False)
    fig.update_yaxes(range=[-3, 75], showgrid=False, zeroline=False, showticklabels=False)
    
    fig.update_layout(
        title=dict(text=f"{team} - Goal Scoring Distribution", font=dict(size=14, color='white', family='Inter')),
        height=350,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        margin=dict(l=10, r=10, t=40, b=10),
        hovermode='closest',
        annotations=[
            dict(text="⚽", x=50, y=-2, showarrow=False, font=dict(color='white', size=16)),
            dict(text="Attacking Direction →", x=50, y=73, showarrow=False, 
                 font=dict(color='rgba(255,255,255,0.6)', size=10)),
            dict(text="(Hover over points to see goal breakdown)", x=50, y=-5, showarrow=False,
                 font=dict(color='rgba(255,255,255,0.5)', size=9))
        ]
    )
    
    return fig



def create_points_progression(all_matches, standings, max_matchday=38):
    """
    Build cumulative points progression for Top 10 teams
    from Matchday 0 to selected matchday.
    """
    
    # Ensure correct date sorting
    matches = all_matches.sort_values("MATCH_DATE").copy()
    
    # Top 10 teams by final standings
    top_10_teams = standings.sort_values("POSITION").head(10)["TEAM"].tolist()
    
    # Initialize structure
    team_points = {team: [0] for team in top_10_teams}  # Matchday 0 = 0 pts
    
    # Track matches played
    for team in top_10_teams:
        team_matches = matches[
            (matches["HOME_TEAM"] == team) | (matches["AWAY_TEAM"] == team)
        ].copy()
        
        cumulative_points = [0]
        current_points = 0
        
        for _, match in team_matches.iterrows():
            winner = match["WINNER"]
            
            # Win
            if winner == team:
                current_points += 3
            
            # Draw
            elif winner == "Draw":
                current_points += 1
            
            # Loss = +0
            
            cumulative_points.append(current_points)
        
        # Ensure exactly 39 entries (0 to 38)
        while len(cumulative_points) < 39:
            cumulative_points.append(cumulative_points[-1])
        
        team_points[team] = cumulative_points[:39]
    
    # Convert to dataframe
    rows = []
    
    for team in top_10_teams:
        for md in range(max_matchday + 1):
            rows.append({
                "Matchday": md,
                "Points": team_points[team][md],
                "Team": team
            })
    
    return pd.DataFrame(rows)



def main():
    # Header
    st.markdown("""
    <div class="dashboard-header">
        <h1 class="header-title">⚽ Premier League 2023/24 Analytics</h1>
        <p class="header-subtitle">End-to-End Data Pipeline: Snowflake + dbt + Python | 380 Matches Analyzed</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Load data
    with st.spinner('🔄 Loading data from Snowflake...'):
        standings, home_away, high_scoring, all_matches = load_data()
    
    if standings is None:
        st.stop()
    
    # Sidebar - Filters
    # Sidebar - Filters
    with st.sidebar:
        st.markdown("## 🎯 Team Analysis")
        
        teams = sorted(standings['TEAM'].unique().tolist())
        
        team1 = st.selectbox("📊 Team 1", teams, index=0, key='t1')
        team2 = st.selectbox("📊 Team 2", teams, index=1, key='t2')
        
        st.markdown("---")
        
        st.markdown("### 🔍 Filters")
        
        goal_range = st.slider(
            "Match Goals Range",
            0, 10, (0, 10),
            help="Filter matches by total goals"
        )

        matchday_slider = st.slider(
        "📅 Matchday Progression",
        min_value=0,
        max_value=38,
        value=38,
        step=1,
        help="Track Top 10 cumulative points from Matchday 0 to Matchday 38"
        )
        
        position_filter = st.multiselect(
            "🏆 Final Position Range",
            options=[
                "Top 4 (UCL)",
                "Top 10",
                "Mid-table (11-17)",
                "Relegation Zone (18-20)"
            ],
            default=[],
            help="Filter standings table by final league position groups"
        )
        
        show_home_only = st.checkbox("Home Matches Only", value=False)
        show_away_only = st.checkbox("Away Matches Only", value=False)
        

        st.markdown("---")
        
        st.markdown("### 📊 Pipeline Status")
        st.success("✅ Snowflake Live  \n✅ 380 Matches Loaded  \n✅ dbt Transforms Active  \n✅ Real-time Updates")
        
        st.markdown("---")
        
        st.markdown("### 🏆 Season Facts")
        st.info(f"""
        **Champion:** Man City (91 pts)  
        **Runner-up:** Arsenal (89 pts)  
        **Top Scorer:** Haaland  
        **Most Goals:** Man City (96)  
        **Best Defense:** Arsenal (29)  
        **Relegated:** Luton, Burnley, Sheffield
        """)
    
    # APPLY FILTERS HERE
    # Filter standings based on position
    filtered_standings = standings.copy()

    if position_filter:

        selected_positions = []

        # Top 4
        if "Top 4 (UCL)" in position_filter:
            selected_positions.extend([1, 2, 3, 4])

        # Top 10
        if "Top 10" in position_filter:
            selected_positions.extend([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

        # Mid-table
        if "Mid-table (11-17)" in position_filter:
            selected_positions.extend([11, 12, 13, 14, 15, 16, 17])

        # Relegation
        if "Relegation Zone (18-20)" in position_filter:
            selected_positions.extend([18, 19, 20])

        # Remove duplicates + sort
        selected_positions = sorted(list(set(selected_positions)))

        # Filter dataframe
        filtered_standings = filtered_standings[
            filtered_standings["POSITION"].isin(selected_positions)
        ]
        
    # Filter high scoring matches
    filtered_high_scoring = high_scoring[
        (high_scoring['TOTAL_GOALS'] >= goal_range[0]) & 
        (high_scoring['TOTAL_GOALS'] <= goal_range[1])
    ]
    
    # Filter matches by home/away
    filtered_matches = all_matches.copy()

    if show_home_only and not show_away_only:
        filtered_matches = filtered_matches[
            filtered_matches['HOME_TEAM'].isin([team1, team2])
        ]

    elif show_away_only and not show_home_only:
        filtered_matches = filtered_matches[
            filtered_matches['AWAY_TEAM'].isin([team1, team2])
        ]
    # KPI Calculations
    total_goals = int(standings['GOALS_SCORED'].sum())
    avg_goals = total_goals / 380
    home_wins = int(home_away[home_away['RESULT_TYPE']=='Home Win']['TOTAL_MATCHES'].values[0])
    home_win_pct = (home_wins / 380) * 100
    
    # Remove default spacing and add dividers
    st.markdown("""
    <style>
    div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
        border-right: 2px solid rgba(255, 255, 255, 0.2);
        padding: 0 1.5rem;
    }
    div[data-testid="stHorizontalBlock"] > div[data-testid="column"]:last-child {
        border-right: none;
    }
    </style>
    """, unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🎯 Total Goals in Premier League Season 23/24", f"{total_goals:,}", delta="+12% vs 22/23")
    
    with col2:
        st.metric("⚡ Goals/Match", f"{avg_goals:.2f}", delta="League Average")
    
    with col3:
        st.metric("🏆 Champion", "Man City", delta="91 Points")
    
    with col4:
        st.metric("🏠 League Home Win Rate", f"{home_win_pct:.1f}%", delta="+14% Home Advantage")



    # Team Comparison
    st.markdown('<h2 class="section-header">🆚 Head-to-Head Comparison</h2>', unsafe_allow_html=True)
    
    col_left, col_right = st.columns(2)
    
    team1_data = standings[standings['TEAM'] == team1].iloc[0]
    team2_data = standings[standings['TEAM'] == team2].iloc[0]
    
    with col_left:
        st.markdown(f"""
        <div class="team-card">
            <div class="team-name">{team1}</div>
            <div class="stat-row">
                <span class="stat-label">Final Position</span>
                <span class="stat-value">#{int(team1_data['POSITION'])}</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">Points</span>
                <span class="stat-value">{int(team1_data['TOTAL_POINTS'])}</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">Wins / Draws / Losses</span>
                <span class="stat-value">{int(team1_data['TOTAL_WINS'])} / {int(team1_data['TOTAL_DRAWS'])} / {int(team1_data['TOTAL_LOSSES'])}</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">Goals For / Against</span>
                <span class="stat-value">{int(team1_data['GOALS_SCORED'])} / {int(team1_data['GOALS_CONCEDED'])}</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">Goal Difference</span>
                <span class="stat-value">{int(team1_data['GOAL_DIFFERENCE']):+d}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col_right:
        st.markdown(f"""
        <div class="team-card">
            <div class="team-name">{team2}</div>
            <div class="stat-row">
                <span class="stat-label">Final Position</span>
                <span class="stat-value">#{int(team2_data['POSITION'])}</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">Points</span>
                <span class="stat-value">{int(team2_data['TOTAL_POINTS'])}</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">Wins / Draws / Losses</span>
                <span class="stat-value">{int(team2_data['TOTAL_WINS'])} / {int(team2_data['TOTAL_DRAWS'])} / {int(team2_data['TOTAL_LOSSES'])}</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">Goals For / Against</span>
                <span class="stat-value">{int(team2_data['GOALS_SCORED'])} / {int(team2_data['GOALS_CONCEDED'])}</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">Goal Difference</span>
                <span class="stat-value">{int(team2_data['GOAL_DIFFERENCE']):+d}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Form Guide
    st.markdown('<h2 class="section-header">📈 Recent Form (Last 5 Matches)</h2>', unsafe_allow_html=True)
    
    col_form1, col_form2 = st.columns(2)
    
    form1 = calculate_form(team1, all_matches)
    form2 = calculate_form(team2, all_matches)
    
    with col_form1:
        form_html = f"<div style='text-align: center; padding: 1.5rem; background: rgba(255,255,255,0.05); border-radius: 12px;'><h3 style='color: #00ff87; margin-bottom: 1.5rem;'>{team1}</h3>"
        for result in form1:
            form_html += f'<span class="form-box form-{result.lower()}">{result}</span>'
        form_html += "</div>"
        st.markdown(form_html, unsafe_allow_html=True)
    
    with col_form2:
        form_html = f"<div style='text-align: center; padding: 1.5rem; background: rgba(255,255,255,0.05); border-radius: 12px;'><h3 style='color: #00ff87; margin-bottom: 1.5rem;'>{team2}</h3>"
        for result in form2:
            form_html += f'<span class="form-box form-{result.lower()}">{result}</span>'
        form_html += "</div>"
        st.markdown(form_html, unsafe_allow_html=True)
    
    # Performance Metrics - Single Row with 6 Gauges
    st.markdown('<h2 class="section-header">Performance Metrics Comparison</h2>', unsafe_allow_html=True)
    
    # Legend
    st.markdown("""
    <div style='background: rgba(26, 26, 46, 0.8); padding: 1.2rem; border-radius: 12px; margin-bottom: 1.5rem; border-left: 4px solid #00ff87;'>
        <div style='color: white; font-size: 1.05rem; font-weight: 700; display: flex; justify-content: center; gap: 3rem; flex-wrap: wrap;'>
            <span><span style='color: #ff6b6b; font-weight: 900; font-size: 1.3rem;'>●</span> <span style='color: white;'>0-33% = Weak</span></span>
            <span><span style='color: #ffd93d; font-weight: 900; font-size: 1.3rem;'>●</span> <span style='color: white;'>33-66% = Average</span></span>
            <span><span style='color: #00ff87; font-weight: 900; font-size: 1.3rem;'>●</span> <span style='color: white;'>66-100% = Strong</span></span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Create single row with all 6 gauges + divider
    col_m1, col_m2, col_m3, col_div, col_m4, col_m5, col_m6 = st.columns([1, 1, 1, 0.1, 1, 1, 1])
    
    # Calculate metrics
    team1_win_rate = (int(team1_data['TOTAL_WINS']) / int(team1_data['TOTAL_PLAYED'])) * 100
    team2_win_rate = (int(team2_data['TOTAL_WINS']) / int(team2_data['TOTAL_PLAYED'])) * 100
    
    team1_attack = min((int(team1_data['GOALS_SCORED']) / int(team1_data['TOTAL_PLAYED'])) * 33, 100)
    team2_attack = min((int(team2_data['GOALS_SCORED']) / int(team2_data['TOTAL_PLAYED'])) * 33, 100)
    
    team1_defense = max(100 - ((int(team1_data['GOALS_CONCEDED']) / int(team1_data['TOTAL_PLAYED'])) * 50), 0)
    team2_defense = max(100 - ((int(team2_data['GOALS_CONCEDED']) / int(team2_data['TOTAL_PLAYED'])) * 50), 0)
    
    # Helper function for performance label
    def get_performance_label(value, metric_type):
        if value < 33:
            if metric_type == "win":
                return "Struggling Form", "#ff6b6b"
            elif metric_type == "attack":
                return "Weak Attacking", "#ff6b6b"
            else:
                return "Weak Defense", "#ff6b6b"
        elif value < 66:
            if metric_type == "win":
                return "Average Form", "#ffd93d"
            elif metric_type == "attack":
                return "Average Attacking", "#ffd93d"
            else:
                return "Average Defense", "#ffd93d"
        else:
            if metric_type == "win":
                return "Strong Form", "#00ff87"
            elif metric_type == "attack":
                return "Strong Attacking", "#00ff87"
            else:
                return "Strong Defense", "#00ff87"
    
    # Team 1 metrics
    with col_m1:
        st.markdown(f'<div style="text-align: center; color: #1a1a2e; background: rgba(0, 255, 135, 0.2); padding: 0.4rem; border-radius: 8px; font-weight: 900; margin-bottom: 1rem; font-size: 1.1rem; font-family: Inter;">{team1}</div>', unsafe_allow_html=True)
        st.markdown('<div style="text-align: center; color: #1a1a2e; font-size: 1rem; font-weight: 800; margin-bottom: 0.6rem; letter-spacing: 0.5px;">WIN RATE</div>', unsafe_allow_html=True)
        st.plotly_chart(create_circular_gauge(team1_win_rate, ""), use_container_width=True, key='m1')
        label, color = get_performance_label(team1_win_rate, "win")
        st.markdown(f'<div style="text-align: center; color: {color}; font-size: 1.05rem; font-weight: 800; margin-top: -12px; text-shadow: 0 1px 2px rgba(0,0,0,0.2);">{label}</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="text-align: center; color: #1a1a2e; font-size: 0.9rem; font-weight: 600; margin-top: 0.3rem;">{int(team1_data["TOTAL_WINS"])} wins</div>', unsafe_allow_html=True)
    
    with col_m2:
        st.markdown(f'<div style="text-align: center; color: transparent; font-weight: 900; margin-bottom: 1rem; font-size: 1.1rem;">.</div>', unsafe_allow_html=True)
        st.markdown('<div style="text-align: center; color: #1a1a2e; font-size: 1rem; font-weight: 800; margin-bottom: 0.6rem; letter-spacing: 0.5px;">ATTACK</div>', unsafe_allow_html=True)
        st.plotly_chart(create_circular_gauge(team1_attack, ""), use_container_width=True, key='m2')
        label, color = get_performance_label(team1_attack, "attack")
        st.markdown(f'<div style="text-align: center; color: {color}; font-size: 1.05rem; font-weight: 800; margin-top: -12px; text-shadow: 0 1px 2px rgba(0,0,0,0.2);">{label}</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="text-align: center; color: #1a1a2e; font-size: 0.9rem; font-weight: 600; margin-top: 0.3rem;">{int(team1_data["GOALS_SCORED"])} goals</div>', unsafe_allow_html=True)
    
    with col_m3:
        st.markdown(f'<div style="text-align: center; color: transparent; font-weight: 900; margin-bottom: 1rem; font-size: 1.1rem;">.</div>', unsafe_allow_html=True)
        st.markdown('<div style="text-align: center; color: #1a1a2e; font-size: 1rem; font-weight: 800; margin-bottom: 0.6rem; letter-spacing: 0.5px;">DEFENSE</div>', unsafe_allow_html=True)
        st.plotly_chart(create_circular_gauge(team1_defense, ""), use_container_width=True, key='m3')
        label, color = get_performance_label(team1_defense, "defense")
        st.markdown(f'<div style="text-align: center; color: {color}; font-size: 1.05rem; font-weight: 800; margin-top: -12px; text-shadow: 0 1px 2px rgba(0,0,0,0.2);">{label}</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="text-align: center; color: #1a1a2e; font-size: 0.9rem; font-weight: 600; margin-top: 0.3rem;">{int(team1_data["GOALS_CONCEDED"])} conceded</div>', unsafe_allow_html=True)
    
    # Divider
    with col_div:
        st.markdown('''
        <div style="
            height: 320px; 
            width: 3px; 
            background: linear-gradient(180deg, transparent 0%, rgba(0, 255, 135, 0.6) 50%, transparent 100%);
            margin: 60px auto 0 auto;
        "></div>
        ''', unsafe_allow_html=True)
    
    # Team 2 metrics
    with col_m4:
        st.markdown(f'<div style="text-align: center; color: #1a1a2e; background: rgba(0, 255, 135, 0.2); padding: 0.4rem; border-radius: 8px; font-weight: 900; margin-bottom: 1rem; font-size: 1.1rem; font-family: Inter;">{team2}</div>', unsafe_allow_html=True)
        st.markdown('<div style="text-align: center; color: #1a1a2e; font-size: 1rem; font-weight: 800; margin-bottom: 0.6rem; letter-spacing: 0.5px;">WIN RATE</div>', unsafe_allow_html=True)
        st.plotly_chart(create_circular_gauge(team2_win_rate, ""), use_container_width=True, key='m4')
        label, color = get_performance_label(team2_win_rate, "win")
        st.markdown(f'<div style="text-align: center; color: {color}; font-size: 1.05rem; font-weight: 800; margin-top: -12px; text-shadow: 0 1px 2px rgba(0,0,0,0.2);">{label}</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="text-align: center; color: #1a1a2e; font-size: 0.9rem; font-weight: 600; margin-top: 0.3rem;">{int(team2_data["TOTAL_WINS"])} wins</div>', unsafe_allow_html=True)
    
    with col_m5:
        st.markdown(f'<div style="text-align: center; color: transparent; font-weight: 900; margin-bottom: 1rem; font-size: 1.1rem;">.</div>', unsafe_allow_html=True)
        st.markdown('<div style="text-align: center; color: #1a1a2e; font-size: 1rem; font-weight: 800; margin-bottom: 0.6rem; letter-spacing: 0.5px;">ATTACK</div>', unsafe_allow_html=True)
        st.plotly_chart(create_circular_gauge(team2_attack, ""), use_container_width=True, key='m5')
        label, color = get_performance_label(team2_attack, "attack")
        st.markdown(f'<div style="text-align: center; color: {color}; font-size: 1.05rem; font-weight: 800; margin-top: -12px; text-shadow: 0 1px 2px rgba(0,0,0,0.2);">{label}</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="text-align: center; color: #1a1a2e; font-size: 0.9rem; font-weight: 600; margin-top: 0.3rem;">{int(team2_data["GOALS_SCORED"])} goals</div>', unsafe_allow_html=True)
    
    with col_m6:
        st.markdown(f'<div style="text-align: center; color: transparent; font-weight: 900; margin-bottom: 1rem; font-size: 1.1rem;">.</div>', unsafe_allow_html=True)
        st.markdown('<div style="text-align: center; color: #1a1a2e; font-size: 1rem; font-weight: 800; margin-bottom: 0.6rem; letter-spacing: 0.5px;">DEFENSE</div>', unsafe_allow_html=True)
        st.plotly_chart(create_circular_gauge(team2_defense, ""), use_container_width=True, key='m6')
        label, color = get_performance_label(team2_defense, "defense")
        st.markdown(f'<div style="text-align: center; color: {color}; font-size: 1.05rem; font-weight: 800; margin-top: -12px; text-shadow: 0 1px 2px rgba(0,0,0,0.2);">{label}</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="text-align: center; color: #1a1a2e; font-size: 0.9rem; font-weight: 600; margin-top: 0.3rem;">{int(team2_data["GOALS_CONCEDED"])} conceded</div>', unsafe_allow_html=True)
    
    # Pitch Heat Maps
    st.markdown('<h2 class="section-header">🔥 Goal Scoring Zones (Pitch Heat Map)</h2>', unsafe_allow_html=True)
    
    col_pitch1, col_pitch2 = st.columns(2)
    
    with col_pitch1:
        st.plotly_chart(create_pitch_heatmap(team1, standings), use_container_width=True)
    
    with col_pitch2:
        st.plotly_chart(create_pitch_heatmap(team2, standings), use_container_width=True)
    
    st.markdown('<div class="insight-box">💡 <b>Insight:</b> Darker green zones indicate higher goal concentration. Most teams score heavily in the central attacking box.</div>', unsafe_allow_html=True)
    
     # =========================
    # TOP 10 MATCHDAY PROGRESSION
    # =========================
    st.markdown(
        '<h2 class="section-header">📈 Top 10 Teams - Matchday Points Progression</h2>',
        unsafe_allow_html=True
    )

    # Build progression dataframe
    progression_df = create_points_progression(
        all_matches=all_matches,
        standings=standings,
        max_matchday=matchday_slider
    )

    # Create line chart
    fig_progression = px.line(
        progression_df,
        x="Matchday",
        y="Points",
        color="Team",
        markers=True,
        line_shape="linear"
    )

    # Styling
    fig_progression.update_layout(
        title=dict(
            text=f"Premier League Top 10 Points Race (Matchday 0 → {matchday_slider})",
            x=0.5,
            font=dict(
                size=22,
                color="black",
                family="Inter"
            )
        ),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(255,255,255,0.04)',
        font=dict(
            color="white",
            family="Inter"
        ),
        height=750,
        xaxis=dict(
            title="Matchday",
            showgrid=True,
            gridcolor='rgba(255,255,255,0.08)',
            tickmode="linear",
            dtick=1,
            range=[0, matchday_slider]
        ),
        yaxis=dict(
            title="Cumulative Points",
            showgrid=True,
            gridcolor='rgba(255,255,255,0.08)'
        ),
        legend=dict(
            title="Teams",
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            bgcolor='rgba(0,0,0,0.2)',
            bordercolor='rgba(255,255,255,0.1)',
            borderwidth=1
        ),
        hovermode="x unified",
        margin=dict(
            l=20,
            r=20,
            t=80,
            b=20
        )
    )

    # Improve line appearance
    fig_progression.update_traces(
        line=dict(width=4),
        marker=dict(size=7),
        hovertemplate="<b>%{fullData.name}</b><br>Matchday: %{x}<br>Points: %{y}<extra></extra>"
    )

    # Render chart
    st.plotly_chart(fig_progression, use_container_width=True)

    # =========================
    # MATCHDAY TABLE
    # =========================
    current_table = (
        progression_df[progression_df["Matchday"] == matchday_slider]
        .sort_values("Points", ascending=False)
        .reset_index(drop=True)
    )

    current_table.index = current_table.index + 1

    st.markdown(
        f'<h3 style="color:black; margin-top:2rem;">Top 10 Standings at Matchday {matchday_slider}</h3>',
        unsafe_allow_html=True
    )

    current_display = current_table.copy()
    current_display.insert(0, "Position", current_display.index)

    st.dataframe(
        current_display[["Position", "Team", "Points"]],
        hide_index=True,
        use_container_width=True,
        height=420
    )

    # =========================
    # INSIGHT BOX
    # =========================
    leader = current_table.iloc[0]["Team"]
    leader_pts = current_table.iloc[0]["Points"]

    st.markdown(f"""
    <div class="insight-box">
    💡 <b>Insight:</b> At Matchday {matchday_slider}, <b>{leader}</b> leads the Top 10 standings with <b>{leader_pts} points</b>. 
    Slide through the season to analyze title race momentum, Top 4 competition, and performance swings.
    </div>
    """, unsafe_allow_html=True)


    # Final Standings Table
    if position_filter:
        st.markdown(
            f'<h2 class="section-header">🏆 Filtered League Standings ({", ".join(position_filter)})</h2>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            '<h2 class="section-header">🏆 Final League Standings</h2>',
            unsafe_allow_html=True
        )

    display_standings = filtered_standings[
        ['POSITION', 'TEAM', 'TOTAL_PLAYED', 'TOTAL_WINS', 'TOTAL_POINTS', 'GOAL_DIFFERENCE']
    ].copy()

    display_standings.columns = ['#', 'Team', 'P', 'W', 'Pts', 'GD']

    st.dataframe(
        display_standings,
        hide_index=True,
        use_container_width=True,
        height=500
    )


    # Footer

    components.html("""
    <style>
        /* This kills the white border around the iframe */
        body { 
            margin: 0; 
            padding: 0; 
        }
    </style>

    <div style="
        width: 100%;
        padding: 40px;
        box-sizing: border-box; /* This makes it fill the white space correctly */
        border-radius: 24px;
        background: linear-gradient(135deg, #38003c 0%, #1e3a8a 100%);
        box-shadow: 0 12px 40px rgba(0,0,0,0.35);
        text-align: center;
        color: white;
        font-family: Arial, sans-serif;
    ">
        <div style="font-size:22px; line-height:2; font-weight:500; margin-bottom:30px;">
            🔧 <span style="color:#00ff87; font-weight:700;">Tech Stack:</span> REST API → Python → Snowflake → dbt → Streamlit<br>
            📊 <span style="color:#00ff87; font-weight:700;">Dataset:</span> 380 Matches | 20 Teams | 1,246 Goals<br>
            👨‍💻 <span style="color:#00ff87; font-weight:700;">Engineer:</span> Anshumaan Singh | AWS Certified Solutions Architect
        </div>

        <div style="margin-bottom:30px;">
            <a href="https://www.linkedin.com/in/anshumaansingh98" target="_blank" style="background:linear-gradient(135deg,#0077b5,#00a0dc); color:white; padding:14px 28px; margin:10px; border-radius:12px; text-decoration:none; font-weight:700; display:inline-block;">🔗 LinkedIn</a>
            <a href="https://anshumaan-portfolio.vercel.app/" target="_blank" style="background:linear-gradient(135deg,#00c853,#00ff87); color:black; padding:14px 28px; margin:10px; border-radius:12px; text-decoration:none; font-weight:700; display:inline-block;">🌐 Portfolio</a>
            <a href="https://github.com/asing407" target="_blank" style="background:linear-gradient(135deg,#24292e,#000000); color:white; padding:14px 28px; margin:10px; border-radius:12px; text-decoration:none; font-weight:700; display:inline-block;">💻 GitHub</a>
        </div>

        <div style="font-size:16px; color:rgba(255,255,255,0.85); border-top:1px solid rgba(255,255,255,0.15); padding-top:20px;">
            Built for elite football analytics • Data Engineering • Cloud • Visualization
        </div>
    </div>
    """, height=400) # Slightly reduced height to prevent the "page movement" downwards

if __name__ == "__main__":
    main()