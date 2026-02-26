"""
Retail Chain Data Engineering – Streamlit Analytics Dashboard
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import mock_data as md
from db import run_query, USE_MOCK, KPI_SUMMARY_SQL, MONTHLY_TREND_SQL

# ── Page config ─────────────────────────────────────────────
st.set_page_config(
    page_title="Retail Chain – Data Engineering Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ──────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0f1117; }
    .metric-card {
        background: linear-gradient(135deg, #1e2130 0%, #252a3a 100%);
        border: 1px solid #2d3250;
        border-radius: 12px;
        padding: 20px 24px;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    }
    .metric-value { font-size: 2rem; font-weight: 700; color: #4fc3f7; margin: 4px 0; }
    .metric-label { font-size: 0.8rem; color: #8892b0; text-transform: uppercase; letter-spacing: 1px; }
    .metric-delta { font-size: 0.85rem; color: #64ffda; }
    .section-header {
        font-size: 1.1rem; font-weight: 600; color: #ccd6f6;
        border-left: 4px solid #4fc3f7; padding-left: 12px;
        margin: 24px 0 12px 0;
    }
    .stTabs [data-baseweb="tab"] { background: #1e2130; border-radius: 8px 8px 0 0; }
    .stTabs [aria-selected="true"] { background: #252a3a; border-bottom: 2px solid #4fc3f7; }
</style>
""", unsafe_allow_html=True)

# ── Plotly theme ─────────────────────────────────────────────
PLOTLY_THEME = dict(
    template   = "plotly_dark",
    paper_bgcolor = "#0f1117",
    plot_bgcolor  = "#0f1117",
    font_color    = "#ccd6f6",
)
PALETTE = px.colors.qualitative.Set2

# ── Sidebar ──────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🛒 Retail Chain DW")
    st.markdown("---")
    if USE_MOCK:
        st.warning("Running with **mock data**. Set `USE_MOCK_DATA=false` and Snowflake credentials to use live data.")
    else:
        st.success("Connected to **Snowflake**")

    st.markdown("### Navigation")
    page = st.radio(
        "Select View",
        ["Executive Summary", "Sales Trends", "Store Performance",
         "Product Analytics", "Customer Insights", "Inventory Health",
         "Architecture & Pipeline"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.markdown("### Filters")
    year_filter = st.multiselect("Year", [2023, 2024], default=[2023, 2024])
    region_filter = st.multiselect("Region", md.REGIONS, default=md.REGIONS)
    st.markdown("---")
    st.caption("Tech Stack: Snowflake · Python · Streamlit")
    st.caption("Architecture: Stage → Clean → Consumption")


# ── Data loading helpers ─────────────────────────────────────
@st.cache_data(ttl=300)
def load_summary():
    df = run_query(KPI_SUMMARY_SQL)
    return md.get_kpi_summary() if df is None else df.iloc[0].to_dict()

@st.cache_data(ttl=300)
def load_monthly():
    df = run_query(MONTHLY_TREND_SQL)
    return md.get_monthly_trend() if df is None else df

@st.cache_data(ttl=300)
def load_stores():    return md.get_store_performance()
@st.cache_data(ttl=300)
def load_products():  return md.get_top_products(20)
@st.cache_data(ttl=300)
def load_segments():  return md.get_customer_segments()
@st.cache_data(ttl=300)
def load_top_customers(): return md.get_top_customers()
@st.cache_data(ttl=300)
def load_pay_channel(): return md.get_payment_channel_mix()
@st.cache_data(ttl=300)
def load_categories(): return md.get_category_performance()
@st.cache_data(ttl=300)
def load_inventory(): return md.get_inventory_health()
@st.cache_data(ttl=300)
def load_returns():   return md.get_return_analysis()
@st.cache_data(ttl=300)
def load_yoy():       return md.get_yoy_comparison()
@st.cache_data(ttl=300)
def load_regional():  return md.get_regional_quarterly()

summary  = load_summary()
monthly  = load_monthly()
stores   = load_stores()
products = load_products()
segments = load_segments()
cats     = load_categories()
inv      = load_inventory()
returns  = load_returns()
yoy      = load_yoy()
regional = load_regional()
pay_ch   = load_pay_channel()
top_cust = load_top_customers()


def fmt_currency(v):
    if v >= 1_000_000: return f"${v/1_000_000:.2f}M"
    if v >= 1_000:     return f"${v/1_000:.1f}K"
    return f"${v:,.2f}"

def fmt_num(v):
    if v >= 1_000_000: return f"{v/1_000_000:.1f}M"
    if v >= 1_000:     return f"{v/1_000:.1f}K"
    return f"{v:,.0f}"

def metric_card(col, label, value, delta=None, prefix='', suffix=''):
    delta_html = f'<div class="metric-delta">▲ {delta}</div>' if delta else ''
    col.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{prefix}{value}{suffix}</div>
            {delta_html}
        </div>
    """, unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# PAGE: Executive Summary
# ════════════════════════════════════════════════════════════
if page == "Executive Summary":
    st.title("Executive Summary Dashboard")
    st.caption("Retail Chain – End-to-End Data Engineering KPIs")

    # KPI Row 1
    c1, c2, c3, c4, c5 = st.columns(5)
    metric_card(c1, "Net Revenue",           fmt_currency(summary['net_revenue']))
    metric_card(c2, "Gross Profit",          fmt_currency(summary['gross_profit']))
    metric_card(c3, "Gross Margin",          f"{summary['gross_margin_pct']}%")
    metric_card(c4, "Total Transactions",    fmt_num(summary['total_transactions']))
    metric_card(c5, "Unique Customers",      fmt_num(summary['unique_customers']))

    st.markdown("")
    c6, c7, c8, c9, c10 = st.columns(5)
    metric_card(c6, "Units Sold",        fmt_num(summary['units_sold']))
    metric_card(c7, "Avg Order Value",   fmt_currency(summary['avg_transaction_value']))
    metric_card(c8, "Total COGS",        fmt_currency(summary['total_cogs']))
    metric_card(c9, "Total Discounts",   fmt_currency(summary['total_discounts']))
    metric_card(c10,"Return Rate",       f"{summary.get('return_rate_pct', 4.2)}%")

    st.markdown("---")

    # Monthly Revenue vs Profit
    col1, col2 = st.columns([3, 2])
    with col1:
        st.markdown('<div class="section-header">Monthly Revenue & Gross Profit</div>', unsafe_allow_html=True)
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(
            x=monthly['year_month'], y=monthly['net_revenue'],
            name='Net Revenue', marker_color='#4fc3f7', opacity=0.85), secondary_y=False)
        fig.add_trace(go.Scatter(
            x=monthly['year_month'], y=monthly['gross_profit'],
            name='Gross Profit', line=dict(color='#64ffda', width=2.5)), secondary_y=True)
        fig.update_layout(height=350, **PLOTLY_THEME, legend=dict(orientation='h', y=1.1),
                          margin=dict(l=0,r=0,t=20,b=0))
        fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
        fig.update_yaxes(title_text="Profit ($)", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">Revenue by Category</div>', unsafe_allow_html=True)
        fig2 = px.pie(cats, values='net_revenue', names='category_name',
                      color_discrete_sequence=PALETTE, hole=0.4)
        fig2.update_layout(height=350, **PLOTLY_THEME, showlegend=True,
                           legend=dict(orientation='h', y=-0.1),
                           margin=dict(l=0,r=0,t=10,b=40))
        st.plotly_chart(fig2, use_container_width=True)

    # YoY Comparison
    col3, col4 = st.columns([2, 3])
    with col3:
        st.markdown('<div class="section-header">Year-over-Year Revenue</div>', unsafe_allow_html=True)
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            x=yoy['year_number'].astype(str), y=yoy['net_revenue'],
            marker_color=['#4fc3f7' if y < 2024 else '#64ffda' for y in yoy['year_number']],
            text=[fmt_currency(v) for v in yoy['net_revenue']], textposition='outside'))
        fig3.update_layout(height=300, **PLOTLY_THEME, showlegend=False,
                           margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        st.markdown('<div class="section-header">Top 5 Stores by Revenue</div>', unsafe_allow_html=True)
        top5 = stores.head(5)
        fig4 = px.bar(top5, x='net_revenue', y='store_name', orientation='h',
                      color='region', color_discrete_sequence=PALETTE,
                      text=[fmt_currency(v) for v in top5['net_revenue']])
        fig4.update_traces(textposition='outside')
        fig4.update_layout(height=300, **PLOTLY_THEME,
                           yaxis={'categoryorder': 'total ascending'},
                           margin=dict(l=0,r=0,t=10,b=0), showlegend=True)
        st.plotly_chart(fig4, use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE: Sales Trends
# ════════════════════════════════════════════════════════════
elif page == "Sales Trends":
    st.title("Sales Trends Analysis")

    tab1, tab2, tab3 = st.tabs(["Monthly Trends", "Channel & Payment Mix", "Returns Analysis"])

    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="section-header">Monthly Revenue with MoM Growth %</div>', unsafe_allow_html=True)
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(x=monthly['year_month'], y=monthly['net_revenue'],
                                 name='Net Revenue', marker_color='#4fc3f7', opacity=0.8), secondary_y=False)
            fig.add_trace(go.Scatter(x=monthly['year_month'], y=monthly['mom_growth_pct'],
                                     name='MoM Growth %', line=dict(color='#ff6b6b', width=2),
                                     mode='lines+markers'), secondary_y=True)
            fig.update_layout(height=360, **PLOTLY_THEME, margin=dict(l=0,r=0,t=20,b=0))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown('<div class="section-header">Monthly Transactions & Customers</div>', unsafe_allow_html=True)
            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(x=monthly['year_month'], y=monthly['transactions'],
                                      fill='tozeroy', name='Transactions',
                                      line=dict(color='#4fc3f7', width=2)))
            fig2.add_trace(go.Scatter(x=monthly['year_month'], y=monthly['unique_customers'],
                                      fill='tozeroy', name='Unique Customers',
                                      line=dict(color='#64ffda', width=2)))
            fig2.update_layout(height=360, **PLOTLY_THEME, margin=dict(l=0,r=0,t=20,b=0))
            st.plotly_chart(fig2, use_container_width=True)

        st.markdown('<div class="section-header">Units Sold & Avg Basket Size</div>', unsafe_allow_html=True)
        fig3 = make_subplots(specs=[[{"secondary_y": True}]])
        fig3.add_trace(go.Bar(x=monthly['year_month'], y=monthly['units_sold'],
                              name='Units Sold', marker_color='#a78bfa', opacity=0.7), secondary_y=False)
        fig3.add_trace(go.Scatter(x=monthly['year_month'], y=monthly['avg_basket_size'],
                                  name='Avg Basket ($)', line=dict(color='#fbbf24', width=2)), secondary_y=True)
        fig3.update_layout(height=300, **PLOTLY_THEME, margin=dict(l=0,r=0,t=20,b=0))
        st.plotly_chart(fig3, use_container_width=True)

    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="section-header">Revenue by Sales Channel</div>', unsafe_allow_html=True)
            ch_grp = pay_ch.groupby('channel_name')['net_revenue'].sum().reset_index()
            fig = px.pie(ch_grp, values='net_revenue', names='channel_name',
                         color_discrete_sequence=PALETTE, hole=0.35)
            fig.update_layout(height=340, **PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown('<div class="section-header">Revenue by Payment Method</div>', unsafe_allow_html=True)
            pm_grp = pay_ch.groupby('payment_method')['net_revenue'].sum().reset_index()
            fig2 = px.bar(pm_grp, x='payment_method', y='net_revenue',
                          color='payment_method', color_discrete_sequence=PALETTE,
                          text=[fmt_currency(v) for v in pm_grp['net_revenue']])
            fig2.update_traces(textposition='outside')
            fig2.update_layout(height=340, **PLOTLY_THEME, showlegend=False,
                               margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig2, use_container_width=True)

        st.markdown('<div class="section-header">Channel × Payment Method Heatmap</div>', unsafe_allow_html=True)
        pivot = pay_ch.pivot_table(values='net_revenue', index='channel_name',
                                   columns='payment_method', aggfunc='sum', fill_value=0)
        fig3 = px.imshow(pivot, text_auto='.2s', color_continuous_scale='Blues',
                         aspect='auto')
        fig3.update_layout(height=300, **PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig3, use_container_width=True)

    with tab3:
        st.markdown('<div class="section-header">Monthly Return Count & Refund Amount</div>', unsafe_allow_html=True)
        ret_monthly = returns.groupby('year_month').agg(
            return_count=('return_count', 'sum'),
            total_refunds=('total_refunds', 'sum')
        ).reset_index()
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=ret_monthly['year_month'], y=ret_monthly['return_count'],
                             name='Return Count', marker_color='#ef4444', opacity=0.7), secondary_y=False)
        fig.add_trace(go.Scatter(x=ret_monthly['year_month'], y=ret_monthly['total_refunds'],
                                 name='Refund Amount ($)', line=dict(color='#f97316', width=2)), secondary_y=True)
        fig.update_layout(height=350, **PLOTLY_THEME, margin=dict(l=0,r=0,t=20,b=0))
        st.plotly_chart(fig, use_container_width=True)

        st.markdown('<div class="section-header">Return Reasons Breakdown</div>', unsafe_allow_html=True)
        ret_reason = returns.groupby('return_reason')['return_count'].sum().reset_index()
        fig2 = px.bar(ret_reason.sort_values('return_count', ascending=False),
                      x='return_reason', y='return_count',
                      color='return_count', color_continuous_scale='Reds')
        fig2.update_layout(height=320, **PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig2, use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE: Store Performance
# ════════════════════════════════════════════════════════════
elif page == "Store Performance":
    st.title("Store Performance")

    tab1, tab2 = st.tabs(["Store Rankings", "Regional Analysis"])

    with tab1:
        col1, col2 = st.columns([3, 2])
        with col1:
            st.markdown('<div class="section-header">Top 10 Stores – Net Revenue</div>', unsafe_allow_html=True)
            top10 = stores.head(10)
            fig = px.bar(top10, x='net_revenue', y='store_name', orientation='h',
                         color='region', color_discrete_sequence=PALETTE,
                         text=[fmt_currency(v) for v in top10['net_revenue']])
            fig.update_traces(textposition='outside')
            fig.update_layout(height=420, **PLOTLY_THEME, yaxis={'categoryorder': 'total ascending'},
                               margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown('<div class="section-header">Revenue by Region</div>', unsafe_allow_html=True)
            reg_rev = stores.groupby('region')['net_revenue'].sum().reset_index()
            fig2 = px.pie(reg_rev, values='net_revenue', names='region',
                          color_discrete_sequence=PALETTE, hole=0.4)
            fig2.update_layout(height=420, **PLOTLY_THEME, margin=dict(l=0,r=20,t=10,b=0))
            st.plotly_chart(fig2, use_container_width=True)

        st.markdown('<div class="section-header">Store Revenue vs Gross Margin %</div>', unsafe_allow_html=True)
        fig3 = px.scatter(stores, x='net_revenue', y='margin_pct',
                          color='region', size='transactions', hover_name='store_name',
                          color_discrete_sequence=PALETTE, size_max=40)
        fig3.update_layout(height=380, **PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig3, use_container_width=True)

        st.markdown('<div class="section-header">Store Details Table</div>', unsafe_allow_html=True)
        display_cols = ['store_name', 'store_type', 'region', 'net_revenue',
                        'gross_profit', 'margin_pct', 'transactions', 'avg_basket_value']
        fmt_stores = stores[display_cols].copy()
        fmt_stores['net_revenue']  = fmt_stores['net_revenue'].apply(fmt_currency)
        fmt_stores['gross_profit'] = fmt_stores['gross_profit'].apply(fmt_currency)
        fmt_stores['avg_basket_value'] = fmt_stores['avg_basket_value'].apply(fmt_currency)
        fmt_stores['margin_pct']   = fmt_stores['margin_pct'].apply(lambda v: f"{v}%")
        st.dataframe(fmt_stores, use_container_width=True, height=300)

    with tab2:
        st.markdown('<div class="section-header">Regional Revenue by Quarter</div>', unsafe_allow_html=True)
        fig = px.bar(regional, x='quarter_name', y='net_revenue', color='region',
                     barmode='group', facet_col='year_number',
                     color_discrete_sequence=PALETTE)
        fig.update_layout(height=400, **PLOTLY_THEME, margin=dict(l=0,r=0,t=30,b=0))
        st.plotly_chart(fig, use_container_width=True)

        st.markdown('<div class="section-header">Revenue per Store by Region</div>', unsafe_allow_html=True)
        fig2 = px.box(stores, x='region', y='net_revenue', color='region',
                      color_discrete_sequence=PALETTE, points='all')
        fig2.update_layout(height=380, **PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig2, use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE: Product Analytics
# ════════════════════════════════════════════════════════════
elif page == "Product Analytics":
    st.title("Product Analytics")

    tab1, tab2 = st.tabs(["Top Products", "Category Analysis"])

    with tab1:
        col1, col2 = st.columns([3, 2])
        with col1:
            st.markdown('<div class="section-header">Top 15 Products – Revenue & Margin</div>', unsafe_allow_html=True)
            top15 = products.head(15)
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(x=top15['product_name'], y=top15['net_revenue'],
                                 name='Net Revenue', marker_color='#4fc3f7', opacity=0.85), secondary_y=False)
            fig.add_trace(go.Scatter(x=top15['product_name'], y=top15['margin_pct'],
                                     name='Margin %', mode='lines+markers',
                                     line=dict(color='#64ffda', width=2)), secondary_y=True)
            fig.update_layout(height=420, **PLOTLY_THEME,
                               xaxis_tickangle=-45, margin=dict(l=0,r=0,t=10,b=80))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown('<div class="section-header">Top 10 – Units Sold</div>', unsafe_allow_html=True)
            top10u = products.nlargest(10, 'units_sold')
            fig2 = px.bar(top10u, x='units_sold', y='product_name', orientation='h',
                          color='category_name', color_discrete_sequence=PALETTE)
            fig2.update_layout(height=420, **PLOTLY_THEME,
                               yaxis={'categoryorder': 'total ascending'},
                               margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig2, use_container_width=True)

        st.markdown('<div class="section-header">Revenue vs Units (Bubble = Margin)</div>', unsafe_allow_html=True)
        fig3 = px.scatter(products, x='units_sold', y='net_revenue',
                          size='margin_pct', color='category_name',
                          hover_name='product_name', color_discrete_sequence=PALETTE, size_max=50)
        fig3.update_layout(height=380, **PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig3, use_container_width=True)

    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="section-header">Category Revenue Share</div>', unsafe_allow_html=True)
            fig = px.treemap(cats, path=['category_name'], values='net_revenue',
                             color='margin_pct', color_continuous_scale='Blues')
            fig.update_layout(height=400, **PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown('<div class="section-header">Category – Revenue vs Margin</div>', unsafe_allow_html=True)
            fig2 = px.scatter(cats, x='net_revenue', y='margin_pct',
                              size='units_sold', color='category_name',
                              hover_name='category_name', color_discrete_sequence=PALETTE,
                              text='category_name', size_max=60)
            fig2.update_traces(textposition='top center')
            fig2.update_layout(height=400, **PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig2, use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE: Customer Insights
# ════════════════════════════════════════════════════════════
elif page == "Customer Insights":
    st.title("Customer Insights")

    tab1, tab2 = st.tabs(["Segmentation", "Top Customers"])

    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="section-header">Revenue by Loyalty Tier</div>', unsafe_allow_html=True)
            tier_grp = segments.groupby('loyalty_tier').agg(
                total_revenue=('total_revenue', 'sum'),
                customer_count=('customer_count', 'sum')
            ).reset_index()
            tier_order = {'BRONZE': 0, 'SILVER': 1, 'GOLD': 2, 'PLATINUM': 3}
            tier_grp['order'] = tier_grp['loyalty_tier'].map(tier_order)
            tier_grp = tier_grp.sort_values('order')
            fig = px.bar(tier_grp, x='loyalty_tier', y='total_revenue',
                         color='loyalty_tier',
                         color_discrete_map={'BRONZE':'#cd7f32','SILVER':'#c0c0c0',
                                              'GOLD':'#ffd700','PLATINUM':'#e5e4e2'},
                         text=[fmt_currency(v) for v in tier_grp['total_revenue']])
            fig.update_traces(textposition='outside')
            fig.update_layout(height=360, **PLOTLY_THEME, showlegend=False,
                               margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown('<div class="section-header">Customer Count by Age Group & Tier</div>', unsafe_allow_html=True)
            fig2 = px.bar(segments, x='age_group', y='customer_count',
                          color='loyalty_tier', barmode='stack',
                          color_discrete_map={'BRONZE':'#cd7f32','SILVER':'#c0c0c0',
                                               'GOLD':'#ffd700','PLATINUM':'#e5e4e2'})
            fig2.update_layout(height=360, **PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig2, use_container_width=True)

        st.markdown('<div class="section-header">Avg Revenue per Customer by Segment</div>', unsafe_allow_html=True)
        fig3 = px.density_heatmap(segments, x='loyalty_tier', y='age_group',
                                   z='avg_revenue_per_customer', histfunc='avg',
                                   color_continuous_scale='Blues', text_auto=True)
        fig3.update_layout(height=320, **PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig3, use_container_width=True)

    with tab2:
        st.markdown('<div class="section-header">Top 10 Customers by Lifetime Value</div>', unsafe_allow_html=True)
        fig = px.bar(top_cust, x='lifetime_value', y='full_name', orientation='h',
                     color='loyalty_tier',
                     color_discrete_map={'BRONZE':'#cd7f32','SILVER':'#c0c0c0',
                                          'GOLD':'#ffd700','PLATINUM':'#e5e4e2'},
                     text=[fmt_currency(v) for v in top_cust['lifetime_value']])
        fig.update_traces(textposition='outside')
        fig.update_layout(height=420, **PLOTLY_THEME,
                           yaxis={'categoryorder': 'total ascending'},
                           margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig, use_container_width=True)

        st.markdown('<div class="section-header">Top Customer Details</div>', unsafe_allow_html=True)
        disp = top_cust[['rank','full_name','loyalty_tier','region','total_orders',
                          'lifetime_value','avg_order_value']].copy()
        disp['lifetime_value']  = disp['lifetime_value'].apply(fmt_currency)
        disp['avg_order_value'] = disp['avg_order_value'].apply(fmt_currency)
        st.dataframe(disp, use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE: Inventory Health
# ════════════════════════════════════════════════════════════
elif page == "Inventory Health":
    st.title("Inventory Health Dashboard")

    # Status KPIs
    c1, c2, c3, c4 = st.columns(4)
    metric_card(c1, "Total SKUs Tracked", fmt_num(len(inv)))
    metric_card(c2, "Out of Stock",       str(len(inv[inv['inventory_status']=='OUT_OF_STOCK'])))
    metric_card(c3, "Reorder Needed",     str(len(inv[inv['inventory_status']=='REORDER_NEEDED'])))
    metric_card(c4, "Slow Moving",        str(len(inv[inv['inventory_status']=='SLOW_MOVING'])))
    st.markdown("")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="section-header">Inventory Status Distribution</div>', unsafe_allow_html=True)
        status_counts = inv['inventory_status'].value_counts().reset_index()
        status_counts.columns = ['status', 'count']
        color_map = {'HEALTHY':'#22c55e','REORDER_NEEDED':'#f59e0b',
                     'SLOW_MOVING':'#f97316','OUT_OF_STOCK':'#ef4444'}
        fig = px.pie(status_counts, values='count', names='status',
                     color='status', color_discrete_map=color_map, hole=0.4)
        fig.update_layout(height=360, **PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<div class="section-header">Inventory Value by Category</div>', unsafe_allow_html=True)
        cat_inv = inv.groupby('category_name').agg(
            total_cost=('inventory_value_cost', 'sum'),
            total_retail=('inventory_value_retail', 'sum')
        ).reset_index()
        fig2 = px.bar(cat_inv, x='category_name', y=['total_cost', 'total_retail'],
                      barmode='group', color_discrete_sequence=['#4fc3f7','#64ffda'],
                      labels={'value':'Value ($)', 'variable':'Valuation'})
        fig2.update_layout(height=360, **PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown('<div class="section-header">Days Since Last Sale vs Quantity Available</div>', unsafe_allow_html=True)
    fig3 = px.scatter(inv, x='days_since_last_sale', y='quantity_available',
                      color='inventory_status', color_discrete_map=color_map,
                      hover_name='product_name', size='quantity_on_hand', size_max=30)
    fig3.update_layout(height=380, **PLOTLY_THEME, margin=dict(l=0,r=0,t=10,b=0))
    st.plotly_chart(fig3, use_container_width=True)

    st.markdown('<div class="section-header">Items Needing Attention</div>', unsafe_allow_html=True)
    attention = inv[inv['inventory_status'].isin(['OUT_OF_STOCK','REORDER_NEEDED'])][
        ['store_name','product_name','category_name','quantity_available',
         'reorder_point','days_since_last_sale','inventory_status']
    ].head(30)
    st.dataframe(attention, use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE: Architecture & Pipeline
# ════════════════════════════════════════════════════════════
elif page == "Architecture & Pipeline":
    st.title("Architecture & Data Pipeline")

    st.markdown("""
    <div style="background:#1e2130;border-radius:12px;padding:24px;border:1px solid #2d3250;">
    <h3 style="color:#4fc3f7;margin-top:0">Business Scenario</h3>
    <p style="color:#8892b0">
    A retail chain operates <b>20+ stores</b> across <b>5 regions</b> (North, South, East, West, Central),
    selling products across Electronics, Clothing, Food & Beverages, Home & Garden, and Sports categories.
    The system tracks <b>customers</b> (loyalty tiers), <b>sales transactions</b> (in-store, online, mobile),
    <b>payments</b> (multiple methods), <b>inventory</b> snapshots, and <b>returns</b>.
    </p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("""
        <div class="metric-card">
        <div class="metric-label">Layer 1 – Stage</div>
        <div style="color:#4fc3f7;font-size:1.1rem;margin:8px 0">Raw Ingestion</div>
        <div style="color:#8892b0;font-size:0.85rem;text-align:left">
        • Internal Snowflake Stages<br>
        • All columns VARCHAR (no transformation)<br>
        • COPY INTO raw tables<br>
        • Metadata: filename, load timestamp<br>
        • ON_ERROR = CONTINUE (reject bad rows)<br>
        • 10 raw tables (Location, Store, Customer, Product, Category, Sales, Lines, Payment, Returns, Inventory)
        </div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class="metric-card">
        <div class="metric-label">Layer 2 – Clean / Curated</div>
        <div style="color:#64ffda;font-size:1.1rem;margin:8px 0">Validated & Typed</div>
        <div style="color:#8892b0;font-size:0.85rem;text-align:left">
        • MERGE-based upserts (no duplicates)<br>
        • Type casting (TRY_TO_NUMBER, TRY_TO_DATE)<br>
        • Data quality: null checks, range validation<br>
        • Computed columns (gross_margin, age, etc.)<br>
        • CDC Streams on all clean tables<br>
        • Audit columns: _dw_inserted_ts, _is_deleted
        </div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown("""
        <div class="metric-card">
        <div class="metric-label">Layer 3 – Consumption</div>
        <div style="color:#a78bfa;font-size:1.1rem;margin:8px 0">Star Schema</div>
        <div style="color:#8892b0;font-size:0.85rem;text-align:left">
        • SCD Type 2: Store, Customer, Product<br>
        • DIM_DATE (2020–2030 calendar)<br>
        • FACT_SALES (grain: transaction line)<br>
        • FACT_INVENTORY (periodic snapshot)<br>
        • FACT_RETURNS (return events)<br>
        • Pre-aggregated monthly tables for BI
        </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown('<div class="section-header">SCD Type 2 Implementation</div>', unsafe_allow_html=True)
    scd_data = {
        'Dimension':   ['DIM_STORE', 'DIM_CUSTOMER', 'DIM_PRODUCT'],
        'Natural Key': ['store_id',  'customer_id',   'product_id'],
        'Tracked Attributes': [
            'location_id, manager_name, store_type',
            'loyalty_tier, region, is_active',
            'unit_price, unit_cost, category, is_active',
        ],
        'SCD Columns': ['scd_effective_date, scd_expiry_date, scd_is_current, scd_action'] * 3,
        'Lookup Strategy': ['Date BETWEEN effective AND expiry'] * 3,
    }
    st.dataframe(pd.DataFrame(scd_data), use_container_width=True)

    st.markdown('<div class="section-header">Pipeline Orchestration (Snowflake Tasks)</div>', unsafe_allow_html=True)
    tasks = [
        {"Step": 1, "Task": "TASK_STAGE_TO_CLEAN",    "Schedule": "Every Hour (CRON)",
         "Action": "MERGE Stage raw → Clean typed tables", "Depends On": "Root"},
        {"Step": 2, "Task": "TASK_LOAD_DIMENSIONS",    "Schedule": "After Step 1",
         "Action": "SCD Type 2 MERGE into DIM tables", "Depends On": "TASK_STAGE_TO_CLEAN"},
        {"Step": 3, "Task": "TASK_LOAD_FACTS",         "Schedule": "After Step 2",
         "Action": "INSERT new rows into FACT_SALES, FACT_INVENTORY, FACT_RETURNS", "Depends On": "TASK_LOAD_DIMENSIONS"},
        {"Step": 4, "Task": "TASK_REFRESH_AGGREGATES", "Schedule": "After Step 3",
         "Action": "TRUNCATE + INSERT monthly aggregate tables", "Depends On": "TASK_LOAD_FACTS"},
    ]
    st.dataframe(pd.DataFrame(tasks), use_container_width=True)

    st.markdown('<div class="section-header">Tech Stack</div>', unsafe_allow_html=True)
    tech = [
        {"Component": "Snowflake Warehouse", "Role": "Storage + Compute", "Details": "RETAIL_WH (X-Small, Auto-suspend 60s)"},
        {"Component": "Internal Stages",     "Role": "File Landing Zone",  "Details": "10 stages (CSV, gzip compressed)"},
        {"Component": "Streams",             "Role": "CDC",                "Details": "6 streams on clean layer tables"},
        {"Component": "Tasks",               "Role": "Orchestration",      "Details": "4 chained tasks, hourly schedule"},
        {"Component": "Python + Faker",      "Role": "Data Generation",    "Details": "500 customers, 200 products, 3000 transactions"},
        {"Component": "Streamlit",           "Role": "BI Dashboard",       "Details": "12 KPI views, interactive charts"},
        {"Component": "Plotly",              "Role": "Visualization",      "Details": "Bar, Line, Scatter, Heatmap, Treemap, Pie"},
    ]
    st.dataframe(pd.DataFrame(tech), use_container_width=True)
