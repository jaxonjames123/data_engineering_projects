import pandas as pd
from bokeh.io import curdoc
from bokeh.layouts import column, row
from bokeh.models import (
    ColumnDataSource,
    DataTable,
    DateFormatter,
    FactorRange,
    HoverTool,
    TableColumn,
)
from bokeh.plotting import ColumnDataSource, figure, show


def create_table_redshift_inventory():
    roster_data = dict(pd.read_pickle('/home/etl/etl_home/downloads/roster_data.pkl')[['mco', 'ipa', 'total_count', 'earliest_month', 'latest_month', 'effective_date', 'latest_month_member_count', 'last_received']])
    claims_data = dict(pd.read_pickle('/home/etl/etl_home/downloads/claim_data.pkl')[['mco', 'ipa', 'total_count', 'earliest_month', 'latest_month', 'activity_date', 'latest_month_claim_count', 'last_received']])
    pharmacy_data = dict(pd.read_pickle('/home/etl/etl_home/downloads/rx_data.pkl')[['mco', 'ipa', 'total_count', 'earliest_month', 'latest_month', 'effective_date', 'latest_month_rx_count', 'last_received']])
    gic_data = dict(pd.read_pickle('/home/etl/etl_home/downloads/gic_data.pkl')[['mco', 'ipa', 'total_count', 'earliest_month', 'latest_month', 'effective_date', 'latest_gic_count', 'last_received']])

    roster_source = ColumnDataSource(roster_data)
    roster_columns = [
        TableColumn(field='mco', title='MCO'),
        TableColumn(field='ipa', title='IPA'),
        TableColumn(field='total_count', title='Total Count'),
        TableColumn(field='earliest_month', title='Earliest Month'),
        TableColumn(field='latest_month', title='Latest Received Month'),
        TableColumn(field='effective_date', title='Effective Date',
                    formatter=DateFormatter()),
        TableColumn(field='latest_month_member_count', title='Member Count'),
        TableColumn(field='last_received', title='Last Ingested'),
    ]
    roster_data_table = DataTable(source=roster_source, columns=roster_columns, fit_columns=True, row_height=20, height=250, width=1100)

    claims_source = ColumnDataSource(claims_data)
    claims_columns = [
        TableColumn(field='mco', title='MCO'),
        TableColumn(field='ipa', title='IPA'),
        TableColumn(field='total_count', title='Total Count'),
        TableColumn(field='earliest_month', title='Earliest Month'),
        TableColumn(field='latest_month', title='Latest Received Month'),
        TableColumn(field='activity_date', title='Effective Date',
                    formatter=DateFormatter()),
        TableColumn(field='latest_month_claim_count', title='Claim Count'),
        TableColumn(field='last_received', title='Last Ingested'),
    ]
    claims_data_table = DataTable(source=claims_source, columns=claims_columns, sortable=True, fit_columns=True, row_height=20, height=250, width=1100)

    pharmacy_source = ColumnDataSource(pharmacy_data)
    pharmacy_columns = [
        TableColumn(field='mco', title='MCO'),
        TableColumn(field='ipa', title='IPA'),
        TableColumn(field='total_count', title='Total Count'),
        TableColumn(field='earliest_month', title='Earliest Month'),
        TableColumn(field='latest_month', title='Latest Received Month'),
        TableColumn(field='effective_date', title='Effective Date',
                    formatter=DateFormatter()),
        TableColumn(field='latest_month_rx_count',
                    title='Pharmacy Claim Count'),
        TableColumn(field='last_received', title='Last Ingested'),
    ]
    pharmacy_data_table = DataTable(source=pharmacy_source, columns=pharmacy_columns,
                                    sortable=True, fit_columns=True, row_height=20, height=250, width=1100)

    gic_source = ColumnDataSource(gic_data)
    gic_columns = [
        TableColumn(field='mco', title='MCO'),
        TableColumn(field='ipa', title='IPA'),
        TableColumn(field='total_count', title='Total Count'),
        TableColumn(field='earliest_month', title='Earliest Month'),
        TableColumn(field='latest_month', title='Latest Received Month'),
        TableColumn(field='effective_date', title='Effective Date',
                    formatter=DateFormatter()),
        TableColumn(field='latest_gic_count',
                    title='GIC Count'),
        TableColumn(field='last_received', title='Last Ingested'),
    ]
    gic_data_table = DataTable(source=gic_source, columns=gic_columns, sortable=True, fit_columns=True, row_height=20, height=320, width=1100)
    return [roster_data_table, claims_data_table, pharmacy_data_table, gic_data_table]


def generate_mco_dashboard():
    df = pd.read_pickle('mco_data.pkl')
    min_range = -100
    max_range = (df.groupby(["ipa", "for_month"],
                            as_index=False).sum()["cnt"].max()) * 1.05
    ipas = df["ipa"].unique()
    lst = []
    e_period = sorted(list(df.for_month.unique()))
    stacked = list(df.feed_type.unique())
    ipas = sorted(list(df.ipa.unique()))
    for ipa in ipas:
        for period in e_period:
            for feed_type in stacked:
                if not ((df["ipa"] == ipa) & (df["for_month"] == period) & (df["feed_type"] == feed_type)).any():
                    lst.append([ipa, feed_type, period, 0, 0])
    df2 = pd.DataFrame(
        lst, columns=["ipa", "feed_type", "for_month", "cnt", "rnk"])
    df = pd.concat([df, df2])
    factors = [(period, ipa) for period in e_period for ipa in ipas]
    feeds = {}
    for feed_type in stacked:
        feeds[feed_type] = [df[(df["feed_type"] == feed_type) & (df["for_month"] == period) & (
            df["ipa"] == ipa)]["cnt"].to_list() for period, ipa in factors]
    for feed_type in stacked:
        feeds[feed_type] = [val[0] for val in feeds[feed_type]]
    feeds["x"] = factors
    claim_full = []
    eligibility_full = []
    rxclaim_full = []
    gic_full = []
    for feed_type in feeds:
        if feed_type == 'Claim':
            for number in feeds[feed_type]:
                claim_full.append(number * 1000)
        if feed_type == 'Eligibility':
            for number in feeds[feed_type]:
                eligibility_full.append(number * 100)
        if feed_type == 'RXClaim':
            for number in feeds[feed_type]:
                rxclaim_full.append(number * 1000)
        if feed_type == 'GIC':
            for number in feeds[feed_type]:
                gic_full.append(number * 100)
    feeds["claim_full"] = claim_full
    feeds["eligibility_full"] = eligibility_full
    feeds["rxclaim_full"] = rxclaim_full
    feeds["gic_full"] = gic_full
    source = ColumnDataSource(feeds)
    p = figure(x_range=FactorRange(*factors), height=500, width=1200, toolbar_location=None, tools="")
    p.vbar_stack(stacked, x='x', width=0.9, line_width=0.5, line_alpha=1.0, color=[
        "blue", "red", "green", "yellow"], source=source, legend_label=stacked)
    p.y_range.start = min_range
    p.y_range.end = max_range
    p.x_range.group_padding = 2.5
    p.x_range.range_padding = 0.01
    p.xaxis.major_label_orientation = 1.5
    p.xaxis.axis_label_text_font_size = "8px"
    p.xaxis.axis_label_text_font = "helvetica"
    p.xaxis.axis_label_text_line_height = 2.4
    p.xaxis.axis_line_width = 3.8
    p.xgrid.grid_line_color = None
    p.yaxis.ticker.max_interval = 1000
    p.add_tools(HoverTool(show_arrow=False, line_policy='next', tooltips=[("Eligibility", "@eligibility_full{0,0}"), ("Claim", "@claim_full{0,0}"), ("RX Claim", "@rxclaim_full{0,0}"), ("GIC", "@gic_full{0,0}"), ("Group", "@x")]))
    show(p)
    # return p


tables = create_table_redshift_inventory()

curdoc().add_root(column(row(tables[0]), name="Roster", sizing_mode="scale_width"))
curdoc().add_root(column(row(tables[1]), name="Claims", sizing_mode="scale_width"))
curdoc().add_root(
    column(row(tables[2]), name="Pharmacy", sizing_mode="scale_width"))
curdoc().add_root(column(row(tables[3]), name="GIC", sizing_mode="scale_width"))
curdoc().add_root(column(row(generate_mco_dashboard(),), name="MCOStatus", sizing_mode="scale_width"))
