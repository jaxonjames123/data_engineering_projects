import pandas as pd
import pyodbc
from bokeh.io import curdoc
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, Div, HoverTool
from bokeh.models.formatters import NumeralTickFormatter
from bokeh.models.widgets import DataTable, TableColumn

conn = pyodbc.connect(dsn="somos_redshift_1")
query_enc = """
SELECT PRACTICENAME,EMRTYPE,tin,to_char(min_pull_date,'yyyymmdd') as first_pull_date,TO_CHAR(LAST_PULL_DATE,'yyyymmdd') as last_pull_date,to_char(LAST_ENCOUNTER_DATE,'yyyymmdd') as last_encounter_date,DATEDIFF(DAY,LAST_ENCOUNTER_DATE,LAST_PULL_DATE) AS LAG,
total_encounter_count,total_patient_count
FROM (
select practicename,emrtype,tin,max(t1.createdate) as last_pull_date,max(encounterdate) as last_encounter_date,count(t1.id) as total_encounter_count,count(distinct patientid) as total_patient_count,
min(t1.createdate) as min_pull_date
from gdw.emr_encounters t1
inner join gdw.practice_details t2
on t1.practiceid = t2.id
WHERE ENCOUNTERDATE < T1.CREATEDATE
group by practicename,emrtype,tin ) A
order by 2,1
"""
query_status = """ select date_processed,tablename,row_count,practice_count from ( 
select date_processed,case when tablename = 'Encounter DX' then 'EncounterDX' else tablename end as tablename,sum(count) as row_count,count(distinct practiceid) as practice_count, rank() over(partition by tablename order by date_processed desc) as rnk from gdw.gdw_status where date_processed != '20221010' and substring(monthid,1,4) >= '2022' group by date_processed,tablename  ) a where rnk <= 5
order by 1,2
"""

df_enc = pd.read_sql(query_enc, conn)
source = ColumnDataSource(df_enc)
df_stat = pd.read_sql(query_status, conn)
source_stat = ColumnDataSource(df_stat)
cols = [TableColumn(field=col, title=col) for col in df_enc.columns]
tbl = DataTable(
    source=source,
    columns=cols,
    sortable=True,
    row_height=20,
    height=500,
    width=1100,
    name="Encounters",
)

min_range = 0
max_range = (
    pd.DataFrame(df_stat.groupby(["date_processed"], as_index=False).sum())[
        "row_count"
    ].max()
    * 1.2
)
stacked = list(df_stat["tablename"].unique())
colors1 = ["#30678D", "#35B778", "#4E6C50", "#AA8B56", "#FOBECE"]
colors2 = ["#FAF7F0", "#CDFCF6", "#BCCEF8", "#98A8F8", "#ADECF9"]
e_period = df_stat["date_processed"].unique().astype("str")
data = {tname: df_stat[df_stat["tablename"] == tname]["row_count"] for tname in stacked}
data["period"] = e_period
data1 = {
    tname: df_stat[df_stat["tablename"] == tname]["practice_count"] for tname in stacked
}
data1["period"] = e_period
p = figure(
    x_range=e_period,
    y_range=(min_range, max_range),
    title="Weekly GDW status Row counts ",
    height=400,
    width=500,
    tools=["box_select", "wheel_zoom", "tap"],
)
p1 = figure(
    x_range=e_period,
    y_range=(0, 2000),
    title="Weekly GDW status Practice counts ",
    height=400,
    width=500,
    tools=["box_select", "wheel_zoom", "tap"],
)
p.x_range.factors = e_period
p.y_range.start = min_range
p.y_range.end = max_range
p.vbar_stack(
    stacked,
    x="period",
    width=0.5,
    color=colors1,
    source=data,
    legend_label=stacked,
)
p1.vbar_stack(
    stacked,
    x="period",
    width=0.5,
    color=colors2,
    source=data1,
    legend_label=stacked,
)
p.yaxis.formatter = NumeralTickFormatter(format="0")
p.add_tools(
    HoverTool(
        show_arrow=False,
        line_policy="next",
        tooltips=[
            ("Encounters", "@Encounters"),
            ("Encounter DX", "@EncounterDX"),
            ("procedure", "@procedures"),
            ("laborder_details", "@laborder_details"),
            ("vitals", "@vitals"),
        ],
    )
)
p1.add_tools(
    HoverTool(
        show_arrow=False,
        line_policy="next",
        tooltips=[
            ("Encounters", "@Encounters"),
            ("Encounter DX", "@EncounterDX"),
            ("procedure", "@procedures"),
            ("laborder_details", "@laborder_details"),
            ("vitals", "@vitals"),
        ],
    )
)
p.legend.location = "top_left"
p.legend.orientation = "vertical"
p1.legend.location = "top_left"
p1.legend.orientation = "horizontal"
p1.legend.label_text_font_size = "8pt"
pr = Div(
    text="""Note: Count in Charts for Encounters starting Jan 2022""",
    width=1200,
    height=100,
)

curdoc().add_root(
    column(
        row(
            p,
        ),
        name="GDWStatus1",
        sizing_mode="scale_width",
    )
)
curdoc().add_root(
    column(
        row(
            p1,
        ),
        name="GDWStatus2",
        sizing_mode="scale_width",
    )
)
# curdoc().add_root(column(row(pr),name="notes",sizing_mode="scale_width"))
curdoc().add_root(column(row(tbl), name="GDWTbl", sizing_mode="scale_width"))
