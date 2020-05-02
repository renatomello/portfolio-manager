from dash import Dash
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html

from flask import Flask
from flask_restful import Resource, Api

from sqlite3 import connect
from pandas import read_sql_query

database = 'database.db'
connection = connect(database)
portfolio = read_sql_query('SELECT * FROM portfolio', connection)[1:8]
aggregate = read_sql_query('SELECT * FROM aggregate', connection)
connection.close()

external_stylesheets = [dbc.themes.DARKLY]

app = Dash(
    __name__,
    meta_tags = [{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}], 
    external_stylesheets = external_stylesheets
    )
server = app.server
app.config['suppress_callback_exceptions'] = True

def build_banner():
    element = html.Div(
        id = 'banner',
        className = 'banner',
        children = [
            html.Div(
                id = 'banner-text',
                children = [
                    html.H5('Portfolio Dashboard'),
                    html.H6('Investment Control and Exception Reporting'),
                ],
            ),
            html.Div(
                id = 'banner-logo',
                children = [
                    html.Button(
                        id = 'learn-more-button',
                        children = 'LEARN MORE',
                        n_clicks = 0
                    ),
                ],
            ),
        ],
    )
    return element


def build_tabs():
    element = html.Div(
        id = 'tabs',
        className = 'tabs',
        children = [
            dcc.Tabs(
                id = 'app-tabs',
                value = 'tab2',
                className = 'custom-tabs',
                children = [
                    dcc.Tab(
                        id = 'Specs-tab',
                        label = 'General View',
                        value = 'tab1',
                        className = 'custom-tab',
                        selected_className = 'custom-tab--selected',
                    ),
                    dcc.Tab(
                        id = 'Control-chart-tab',
                        label = 'Advanced Charts',
                        value = 'tab2',
                        className = 'custom-tab',
                        selected_className = 'custom-tab--selected',
                    ),
                ],
            )
        ],
    )
    return element

def generate_section_banner(title):
    return html.Div(className='section-banner', children=title)

def generate_modal():
    return html.Div(
        id='markdown',
        className='modal',
        children=(
            html.Div(
                id='markdown-container',
                className='markdown-container',
                children=[
                    html.Div(
                        className='close-container',
                        children=html.Button(
                            'Close',
                            id='markdown_close',
                            n_clicks=0,
                            className='closeButton',
                        ),
                    ),
                    html.Div(
                        className='markdown-text',
                        children=dcc.Markdown(
                            children=(
                                '''
                        ###### What is this mock app about?
                        This is a dashboard for monitoring real-time process quality along manufacture production line.
                        ###### What does this app shows
                        Click on buttons in `Parameter` column to visualize details of measurement trendlines on the bottom panel.
                        The sparkline on top panel and control chart on bottom panel show Shewhart process monitor using mock data.
                        The trend is updated every other second to simulate real-time measurements. Data falling outside of six-sigma control limit are signals indicating 'Out of Control(OOC)', and will
                        trigger alerts instantly for a detailed checkup.
                        
                        Operators may stop measurement by clicking on `Stop` button, and edit specification parameters by clicking specification tab.
                    '''
                            )
                        ),
                    ),
                ],
            )
        ),
    )

app.layout = html.Div(
    id = 'big-app-container',
    children = [
        build_banner(),
        dcc.Interval(
            id='interval-component',
            interval=2 * 1000,  # in milliseconds
            n_intervals=50,  # start at batch 50
            disabled=True,
        ),
        html.Div(
            id = 'app-container',
            children = [ 
                build_tabs(),
                # Main app
                html.Div( id = 'app-content'),
            ],
        ),
        dcc.Store(id='value-setter-store'),
        dcc.Store(id='n-interval-stage', data=50),
        generate_modal(),
    ],
)

def build_tab_1():
    return [
        # Manually select metrics
        html.Div(
            id="set-specs-intro-container",
            # className='twelve columns',
            children=html.P(
                "Use historical control limits to establish a benchmark, or set new values."
            ),
        ),
        html.Div(
            id="settings-menu",
            children=[
                html.Div(
                    id="metric-select-menu",
                    # className='five columns',
                    children=[
                        html.Label(id="metric-select-title", children="Select Metrics"),
                        html.Br(),
                        #dcc.Dropdown(
                        #    id="metric-select-dropdown",
                        #    options=list(
                        #        {"label": param, "value": param} for param in params[1:]
                        #    ),
                        #    value=params[1],
                        #),
                    ],
                ),
                html.Div(
                    id="value-setter-menu",
                    # className='six columns',
                    children=[
                        html.Div(id="value-setter-panel"),
                        html.Br(),
                        html.Div(
                            id="button-div",
                            children=[
                                html.Button("Update", id="value-setter-set-btn"),
                                html.Button(
                                    "View current setup",
                                    id="value-setter-view-btn",
                                    n_clicks=0,
                                ),
                            ],
                        ),
                        html.Div(
                            id="value-setter-view-output", className="output-datatable"
                        ),
                    ],
                ),
            ],
        ),
    ]

def generate_piechart(labels, values):
    graph = dcc.Graph(
        id = 'piechart',
        figure = {
            'data': [
                {
                    'labels': labels,
                    'values': values,
                    'type': 'pie',
                    'marker': {'line': {'color': 'white', 'width': 1}},
                    'hoverinfo': 'label',
                    'textinfo': 'label',
                }
            ],
            'layout': {
                'margin': dict(l=20, r=20, t=20, b=20),
                'showlegend': True,
                'paper_bgcolor': 'rgba(0,0,0,0)',
                'plot_bgcolor': 'rgba(0,0,0,0)',
                'font': {'color': 'white'},
                'autosize': True,
            },
        },
    )
    return graph

def build_top_panel(labels, values, title):
    element = html.Div(
        id = 'top-section-container',
        className = 'row',
        children = [
            html.Div(
                id = 'ooc-piechart-outer',
                className = 'four columns',
                children = [
                    generate_section_banner(title),
                    generate_piechart(labels, values),
                ],
            ),
        ],
    )
    return element

@app.callback(
    [Output('app-content', 'children')],#, Output('interval-component', 'n_intervals')],
    [Input('app-tabs', 'value')],
    [State('n-interval-stage', 'data')],
)
def render_tab_content(tab_switch, stop_interval):
    if tab_switch == 'tab1':
        return build_tab_1()#, stop_interval
    element = (
        html.Div(
            id = 'status-container',
            children = [
                #build_quick_stats_panel(),
                html.Div(
                    id = 'graphs-container',
                    children = [
                        build_top_panel(aggregate.asset, aggregate.value_usd, 'General Portfolio'),                    ],
                ),
            ],
            #style = {'width': '50%', 'display': 'inline-block', 'textAlign': 'center'},
        ),
        #stop_interval
    )
    return element

if __name__ == '__main__':
    app.run_server(debug = True)