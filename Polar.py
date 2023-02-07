import dash
from dash import html
from dash import dcc
import plotly.express as px
import pandas as pd
import numpy as np
import dash_bootstrap_components as dbc
from dash.dependencies import Output, Input, State
import plotly.graph_objects as go
from datetime import datetime
import dash_daq as daq
# Importar hojas de trabajo de google drive     https://bit.ly/3uQfOvs
from googleapiclient.discovery import build
from google.oauth2 import service_account
from datetime import datetime
import time
import mysql.connector
import pymysql
from ast import literal_eval

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.SANDSTONE])
app.css.append_css({'external_url': '/static/reset.css'})
app.server.static_folder = 'static'
server = app.server

app.layout = dbc.Container([
    dcc.Store(id='store-data-principal', storage_type='memory'),  # 'local' or 'session'
    dcc.Store(id='store-data-cliente', storage_type='memory'),  # 'local' or 'session'
    dcc.Store(id='store-data-zona', storage_type='memory'),  # 'local' or 'session'

    dcc.Interval(
        id='my_interval',
        disabled=False,
        interval=1 * 1000,
        n_intervals=0,
        max_intervals=1
    ),
    dbc.Row([
        dbc.Col(html.H5(
            '"Cualquier tecnología lo suficientemente avanzada, es indistinguible de la magia." - Arthur C. Clarke '),
                style={'color': "green", 'font-family': "Franklin Gothic"}, width=7),

    ]),
    dbc.Row([
        dbc.Col(html.H1(
            "Prueba Técnica - Analista de Ventas - Alimentos Polar",
            style={'textAlign': 'center', 'color': '#082255', 'font-family': "Franklin Gothic"}), width=12, )
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Accordion([
                dbc.AccordionItem([
                    html.H5([
                                'La siguiente aplicación muestra la solución a la prueba del cargo de analista de ventas para la empresa Alimentos Polar.'])

                ], title="Introducción"),
            ], start_collapsed=True, style={'textAlign': 'left', 'color': '#082255', 'font-family': "Franklin Gothic"}),

        ], style={'color': '#082255', 'font-family': "Franklin Gothic"}),
    ]),
    dbc.Row([
        dbc.Col([
            # html.H5('Última actualización: ' + str(ultAct), style={'textAlign': 'right'})
        ])
    ]),
    dbc.Row([
        dbc.Tabs([
            dbc.Tab([
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                dbc.Row([
                                    dbc.Col([
                                        dbc.Row(html.H2(['Punto 1A']),
                                                style={'color': '#082255', 'font-family': "Franklin Gothic"})
                                    ])
                                ]),
                                dbc.Row([
                                    dbc.Col([
                                        dbc.Button(
                                            "Seleccionar Unidad Geográfica:",
                                            id="selec-uni-geo-target",
                                            color="primary",
                                            style={'font-family': "Franklin Gothic"},
                                            className="me-1",
                                            n_clicks=0,
                                        ),
                                        dbc.Popover(
                                            "Es la unidad geográfica introducida por el usuario para conocer las ventas de julio, agosto y septiembre.",
                                            target="selec-uni-geo-target",
                                            body=True,
                                            trigger="hover",
                                            style={'font-family': "Franklin Gothic"}
                                        ),
                                    ], width=2, align='center', className="d-grid gap-2"),

                                    dbc.Col(
                                        dbc.Spinner(children=[dcc.Dropdown(id='unidad-geografica', style={'font-family': "Franklin Gothic"})], size="lg",
                                                    color="primary", type="border", fullscreen=True, ),
                                                                                xs=3, sm=3, md=3, lg=2, xl=2, align='center'),

                                ]),
                                dbc.Row([
                                    dbc.Col([
                                        dbc.Button(
                                            "Seleccionar Zona:",
                                            id="selec-zona-target",
                                            color="success",
                                            style={'font-family': "Franklin Gothic"},
                                            className="me-1",
                                            n_clicks=0,
                                        ),
                                        dbc.Popover(
                                            "Es la zona introducida por el usuario para conocer las ventas de julio, agosto y septiembre.",
                                            target="selec-zona-target",
                                            body=True,
                                            trigger="hover",
                                            style={'font-family': "Franklin Gothic"}
                                        ),
                                    ], width=2, align='center', className="d-grid gap-2"),

                                    dbc.Col(
                                        dbc.Spinner(
                                            children=[dcc.Dropdown(id='zona', style={'font-family': "Franklin Gothic"})],
                                            size="lg",
                                            color="primary", type="border", fullscreen=True, ),
                                        xs=3, sm=3, md=3, lg=2, xl=2, align='center'),

                                ]),
                                dbc.Row(dcc.Graph(id='fig-p1')),

                            ])
                        ]),

                    ]),

                ]),
            ], label="Punto 1A", label_style={'color': '#082255', 'font-family': "Franklin Gothic"}),
            dbc.Tab([
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                dbc.Row([
                                    dbc.Col([
                                        dbc.Row(html.H2(['Punto 1B']),
                                                style={'color': '#082255', 'font-family': "Franklin Gothic"})
                                    ])
                                ]),

                                dbc.Row(dcc.Graph(id='fig-p2')),

                            ])
                        ]),

                    ]),

                ]),
            ], label="Punto 1B", label_style={'color': '#082255', 'font-family': "Franklin Gothic"}),
            dbc.Tab([
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                dbc.Row([
                                    dbc.Col([
                                        dbc.Row(html.H2(['Punto 1C']),
                                                style={'color': '#082255', 'font-family': "Franklin Gothic"})
                                    ])
                                ]),

                                dbc.Row(dcc.Graph(id='fig-1pc')),

                            ])
                        ]),

                    ]),

                ]),
            ], label="Punto 1C", label_style={'color': '#082255', 'font-family': "Franklin Gothic"}),
            dbc.Tab([
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                dbc.Row([
                                    dbc.Col([
                                        dbc.Row(html.H2(['Punto 1E']),
                                                style={'color': '#082255', 'font-family': "Franklin Gothic"})
                                    ])
                                ]),
                                dbc.Row([
                                    dbc.Col([
                                        dbc.Button(
                                            "Seleccionar Zona:",
                                            id="selec-zona1e-target",
                                            color="primary",
                                            style={'font-family': "Franklin Gothic"},
                                            className="me-1",
                                            n_clicks=0,
                                        ),
                                        dbc.Popover(
                                            "Es la zona introducida por el usuario para conocer las ventas de julio, agosto y septiembre.",
                                            target="selec-zona1e-target",
                                            body=True,
                                            trigger="hover",
                                            style={'font-family': "Franklin Gothic"}
                                        ),
                                    ], width=2, align='center', className="d-grid gap-2"),

                                    dbc.Col(
                                        dbc.Spinner(
                                            children=[
                                                dcc.Dropdown(id='zona1e', style={'font-family': "Franklin Gothic"})],
                                            size="lg",
                                            color="primary", type="border", fullscreen=True, ),
                                        xs=3, sm=3, md=3, lg=2, xl=2, align='center'),

                                ]),
                                dbc.Row([
                                    dbc.Col([
                                        dbc.Button(
                                            "Costo:",
                                            id="costo-target",
                                            color="success",
                                            style={'font-family': "Franklin Gothic"},
                                            className="me-1",
                                            n_clicks=0,
                                        ),
                                        dbc.Popover(
                                            "Muestra el costo de la zona.",
                                            target="costo-target",
                                            body=True,
                                            trigger="hover",
                                            style={'font-family': "Franklin Gothic"}
                                        ),
                                    ], width=2, align='center', className="d-grid gap-2"),
                                    dbc.Col([
                                        html.Div(id='costo1e', style={'font-family': "Franklin Gothic"})
                                    ], xs=2, sm=2, md=2, lg=2, xl=2, style={'textAlign': 'center'}, align='center'),
                                ]),
                                dbc.Row([
                                    dbc.Col([
                                        dbc.Button(
                                            "Rentabilidad Promedio:",
                                            id="rentabilidad-target",
                                            color="primary",
                                            style={'font-family': "Franklin Gothic"},
                                            className="me-1",
                                            n_clicks=0,
                                        ),
                                        dbc.Popover(
                                            "Muestra la rentabilidad promedio de la zona.",
                                            target="rentabilidad-target",
                                            body=True,
                                            trigger="hover",
                                            style={'font-family': "Franklin Gothic"}
                                        ),
                                    ], width=2, align='center', className="d-grid gap-2"),
                                    dbc.Col([
                                        html.Div(id='rentabilidad1e', style={'font-family': "Franklin Gothic"})
                                    ], xs=2, sm=2, md=2, lg=2, xl=2, style={'textAlign': 'center'}, align='center'),
                                ]),
                                dbc.Row(dcc.Graph(id='fig-p1e')),
                                dbc.Row(dcc.Graph(id='fig-p1e2')),

                            ])
                        ]),

                    ]),

                ]),
            ], label="Punto 1E", label_style={'color': '#082255', 'font-family': "Franklin Gothic"}),
            dbc.Tab([
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                dbc.Row([
                                    dbc.Col([
                                        dbc.Row(html.H2(['Punto 1F']),
                                                style={'color': '#082255', 'font-family': "Franklin Gothic"})
                                    ])
                                ]),

                                dbc.Row(dcc.Graph(id='fig-1f')),

                            ])
                        ]),

                    ]),

                ]),
            ], label="Punto 1F", label_style={'color': '#082255', 'font-family': "Franklin Gothic"}),

        ]),
    ]),

])


@app.callback(
    Output(component_id='store-data-principal', component_property='data'),
    Output(component_id='store-data-cliente', component_property='data'),
    Output(component_id='store-data-zona', component_property='data'),
    Output(component_id='unidad-geografica', component_property='options'),
    Output(component_id='zona', component_property='options'),
    Output(component_id='zona1e', component_property='options'),
    Output(component_id='unidad-geografica', component_property='value'),
    Output(component_id='zona', component_property='value'),
    Output(component_id='zona1e', component_property='value'),

    Input('my_interval', 'n_intervals'),
)
def dropdownTiempoReal(value_intervals):
    df_principal = pd.read_csv('df_principal.csv', encoding='cp1252', sep=";")
    df_cliente = pd.read_csv('df_tipo_de_cliente.csv', encoding='cp1252', sep=";")
    df_zona = pd.read_csv('df_zona.csv', encoding='cp1252', sep=";")


    unigeoDD = df_principal["Unidad Geografica"]
    unigeoDD = unigeoDD.drop_duplicates()

    unigeoDD1 = unigeoDD[0]


    zonaDD = df_zona["Zona"]
    zonaDD = zonaDD.drop_duplicates()
    zonaDD = zonaDD.sort_values()

    zonaDD1 = zonaDD[0]



    # print(df_principal)
    # print(df_cliente)
    # print(df_zona)
    # print(unigeoDD)


    return df_principal.to_dict('records'), df_cliente.to_dict('records'), df_zona.to_dict('records'), unigeoDD, zonaDD,\
           zonaDD, unigeoDD1, zonaDD1, zonaDD1



@app.callback(

    Output(component_id='fig-p1', component_property="figure"),
    Output(component_id='fig-p2', component_property="figure"),
    Output(component_id='fig-1pc', component_property="figure"),
    Output(component_id='costo1e', component_property="children"),
    Output(component_id='rentabilidad1e', component_property="children"),
    Output(component_id='fig-p1e', component_property="figure"),
    Output(component_id='fig-p1e2', component_property="figure"),
    Output(component_id='fig-1f', component_property="figure"),

    Input(component_id='store-data-principal', component_property='data'),
    Input(component_id='store-data-cliente', component_property='data'),
    Input(component_id='store-data-zona', component_property='data'),
    Input(component_id='unidad-geografica', component_property='value'),
    Input(component_id='zona', component_property='value'),
    Input(component_id='zona1e', component_property='value'),



)
def dashboard_interactivo(data1, data2, data3, value_unnigeo, value_zona, value_zona1e):

    df_principal = pd.DataFrame(data1)
    df_cliente = pd.DataFrame(data2)
    df_zona = pd.DataFrame(data3)

    df_principal['Venta Julio'] = pd.to_numeric(df_principal['Venta Julio'])
    df_principal['Venta Agosto'] = pd.to_numeric(df_principal['Venta Agosto'])
    df_principal['Venta Septiembre'] = pd.to_numeric(df_principal['Venta Septiembre'])
    df_zona['Costo'] = pd.to_numeric(df_zona['Costo'])

    # Punto 1A

    df_p1 = df_principal[df_principal["Unidad Geografica"] == value_unnigeo]
    df_p1 = df_p1[df_principal["Zona de ventas"] == value_zona]
    vjuli = df_p1['Venta Julio'].sum()
    vago = df_p1['Venta Agosto'].sum()
    vsept = df_p1['Venta Septiembre'].sum()

    p1x = ['Julio', 'Agosto', 'Septiembre']
    p1y = [vjuli, vago, vsept]


    figurap1 = px.bar(x=p1x, y=p1y)

    figurap1.update_layout(
        font_family="Franklin Gothic",
        title_font_family="Franklin Gothic",
        barmode='group',
        title='Ventas',
        xaxis_title='Mes',
        yaxis_title='Ventas'
    )

    # Punto 1B
    dictionary = dict(zip(df_cliente['Tipo de cliente'], df_cliente['cliente']))
    df_principal['Nombre cliente'] = df_principal['Tipo de cliente'].map(dictionary)


    df1b_juli = df_principal.groupby("Nombre cliente")["Venta Julio"].sum()
    df1b_ago = df_principal.groupby("Nombre cliente")["Venta Agosto"].sum()
    df1b_sep = df_principal.groupby("Nombre cliente")["Venta Septiembre"].sum()

    df1b_juli = df1b_juli.to_frame()
    df1b_ago = df1b_ago.to_frame()
    df1b_sep = df1b_sep.to_frame()

    df1bA = pd.merge(df1b_juli, df1b_ago, how='inner', on=['Nombre cliente'])
    df1b = pd.merge(df1bA, df1b_sep, how='inner', on=['Nombre cliente'])

    venTotJul = df_principal['Venta Julio'].sum()
    venTotAgo = df_principal['Venta Agosto'].sum()
    venTotSep = df_principal['Venta Septiembre'].sum()

    df1b['Prct Julio'] = df1b['Venta Julio'] / venTotJul
    df1b['Prct Agosto'] = df1b['Venta Agosto'] / venTotAgo
    df1b['Prct Septiembre'] = df1b['Venta Septiembre'] / venTotSep

    df1b["Nombre cliente"] = df1b.index

    figurap2 = px.bar(df1b, x="Nombre cliente", y=["Prct Julio", "Prct Agosto", "Prct Septiembre"], title="Porcentaje de Aporte")

    # Punto 1C

    c1Jul = df_principal[df_principal['Venta Julio'] < 1000000].count()
    c2Jul = df_principal[(df_principal['Venta Julio'] >= 1000000) & (df_principal['Venta Julio'] < 20000000)].count()
    c3Jul = df_principal[(df_principal['Venta Julio'] >= 20000000) & (df_principal['Venta Julio'] < 50000000)].count()
    c4Jul = df_principal[(df_principal['Venta Julio'] >= 50000000) & (df_principal['Venta Julio'] < 100000000)].count()
    c5Jul = df_principal[df_principal['Venta Julio'] > 100000000].count()

    c1Jul = c1Jul.loc['Venta Julio']
    c2Jul = c2Jul.loc['Venta Julio']
    c3Jul = c3Jul.loc['Venta Julio']
    c4Jul = c4Jul.loc['Venta Julio']
    c5Jul = c5Jul.loc['Venta Julio']

    c1Ago = df_principal[df_principal['Venta Agosto'] < 1000000].count()
    c2Ago = df_principal[(df_principal['Venta Agosto'] >= 1000000) & (df_principal['Venta Agosto'] < 20000000)].count()
    c3Ago = df_principal[(df_principal['Venta Agosto'] >= 20000000) & (df_principal['Venta Agosto'] < 50000000)].count()
    c4Ago = df_principal[(df_principal['Venta Agosto'] >= 50000000) & (df_principal['Venta Agosto'] < 100000000)].count()
    c5Ago = df_principal[df_principal['Venta Agosto'] > 100000000].count()

    c1Ago = c1Ago.loc['Venta Agosto']
    c2Ago = c2Ago.loc['Venta Agosto']
    c3Ago = c3Ago.loc['Venta Agosto']
    c4Ago = c4Ago.loc['Venta Agosto']
    c5Ago = c5Ago.loc['Venta Agosto']

    c1Sep = df_principal[df_principal['Venta Septiembre'] < 1000000].count()
    c2Sep = df_principal[(df_principal['Venta Septiembre'] >= 1000000) & (df_principal['Venta Septiembre'] < 20000000)].count()
    c3Sep = df_principal[(df_principal['Venta Septiembre'] >= 20000000) & (df_principal['Venta Septiembre'] < 50000000)].count()
    c4Sep = df_principal[(df_principal['Venta Septiembre'] >= 50000000) & (df_principal['Venta Septiembre'] < 100000000)].count()
    c5Sep = df_principal[df_principal['Venta Septiembre'] > 100000000].count()

    c1Sep = c1Sep.loc['Venta Septiembre']
    c2Sep = c2Sep.loc['Venta Septiembre']
    c3Sep = c3Sep.loc['Venta Septiembre']
    c4Sep = c4Sep.loc['Venta Septiembre']
    c5Sep = c5Sep.loc['Venta Septiembre']

    df_cond = pd.DataFrame()
    df_cond['Condición'] = ['<$1.000.000', '$1.000.000-$20.000.000', '$20.000.000-50.000.000', '$50.000.000-100.000.000', '>100.000.000']
    df_cond['# Ventas Julio'] = [c1Jul, c2Jul, c3Jul, c4Jul, c5Jul]
    df_cond['# Ventas Agosto'] = [c1Ago, c2Ago, c3Ago, c4Ago, c5Ago]
    df_cond['# Ventas Septiembre'] = [c1Sep, c2Sep, c3Sep, c4Sep, c5Sep]

    figurap1c = px.bar(df_cond, x="Condición", y=["# Ventas Julio", "# Ventas Agosto", "# Ventas Septiembre"], title="Número de ventas")

    # Punto 1E

    df_1e = pd.DataFrame()
    df_1e['Venta Zona Julio'] = df_principal.groupby("Zona de ventas")["Venta Julio"].sum()
    df_1e['Venta Zona Agosto'] = df_principal.groupby("Zona de ventas")["Venta Agosto"].sum()
    df_1e['Venta Zona Septiembre'] = df_principal.groupby("Zona de ventas")["Venta Septiembre"].sum()

    df_1e['Zona de Ventas'] = df_1e.groupby(["Zona de ventas"]).median().index.get_level_values('Zona de ventas')

    dictionary1e = dict(zip(df_zona['Zona'], df_zona['Costo']))
    df_1e['Costo'] = df_1e['Zona de Ventas'].map(dictionary1e)

    df_1e['Rentabilidad Julio'] = df_1e['Venta Zona Julio'] - df_1e['Costo']
    df_1e['Rentabilidad Agosto'] = df_1e['Venta Zona Agosto'] - df_1e['Costo']
    df_1e['Rentabilidad Septiembre'] = df_1e['Venta Zona Septiembre'] - df_1e['Costo']
    df_1e['Rentabilidad Promedio'] = (df_1e['Rentabilidad Julio']/3 + df_1e['Rentabilidad Agosto']/3 + df_1e['Rentabilidad Septiembre']/3)


    costo1e = df_1e.loc[value_zona1e]
    costo1e = costo1e.get(key='Costo')
    costo1e = round(costo1e, 0)

    rentProm1e = df_1e.loc[value_zona1e]
    rentProm1e = rentProm1e.get(key='Rentabilidad Promedio')
    rentProm1e = round(rentProm1e, 0)

    # print(costo1e)
    # print(rentProm1e)

    df_vz = df_1e.loc[value_zona1e]
    vzj = df_vz.get(key='Venta Zona Julio')
    vza = df_vz.get(key='Venta Zona Agosto')
    vzs = df_vz.get(key='Venta Zona Septiembre')

    rzj = df_vz.get(key='Rentabilidad Julio')
    rza = df_vz.get(key='Rentabilidad Agosto')
    rzs = df_vz.get(key='Rentabilidad Septiembre')


    p1x = ['Venta Zona Julio', 'Venta Zona Agosto', 'Venta Zona Septiembre']
    p1y = [vzj, vza, vzs]
    p1y2 = [rzj, rza, rzs]

    figurap1e1 = px.bar(x=p1x, y=p1y)
    figurap1e1.update_layout(
        font_family="Franklin Gothic",
        title_font_family="Franklin Gothic",
        barmode='group',
        title='Ventas',
        xaxis_title='Mes',
        yaxis_title='Ventas'
    )

    figurap1e2 = px.bar(x=p1x, y=p1y2)
    figurap1e2.update_layout(
        font_family="Franklin Gothic",
        title_font_family="Franklin Gothic",
        barmode='group',
        title='Rentabilidad',
        xaxis_title='Mes',
        yaxis_title='Rentabilidad'
    )

    # Punto 1F
    df_1f = df_1e

    dictionary1f = dict(zip(df_principal['Zona de ventas'], df_principal['Unidad Geografica']))
    df_1e['Unidad Geografica'] = df_1f['Zona de Ventas'].map(dictionary1f)

    df_1e_group = pd.DataFrame()
    df_1e_group['Rentabilidad Julio'] = df_1e.groupby("Unidad Geografica")["Rentabilidad Julio"].sum()
    df_1e_group['Rentabilidad Agosto'] = df_1e.groupby("Unidad Geografica")["Rentabilidad Agosto"].sum()
    df_1e_group['Rentabilidad Septiembre'] = df_1e.groupby("Unidad Geografica")["Rentabilidad Septiembre"].sum()
    df_1e_group['Rentabilidad Promedio'] = (df_1e_group['Rentabilidad Julio'] + df_1e_group['Rentabilidad Agosto'] + df_1e_group['Rentabilidad Septiembre'])/3
    df_1e_group['Unidad Geografica'] = df_1e_group.groupby(["Unidad Geografica"]).median().index.get_level_values('Unidad Geografica')


    # df_1e['Venta Zona Agosto'] = df_principal.groupby("Zona de ventas")["Venta Agosto"].sum()



    print(df_1f)
    print(df_1e_group)

    # Crea la figura de el volumen captado y retornado
    fig1f = go.Figure()

    fig1f.add_trace(go.Scatter(x=['Julio', 'Agosto', 'Septiembre', ], y=df_1e_group.loc['CENTRO - APCOL', :], name='CENTRO - APCOL'))
    fig1f.add_trace(go.Scatter(x=['Julio', 'Agosto', 'Septiembre', ], y=df_1e_group.loc['NORTE - APCOL', :], name='NORTE - APCOL'))
    fig1f.add_trace(go.Scatter(x=['Julio', 'Agosto', 'Septiembre', ], y=df_1e_group.loc['OCCIDENTE - APCOL', :], name='OCCIDENTE - APCOL'))

    fig1f.update_layout(title="Rentabilidad", xaxis_title="Unidad Geografica",
                            yaxis_title='Rentabilidad')
    fig1f.update_layout(legend=dict(
        yanchor="bottom",
        y=-0.5,
        xanchor="center",
        x=0.5
    ))

    fig1f.update_layout(
        font_family="Franklin Gothic",
        title_font_family="Franklin Gothic",
    )
    fig1f.update_xaxes(title_font_family="Franklin Gothic")







    return figurap1, figurap2, figurap1c, costo1e, rentProm1e, figurap1e1, figurap1e2, fig1f


if __name__ == '__main__':
    app.run_server()



