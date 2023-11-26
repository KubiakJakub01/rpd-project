from dash import html, dcc


def create_layout():
    return html.Div(
        [
            html.H1("Stock Market Dashboard"),
            html.Div(id="data-info"),
            dcc.Graph(id="candlestick-chart"),
            dcc.Graph(id="rsi-chart"),
            dcc.Graph(id="macd-chart"),
            dcc.Graph(id="volume-trend-chart"),
            dcc.Graph(id="bollinger-chart"),
            dcc.Graph(id="decomposition-chart"),
            dcc.Graph(id="yearly-performance-chart"),
            dcc.Graph(id="heatmap-chart"),
            dcc.Graph(id="linear-regression-chart"),
            dcc.Graph(id="forecasts-chart"),
            dcc.Interval(
                id="interval-component", interval=60 * 1000, n_intervals=0  # 60 seconds
            ),
        ]
    )
