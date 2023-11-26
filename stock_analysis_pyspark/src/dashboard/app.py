import logging
import os

from dash import Dash
from dash.dependencies import Input, Output

from ..data_preprocessing import load_data_from_cassandra
from . import charts
from .layouts import create_layout

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Dash(__name__)
app.layout = create_layout()


@app.callback(
    [
        Output("data-info", "children"),
        Output("candlestick-chart", "figure"),
        Output("rsi-chart", "figure"),
        Output("macd-chart", "figure"),
        Output("volume-trend-chart", "figure"),
        Output("bollinger-chart", "figure"),
        Output("decomposition-chart", "figure"),
        Output("yearly-performance-chart", "figure"),
        Output("heatmap-chart", "figure"),
        Output("linear-regression-chart", "figure"),
        Output("forecasts-chart", "figure"),
    ],
    [Input("interval-component", "n_intervals")],
)
def update_all_charts(n):
    """Update all charts"""
    logger.info("Updating charts")
    cassandra_host = os.environ.get("CASSANDRA_HOST", "cassandra-node1")
    keyspace = os.environ.get("CASSANDRA_KEYSPACE", "stock_analysis")
    table = os.environ.get("CASSANDRA_TABLE", "stock_prices")
    df = load_data_from_cassandra(cassandra_host, keyspace, table)

    if df is None:
        logger.info("No data found in Cassandra. Skipping chart update.")
        return (None,) * 11

    return (
        f"Number of samples loaded: {len(df.index)}" if df is not None else "No data loaded",
        charts.create_candlestick_chart(df),
        charts.create_rsi_chart(df),
        charts.create_macd_chart(df),
        charts.create_volume_trend_chart(df),
        charts.create_bollinger_chart(df),
        charts.create_decomposition_chart(df),
        charts.create_yearly_performance_chart(df),
        charts.create_heatmap_chart(df),
        charts.create_linear_regression_chart(df),
        charts.create_forecasts_chart(df)
    )


if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0")
