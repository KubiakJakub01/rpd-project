import argparse
import os

import dash
import pandas as pd
import plotly.figure_factory as ff
import plotly.graph_objs as go
from dash import dcc, html
from plotly.subplots import make_subplots

from ..feature_engineering import decompose, monthly_yearly_performance
from ..models.time_series_model import arima_forecast, sarimax_forecast


# Parse command line arguments
def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_path",
        type=str,
        default=os.path.join(os.getcwd(), "data", "stock_prices.csv"),
        help="Path to the data file",
    )
    args = parser.parse_args()
    return args


# Run the app
if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()

    # Load your preprocessed data
    df = pd.read_csv(args.data_path)

    # Initialize the Dash app
    app = dash.Dash(__name__)

    # Create a combined Candlestick and Volume chart
    candlestick_fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.7, 0.3]
    )
    candlestick_fig.add_trace(
        go.Candlestick(
            x=df["timestamp"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
        ),
        row=1,
        col=1,
    )
    candlestick_fig.add_trace(
        go.Bar(x=df["timestamp"], y=df["volume"], marker_color="blue"), row=2, col=1
    )
    candlestick_fig.update_layout(
        title="Candlestick and Volume Chart", xaxis_rangeslider_visible=False
    )

    # Create an RSI chart
    rsi_fig = go.Figure(
        data=go.Scatter(x=df["timestamp"], y=df["rsi"], mode="lines", name="RSI")
    )
    rsi_fig.add_hline(
        y=30,
        line_dash="dash",
        annotation_text="Oversold",
        annotation_position="bottom right",
        line_color="green",
    )
    rsi_fig.add_hline(
        y=70,
        line_dash="dash",
        annotation_text="Overbought",
        annotation_position="bottom right",
        line_color="green",
    )
    rsi_fig.add_hline(
        y=50,
        line_dash="dash",
        annotation_text="Midpoint",
        annotation_position="bottom right",
    )
    rsi_fig.update_layout(title="RSI Chart", yaxis=dict(range=[0, 100]))

    # Create a MACD Histogram chart
    macd_fig = go.Figure(
        data=go.Bar(
            x=df["timestamp"],
            y=df["MACD_histogram_close"],
            marker_color="red",
            name="MACD Histogram",
        )
    )
    macd_fig.update_layout(title="MACD Histogram Chart")

    # Create a Volume Trend Chart
    volume_trend_fig = make_subplots(specs=[[{"secondary_y": True}]])
    volume_trend_fig.add_trace(
        go.Bar(x=df["timestamp"], y=df["volume"], name="Volume", marker_color="green"),
        secondary_y=False,
    )
    volume_trend_fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["close"],
            name="Close",
            mode="lines",
            line=dict(color="blue"),
        ),
        secondary_y=True,
    )
    volume_trend_fig.update_layout(title="Volume and Close Price Trend")
    volume_trend_fig.update_yaxes(title_text="Volume", secondary_y=False)
    volume_trend_fig.update_yaxes(title_text="Close Price", secondary_y=True)

    # Create a Bollinger Bands Chart
    bollinger_fig = go.Figure()
    bollinger_fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["bollinger_bands_upper_close"],
            line=dict(color="orange"),
            name="Upper Bollinger Band",
        )
    )
    bollinger_fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["bollinger_bands_lower_close"],
            line=dict(color="orange"),
            name="Lower Bollinger Band",
            fill="tonexty",
        )
    )
    bollinger_fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["close"],
            line=dict(color="blue"),
            name="Close Price",
        )
    )
    bollinger_fig.update_layout(title="Bollinger Bands Chart")

    # Create a Decomposition Chart
    decomposition_df = decompose(df, column="close", period=365)
    decomposition_fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.02
    )
    decomposition_fig.add_trace(
        go.Scatter(
            x=decomposition_df["timestamp"],
            y=decomposition_df["trend"],
            mode="lines",
            name="Trend",
        ),
        row=1,
        col=1,
    )
    decomposition_fig.add_trace(
        go.Scatter(
            x=decomposition_df["timestamp"],
            y=decomposition_df["seasonal"],
            mode="lines",
            name="Seasonal",
        ),
        row=2,
        col=1,
    )
    decomposition_fig.add_trace(
        go.Scatter(
            x=decomposition_df["timestamp"],
            y=decomposition_df["residual"],
            mode="lines",
            name="Residual",
        ),
        row=3,
        col=1,
    )
    decomposition_fig.update_layout(height=600, title_text="Time Series Decomposition")

    # Create a Monthly and Yearly Performance Chart
    monthly_performance, yearly_performance = monthly_yearly_performance(
        df, column="close"
    )
    yearly_performance_fig = go.Figure()
    yearly_performance_fig.add_trace(
        go.Scatter(
            x=yearly_performance["year"],
            y=yearly_performance["close"],
            mode="lines+markers",
            name="Yearly Performance",
        )
    )
    yearly_performance_fig.update_layout(
        title="Historical Yearly Performance",
        xaxis_title="Time",
        yaxis_title="Average Close Price",
    )

    # Create the Correlation Heatmap chart
    correlation_matrix = df[
        [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "rsi",
            "MA_MACD_close",
            "MACD_histogram_close",
            "bollinger_bands_upper_close",
            "bollinger_bands_lower_close",
        ]
    ].corr()
    heatmap_fig = ff.create_annotated_heatmap(
        z=correlation_matrix.to_numpy(),
        x=correlation_matrix.columns.tolist(),
        y=correlation_matrix.columns.tolist(),
        colorscale="Viridis",
        showscale=True,
        annotation_text=correlation_matrix.round(2).to_numpy().astype(str),
    )
    heatmap_fig.update_layout(
        title="Correlation Heatmap", xaxis_title="Features", yaxis_title="Features"
    )

    # Plot the Linear Regression forecast from 'forecast' column
    linear_regression_fig = go.Figure()
    linear_regression_fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["close"],
            mode="lines",
            name="Close Price",
        )
    )
    linear_regression_fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["forecast"],
            mode="lines",
            name="Forecast",
        )
    )
    linear_regression_fig.update_layout(title="Linear Regression Chart")

    # Get ARIMA and SARIMAX forecasts
    arima_predictions = arima_forecast(df)
    sarimax_predictions = sarimax_forecast(df)
    forecasts_fig = go.Figure()
    forecasts_fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["close"],
            mode="lines",
            name="Close Price",
        )
    )
    forecasts_fig.add_trace(
        go.Scatter(
            x=arima_predictions.index,
            y=arima_predictions["close"],
            mode="lines",
            name="ARIMA Forecast",
        )
    )
    forecasts_fig.add_trace(
        go.Scatter(
            x=sarimax_predictions.index,
            y=sarimax_predictions["close"],
            mode="lines",
            name="SARIMAX Forecast",
        )
    )
    forecasts_fig.update_layout(title="ARIMA Forecast")
   

    # Define the layout of the app
    app.layout = html.Div(
        [
            html.H1("Stock Market Dashboard"),
            html.Div("Interactive stock data visualization."),
            dcc.Graph(figure=candlestick_fig),
            dcc.Graph(figure=rsi_fig),
            dcc.Graph(figure=macd_fig),
            dcc.Graph(figure=volume_trend_fig),
            dcc.Graph(figure=bollinger_fig),
            dcc.Graph(figure=decomposition_fig),
            dcc.Graph(figure=yearly_performance_fig),
            dcc.Graph(figure=heatmap_fig),
            dcc.Graph(figure=linear_regression_fig),
            dcc.Graph(figure=forecasts_fig),
        ]
    )

    app.run_server(debug=True)
