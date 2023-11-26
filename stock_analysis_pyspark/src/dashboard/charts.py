import plotly.figure_factory as ff
import plotly.graph_objs as go
from plotly.subplots import make_subplots

from ..feature_engineering import decompose, monthly_yearly_performance
from ..models.time_series_model import arima_forecast, sarimax_forecast


def create_candlestick_chart(df):
    """Create a candlestick chart with volume bars"""
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
    return candlestick_fig


def create_rsi_chart(df):
    """Create a Relative Strength Index (RSI) chart"""
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
    return rsi_fig


def create_macd_chart(df):
    """Create a Moving Average Convergence Divergence (MACD) chart"""
    macd_fig = go.Figure(
        data=go.Bar(
            x=df["timestamp"],
            y=df["macd_histogram_close"],
            marker_color="red",
            name="MACD Histogram",
        )
    )
    macd_fig.update_layout(title="MACD Histogram Chart")
    return macd_fig


def create_volume_trend_chart(df):
    """Create a Volume Trend chart"""
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
    return volume_trend_fig


def create_bollinger_chart(df):
    """Create a Bollinger Bands chart"""
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
    return bollinger_fig


def create_decomposition_chart(df):
    """Create a Time Series Decomposition chart"""
    try:
        decomposition_df = decompose(df, column="close", period=365)
    except ValueError:
        return None
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
    return decomposition_fig


def create_yearly_performance_chart(df):
    """Create a Yearly Performance chart"""
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
    return yearly_performance_fig


def create_heatmap_chart(df):
    """Create a Correlation Heatmap chart"""
    correlation_matrix = df[
        [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "rsi",
            "ma_macd_close",
            "macd_histogram_close",
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
    return heatmap_fig


def create_linear_regression_chart(df):
    """Create a Linear Regression chart"""
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
    return linear_regression_fig


def create_forecasts_chart(df):
    """Create an ARIMA and SARIMAX forecasts chart"""
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
    return forecasts_fig
