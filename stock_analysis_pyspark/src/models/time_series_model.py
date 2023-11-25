import warnings

import pandas as pd

from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX


def arima_forecast(
    df, column: str = "close", p: int = 5, d: int = 1, q: int = 0, step: int = 30
):
    """Forecast time series using ARIMA"""
    time_series = df[["timestamp", column]]
    time_series.index = pd.DatetimeIndex(time_series["timestamp"]).to_period("W")

    # Fit model
    model = ARIMA(time_series[column], order=(p, d, q))
    model_fit = model.fit()

    # Make predictions
    predictions = model_fit.forecast(steps=step)

    # Convert predictions to dataframe
    predictions = pd.DataFrame(predictions)
    predictions.columns = [column]
    predictions.index = pd.date_range(
        start=time_series["timestamp"].max(),
        periods=step,
        freq="W",
    )

    return predictions


def sarimax_forecast(
    df,
    column: str = "close",
    p: int = 5,
    d: int = 1,
    q: int = 0,
    P: int = 1,
    D: int = 1,
    Q: int = 0,
    s: int = 12,
    step: int = 30,
):
    """Forecast time series using SARIMAX"""
    time_series = df[["timestamp", column]]
    time_series.index = pd.DatetimeIndex(time_series["timestamp"]).to_period("W")

    # Fit model
    model = SARIMAX(df[column], order=(p, d, q), seasonal_order=(P, D, Q, s))
    model_fit = model.fit(disp=0)

    # Make predictions
    predictions = model_fit.forecast(steps=step)

    # Convert predictions to dataframe
    predictions = pd.DataFrame(predictions)
    predictions.columns = [column]
    predictions.index = pd.date_range(
        start=time_series["timestamp"].max(),
        periods=step,
        freq="W",
    )

    return predictions
