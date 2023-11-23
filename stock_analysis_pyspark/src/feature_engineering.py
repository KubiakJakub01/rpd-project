import pyspark.sql.functions as F
import statsmodels.api as sm
from pyspark.sql.functions import mean as _mean
from pyspark.sql.window import Window
import pandas as pd


def add_technical_indicators(df):
    """Add technical indicators"""
    df = moving_average(df)
    df = relative_strength_index(df)
    df = moving_average_convergence_divergence(df)
    df = bollinger_bands(df)
    return df


def add_time_features(df):
    """Add time features"""
    df = df.withColumn("year", F.year("timestamp"))
    df = df.withColumn("month", F.month("timestamp"))
    df = df.withColumn("week", F.weekofyear("timestamp"))
    df = df.withColumn("day", F.dayofmonth("timestamp"))
    df = df.withColumn("dayofweek", F.dayofweek("timestamp"))
    df = df.withColumn("dayofyear", F.dayofyear("timestamp"))
    df = df.withColumn("hour", F.hour("timestamp"))
    df = df.withColumn("minute", F.minute("timestamp"))
    df = df.withColumn("second", F.second("timestamp"))
    df = df.withColumn("quarter", F.quarter("timestamp"))
    df = df.withColumn("is_month_start", F.dayofmonth("timestamp") == 1)
    df = df.withColumn("is_month_end", F.dayofmonth("timestamp") == 31)
    df = df.withColumn("is_year_start", F.dayofyear("timestamp") == 1)
    df = df.withColumn("is_year_end", F.dayofyear("timestamp") == 365)
    df = df.withColumn("is_leap_year", F.year("timestamp") % 4 == 0)
    df = df.withColumn("is_weekend", F.dayofweek("timestamp").isin([1, 7]))
    return df


def moving_average(
    df, column: str = "close", sorted_by: str = "timestamp", window_size=10
):
    """Calculate moving average"""
    df = df.withColumn(
        f"MA_{column}",
        _mean(column).over(
            window=Window.partitionBy("year").orderBy(sorted_by).rowsBetween(-window_size, 0)
        ),
    )
    return df


def relative_strength_index(df, column: str = "close", window_size=14):
    """Calculate Relative Strength Index (RSI)"""
    window = Window.partitionBy("year").orderBy("timestamp")

    # Calculate daily change
    df = df.withColumn("daily_change", F.lag(df[column]).over(window) - df[column])

    # Separate gains and losses
    df = df.withColumn(
        "gain", F.when(df["daily_change"] > 0, df["daily_change"]).otherwise(0)
    )
    df = df.withColumn(
        "loss", F.when(df["daily_change"] < 0, -df["daily_change"]).otherwise(0)
    )

    # Calculate average gain and loss
    window_rsi = Window.partitionBy("year").orderBy("timestamp").rowsBetween(-window_size, 0)
    df = df.withColumn("avg_gain", F.avg(df["gain"]).over(window_rsi))
    df = df.withColumn("avg_loss", F.avg(df["loss"]).over(window_rsi))

    # Calculate RS and RSI
    df = df.withColumn("rs", df["avg_gain"] / df["avg_loss"])
    df = df.withColumn("rsi", 100 - (100 / (1 + df["rs"])))

    # Drop intermediate columns
    df = df.drop("daily_change", "gain", "loss", "avg_gain", "avg_loss", "rs")
    return df


def moving_average_convergence_divergence(
    df,
    column: str = "close",
    sorted_by: str = "timestamp",
    window_slow=26,
    window_fast=12,
    window_macd=9,
):
    """Calculate Moving Average Convergence Divergence (MACD)"""
    df = moving_average(df, column, sorted_by, window_fast)
    df = df.withColumn(f"EMA_{column}_fast", df[f"MA_{column}"])
    df = moving_average(df, column, sorted_by, window_slow)
    df = df.withColumn(f"EMA_{column}_slow", df[f"MA_{column}"])
    df = df.withColumn(
        f"MACD_{column}",
        df[f"EMA_{column}_fast"] - df[f"EMA_{column}_slow"],
    )
    df = moving_average(df, f"MACD_{column}", sorted_by, window_macd)
    df = df.withColumn(f"MACD_signal_{column}", df[f"MA_MACD_{column}"])
    df = df.withColumn(
        f"MACD_histogram_{column}",
        df[f"MACD_{column}"] - df[f"MACD_signal_{column}"],
    )
    df = df.drop(
        f"MA_{column}",
        f"EMA_{column}_fast",
        f"EMA_{column}_slow",
        f"MACD_{column}",
        f"MACD_signal_{column}",
    )
    return df


def volatility(df, column: str = "close", window_size=10):
    """Calculate volatility"""
    df = df.withColumn(
        f"volatility_{column}",
        _mean(column).over(
            window=Window.partitionBy("year")
            .orderBy("timestamp")
            .rowsBetween(-window_size, 0)
        ),
    )
    return df


def bollinger_bands(df, column: str = "close", window_size=10):
    """Calculate Bollinger Bands"""
    df = volatility(df, column, window_size)
    df = df.withColumn(
        f"bollinger_bands_upper_{column}", df[f"volatility_{column}"] * 2
    )
    df = df.withColumn(
        f"bollinger_bands_lower_{column}", df[f"volatility_{column}"] * -2
    )
    df = df.drop(f"volatility_{column}")
    return df


def decompose(df, column: str = "close", period: int = 365):
    """Decompose time series
    
    Args:
        df (pd.DataFrame): dataframe
        column (str): column name
        period (int): period of decomposition"""
    decomposition = sm.tsa.seasonal_decompose(df[column], model='additive', period=period)
    trend = decomposition.trend
    seasonal = decomposition.seasonal
    residual = decomposition.resid
    decomposed_df = pd.concat([df['timestamp'], trend, seasonal, residual], axis=1)
    decomposed_df.columns = ['timestamp', 'trend', 'seasonal', 'residual']
    return decomposed_df


def monthly_yearly_performance(df, column: str = "close"):
    """Calculate monthly and yearly performance"""
    # Calculate monthly performance
    monthly_performance = df.groupby(['year', 'month']).agg({column: 'mean'}).reset_index()

    # Optionally, calculate yearly performance similarly
    yearly_performance = df.groupby('year').agg({'close': 'mean'}).reset_index()

    return monthly_performance, yearly_performance
