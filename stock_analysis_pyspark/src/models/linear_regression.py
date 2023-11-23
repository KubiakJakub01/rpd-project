import logging

from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator


logger = logging.getLogger(__name__)


def train_model(df, save_path: str | None = None):
    """Train linear regression model"""
    # Split data into training and test sets
    train, test = df.randomSplit([0.8, 0.2], seed=42)

    # Create vector assembler for feature columns
    assembler = VectorAssembler(
        inputCols=["open", "high", "low", "close", "volume"],
        outputCol="features",
    )

    # Transform the data
    train = assembler.transform(train)
    test = assembler.transform(test)

    # Create and train the model
    model = LinearRegression(featuresCol="features", labelCol="close")
    model = model.fit(train)

    # Evaluate the model
    train_rmse = evaluate_model(model, train)
    test_rmse = evaluate_model(model, test)
    logger.info(f"Train RMSE: {train_rmse:.2f}")
    logger.info(f"Test RMSE: {test_rmse:.2f}")

    if save_path is not None:
        model.save(save_path)

    return model


def evaluate_model(model, df):
    """Evaluate linear regression model"""
    # Evaluate the model
    evaluator = RegressionEvaluator(
        labelCol="close",
        predictionCol="prediction",
        metricName="rmse"
    )
    predictions = model.transform(df)
    rmse = evaluator.evaluate(predictions)

    return rmse


def add_forecast(model, df, periods=30):
    """Add forecast to the dataframe"""
    # Create vector assembler for feature columns
    assembler = VectorAssembler(
        inputCols=["open", "high", "low", "close", "volume"],
        outputCol="features",
    )

    # Transform the data
    df = assembler.transform(df)

    # Add forecast
    forecast = model.transform(df)
    forecast = forecast.withColumnRenamed("prediction", "forecast")
    forecast = forecast.select("timestamp", "forecast")

    # Add forecast to the dataframe
    df = df.join(forecast, on="timestamp", how="left")

    # Add forecast for the next 30 days
    df = df.withColumn("forecast", df["forecast"].cast("double"))

    # Drop features column
    df = df.drop("features")

    return df
