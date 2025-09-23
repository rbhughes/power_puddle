from flask import Blueprint, jsonify
import pandas as pd
import numpy as np

mart_actual_vs_forecast_bp = Blueprint(
    "mart_actual_vs_forecast", __name__, url_prefix="/api"
)


def mase(actual, forecast, seasonality=1):
    actual = np.array(actual)
    forecast = np.array(forecast)
    n = len(actual)
    # Naive forecast: previous value (seasonality=1)
    naive_forecast = actual[:-seasonality]
    naive_actual = actual[seasonality:]
    mae_naive = np.mean(np.abs(naive_actual - naive_forecast))
    mae_model = np.mean(np.abs(actual - forecast))
    if mae_naive == 0:
        return None
    return mae_model / mae_naive


@mart_actual_vs_forecast_bp.route("/actual-vs-forecast")
def get_actual_vs_forecast():
    """
    Endpoint to return mart_actual_vs_forecast data.
    """

    from ..puddle_api import get_db_connection

    conn = get_db_connection()
    # conn.execute("LOAD 'rapidfuzz';")

    try:
        query = """
            SELECT
                report_date,
                forecast_year,
                month,
                actual_generation_gwh,
                forecast_2016,
                forecast_2017,
                forecast_2018,
                forecast_2019,
                forecast_2020,
                forecast_2021,
                forecast_2022,
                forecast_2023
            FROM main_marts.mart_actual_vs_forecast
            ORDER BY forecast_year, month
        """
        df = conn.execute(query).fetchdf()
        # This will replace all NaN and NaT with None for JSON serialization
        df = df.replace({np.nan: None, pd.NaT: None})

        df["report_date"] = pd.to_datetime(df["report_date"]).dt.strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )

        return jsonify(df.to_dict(orient="records"))
    finally:
        conn.close()


@mart_actual_vs_forecast_bp.route("/actual-vs-forecast-mase")
def get_actual_vs_forecast_mase():
    """
    Endpoint to return MASE for each forecast_xxxx series.
    Mean Absolute Scaled Error (MASE) is a measure of forecast accuracy.
    * A MASE of 0.7 = model’s error is 70% of the naïve error, good forecast.
    * A MASE of 2.2 = error is more than twice as large as simply predicting
      the last actual for each step—poor forecast.
    * Lower is better, ideally below 1.

    MASE is a scale-free error metric. Both the numerator (average absolute
    forecast error) and the denominator (average absolute naive error) are
    affected proportionally by multiplicative changes in the actuals:

    If the actual values in the time series are off by a constant factor
    (say, every value is 10% higher or lower than reality), the MASE scores
    for your forecasts will not change—they will remain exactly the same.

    """
    from ..puddle_api import get_db_connection

    conn = get_db_connection()
    conn.execute("INSTALL 'rapidfuzz';")

    try:
        query = """
            SELECT
                report_date,
                actual_generation_gwh,
                forecast_2016,
                forecast_2017,
                forecast_2018,
                forecast_2019,
                forecast_2020,
                forecast_2021,
                forecast_2022,
                forecast_2023
            -- FROM main_marts.mart_actual_vs_forecast_by_name
            FROM main_marts.mart_actual_vs_forecast
            ORDER BY report_date
        """
        df = conn.execute(query).fetchdf()
        df = df.replace({np.nan: None, pd.NaT: None})

        mase_results = {}
        for year in range(2016, 2025):
            forecast_col = f"forecast_{year}"
            if forecast_col in df.columns:
                actual = df["actual_generation_gwh"].values
                forecast = df[forecast_col].values
                mask = (~pd.isnull(actual)) & (~pd.isnull(forecast))
                actual_clean = actual[mask]
                forecast_clean = forecast[mask]
                if len(actual_clean) > 1 and len(forecast_clean) > 1:
                    mase_value = mase(actual_clean, forecast_clean)
                else:
                    mase_value = None
                mase_results[forecast_col] = mase_value

        return jsonify(mase_results)
    finally:
        conn.close()
