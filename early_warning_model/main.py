
# Utilities
from Utils.model_utils import model_training, FEATURE_COLUMNS
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import pandas as pd
import sys


INPUT_DATA_PATH = "/data/meteo_forecast.csv"


def connect_to_sql_alchemy() -> Engine:
    """
    Create a SQLAlchemy engine connected to the PostgreSQL database.
    """
    db_url = "postgresql+psycopg2://gruppo3:gruppo3@postgres:5432/california_db"
    engine = create_engine(db_url, pool_pre_ping=True)
    return engine


def load_meteorological_data(csv_path: str) -> pd.DataFrame:
    """
    Load meteorological data from a CSV file and parse the time column.

    Args:
        csv_path (str): Path to the input CSV file.

    Returns:
        pd.DataFrame: DataFrame with parsed time column.
    """
    df = pd.read_csv(csv_path)
    df['time'] = pd.to_datetime(df['time'])
    return df


def remap_time_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace the time column with a new range of dates starting from today.

    Args:
        df (pd.DataFrame): Input DataFrame with a 'time' column.

    Returns:
        pd.DataFrame: DataFrame with updated 'time' column.
    """
    unique_times = sorted(df['time'].unique())
    start_date = datetime.today().date()
    new_times = [start_date + timedelta(days=i) for i in range(len(unique_times))]
    time_map = dict(zip(unique_times, new_times))
    df['time'] = df['time'].map(time_map)
    return df


def prepare_features(df: pd.DataFrame, feature_cols: list) -> pd.DataFrame:
    """
    Select and clean feature columns by dropping rows with missing values.

    Args:
        df (pd.DataFrame): Input DataFrame.
        feature_cols (list): List of feature column names.

    Returns:
        pd.DataFrame: Cleaned DataFrame with only the feature columns.
    """
    return df[feature_cols].dropna()


def add_predictions(df: pd.DataFrame, features: pd.DataFrame, model) -> pd.DataFrame:
    """
    Add predictions to the original DataFrame as a new column.

    Args:
        df (pd.DataFrame): Original DataFrame.
        features (pd.DataFrame): Clean feature DataFrame used for predictions.
        model: Trained classification model.

    Returns:
        pd.DataFrame: DataFrame with added 'fire_prediction' column.
    """
    valid_indices = features.index
    predictions = model.predict(features)
    df.loc[valid_indices, 'fire_prediction'] = predictions
    return df


def save_forecast(df: pd.DataFrame) -> None:
    """
    Save the forecast DataFrame to the 'fire_predictions' table using pandas.to_sql with SQLAlchemy.
    """
    engine = None
    try:
        engine = connect_to_sql_alchemy()
        df.to_sql(
            'fire_predictions',
            con=engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        print(f"[INFO] Successfully saved {len(df)} predictions to database")

    except SQLAlchemyError as e:
        print(f"[ERROR] Failed to save predictions: {e}")

    finally:
        if engine:
            try:
                engine.dispose()
            except Exception as e:
                print(f"[WARNING] Failed to dispose engine: {e}")
    

def main():
    
    model = model_training()

    meteo_forecast = load_meteorological_data(INPUT_DATA_PATH)
    processed_meteo_forecast = remap_time_column(meteo_forecast)

    df_features = prepare_features(processed_meteo_forecast, FEATURE_COLUMNS)
    preds = add_predictions(processed_meteo_forecast, df_features, model)
    save_forecast(preds)
    
    exit_code = int(preds["fire_prediction"].max())
    return exit_code


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

