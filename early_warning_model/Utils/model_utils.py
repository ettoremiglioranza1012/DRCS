
# Utilities
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sqlalchemy.exc import SQLAlchemyError
from imblearn.over_sampling import SMOTE
from psycopg2 import OperationalError
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import pandas as pd
import time


# List of meteorological features used for training
FEATURE_COLUMNS = [
    'u10', 'v10', 't2m', 'd2m', 'msl', 'sst', 'sp',
    'u100', 'v100', 'stl1', 'swvl1', 'cvh'
]


def connect_to_sql_alchemy() -> Engine:
    """
    Create a SQLAlchemy engine connected to the PostgreSQL database.
    """
    db_url = "postgresql+psycopg2://gruppo3:gruppo3@postgres:5432/california_db"
    engine = create_engine(db_url, pool_pre_ping=True)
    return engine


def connect_to_db_with_retry(max_retries=30, delay=2):
    """
    Connects to the database with retry logic.
    """
    for attempt in range(max_retries):
        try:
            conn = connect_to_sql_alchemy()
            print(f"[INFO] Successfully connected to database on attempt {attempt + 1}")
            return conn
        except OperationalError as e:
            print(f"[INFO] Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                print(f"[INFO] Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("[ERROR] Max retries reached. Unable to connect to database.")
                raise
        except Exception as e:
            print(f"[ERROR] Unexpected error on attempt {attempt + 1}: {e}")
            raise


def load_historic_meteo_data() -> pd.DataFrame | None:
    """
    Loads historical meteorological data from the database into a pandas DataFrame using SQLAlchemy.
    """
    engine: Engine = None

    try:
        engine = connect_to_sql_alchemy()  # Must return a SQLAlchemy Engine
        with engine.connect() as conn:
            df = pd.read_sql_query("SELECT * FROM meteo_hist", conn)
            return df

    except SQLAlchemyError as e:
        print(f"[ERROR] Failed to connect or read from the database: {e}")
        return None

    finally:
        if engine:
            try:
                engine.dispose()
            except Exception as e:
                print(f"[WARNING] Final cleanup failed: {e}")


def prepare_data(df: pd.DataFrame, feature_cols: list) -> tuple:
    """
    Extract the feature matrix (X) and target vector (y) from the dataset.
    Drops rows with missing values in any of the feature columns.

    Args:
        df (pd.DataFrame): Input DataFrame containing features and target.
        feature_cols (list): List of column names to use as features.

    Returns:
        tuple: A tuple (X, y) with the features and labels.
    """
    X = df[feature_cols].dropna()
    y = df.loc[X.index, 'fire_class']
    return X, y


def split_and_resample(X: pd.DataFrame, y: pd.Series) -> tuple:
    """
    Split the dataset into training and testing sets, and apply SMOTE 
    to balance the classes in the training set.

    Args:
        X (pd.DataFrame): Feature matrix.
        y (pd.Series): Target vector.

    Returns:
        tuple: (X_train_resampled, X_test, y_train_resampled, y_test)
    """
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )
    smote = SMOTE(random_state=42)
    X_train_resampled, y_train_resampled = smote.fit_resample(X_train, y_train)
    return X_train_resampled, X_test, y_train_resampled, y_test


def train_random_forest(X_train: pd.DataFrame, y_train: pd.Series, n_estimators: int = 100) -> RandomForestClassifier:
    """
    Train a Random Forest classifier using the training data.

    Args:
        X_train (pd.DataFrame): Training feature matrix.
        y_train (pd.Series): Training labels.
        n_estimators (int): Number of trees in the forest (default: 100).

    Returns:
        RandomForestClassifier: The trained classifier.
    """
    clf = RandomForestClassifier(n_estimators=n_estimators, random_state=42)
    clf.fit(X_train, y_train)
    return clf


def model_training():
    """
    Main execution flow:
    - Load and prepare data
    - Check class distribution
    - Train and evaluate Random Forest classifier
    """
    df_model = None
    
    while df_model is None:
        df_model = load_historic_meteo_data()
        if df_model is not None and not df_model.empty:
            break
        print("[INFO] Training interrupted cause data wasn't available. Trying again in 10s...")
        time.sleep(10)
        df_model = None 
    
    print("[INFO] Scheduled training inizitialize: training...")
    X, y = prepare_data(df_model, FEATURE_COLUMNS)
    X_train, _, y_train, _ = split_and_resample(X, y)
    clf = train_random_forest(X_train, y_train)
    print("[INFO] Model training complete, returning model.")
    
    return clf

