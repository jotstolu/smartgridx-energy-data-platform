from pathlib import Path
import yaml
import pandas as pd
PROJECT_ROOT = Path(__file__).resolve().parents[2]

def load_config(config_path: "data_generator/config/settings.yml") -> dict:
    full_path = PROJECT_ROOT / config_path
    if not full_path.exists():
        raise FileNotFoundError(f"Config file not found at {full_path}")
    with open(full_path, 'r') as file:
        return yaml.safe_load(file)
    

def ensure_directory(path: Path) -> None:
    """
    Create a directory if it does not already exist.
    """
    path.mkdir(parents=True, exist_ok=True)

from pathlib import Path
import yaml
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_config(config_path: str = "data_generator/config/settings.yml") -> dict:
    """
    Load YAML configuration for the synthetic data generator.
    """
    full_path = PROJECT_ROOT / config_path

    if not full_path.exists():
        raise FileNotFoundError(f"Config file not found: {full_path}")

    with open(full_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def ensure_directory(path: Path) -> None:
    """
    Create a directory if it does not already exist.
    """
    path.mkdir(parents=True, exist_ok=True)


def write_csv(df: pd.DataFrame, output_path: Path) -> None:
    """
    Write dataframe to CSV and create parent folder if needed.
    """
    ensure_directory(output_path.parent)
    df.to_csv(output_path, index=False)


def add_ingestion_metadata(df: pd.DataFrame, source_file_name: str) -> pd.DataFrame:
    df = df.copy()
    df["source_file_name"] = source_file_name
    df["source_system"] = "smart_meter_platform"
    df["generated_at_utc"] = pd.Timestamp.utcnow()
    return df

