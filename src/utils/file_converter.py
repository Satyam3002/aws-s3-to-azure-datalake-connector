"""
File Conversion Utility
Handles conversion of CSV/JSON files to Parquet format.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Tuple, List


def _read_csv_with_fallbacks(path: str, encodings: List[str]) -> Tuple[bool, str, pd.DataFrame | None]:
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc)
            return True, enc, df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            return False, f"CSV read error: {str(e)}", None
    return False, "CSV decode error: tried encodings: " + ", ".join(encodings), None


def _read_json_with_fallbacks(path: str, encodings: List[str]) -> Tuple[bool, str, pd.DataFrame | None]:
    for enc in encodings:
        try:
            df = pd.read_json(path, encoding=enc, lines=False)
            return True, enc, df
        except UnicodeDecodeError:
            continue
        except ValueError:
            # Try newline-delimited JSON
            try:
                df = pd.read_json(path, encoding=enc, lines=True)
                return True, enc, df
            except Exception:
                continue
        except Exception as e:
            return False, f"JSON read error: {str(e)}", None
    return False, "JSON decode error: tried encodings: " + ", ".join(encodings), None


def convert_to_parquet(input_file: str, output_file: str) -> Tuple[bool, str]:
    """
    Convert CSV or JSON file to Parquet format.
    
    Args:
        input_file: Path to input file (CSV or JSON)
        output_file: Path where Parquet file will be saved
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        input_path = Path(input_file)
        
        # Check if file exists
        if not input_path.exists():
            return False, f"Input file not found: {input_file}"
        
        # Determine file type by extension
        file_ext = input_path.suffix.lower()

        # Common encodings to try when UTF-8 fails
        encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin1"]

        # Read file based on extension with encoding fallbacks
        if file_ext == '.csv':
            ok, used, df_or_msg = _read_csv_with_fallbacks(input_file, encodings_to_try)
            if not ok:
                return False, f"Conversion error: {df_or_msg}"
            df = df_or_msg
        elif file_ext == '.json':
            ok, used, df_or_msg = _read_json_with_fallbacks(input_file, encodings_to_try)
            if not ok:
                return False, f"Conversion error: {df_or_msg}"
            df = df_or_msg
        else:
            return False, f"Unsupported file type: {file_ext}. Only CSV and JSON are supported."
        
        # Write to Parquet
        df.to_parquet(output_file, engine='pyarrow', index=False)
        
        # Get file sizes for info
        input_size = input_path.stat().st_size / (1024 * 1024)  # MB
        output_path = Path(output_file)
        output_size = output_path.stat().st_size / (1024 * 1024)  # MB
        
        return True, f"Converted to Parquet: {input_size:.2f} MB → {output_size:.2f} MB"
        
    except pd.errors.EmptyDataError:
        return False, "Input file is empty or invalid."
    except Exception as e:
        return False, f"Conversion error: {str(e)}"


def get_file_extension(file_path: str) -> str:
    """
    Get file extension from file path.
    
    Args:
        file_path: Path to file
    
    Returns:
        File extension (lowercase, without dot)
    """
    return Path(file_path).suffix.lower().lstrip('.')

