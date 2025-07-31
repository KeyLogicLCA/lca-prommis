#!/usr/bin/env python3
"""
DataFrame Utilities Module

This module provides utilities for working with DataFrames containing LCA data.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def get_amount_from_dataframe_row(row: pd.Series) -> float:
    """
    Get amount from DataFrame row.
    
    Args:
        row (pd.Series): DataFrame row containing amount information
    
    Returns:
        float: Amount value
    """
    amount = row.get('LCA_Amount', row.get('Amount', 1.0))
    
    # Convert to float if it's a string
    if isinstance(amount, str):
        try:
            amount = float(amount)
        except ValueError:
            logger.warning(f"Could not convert amount '{amount}' to float, using 1.0")
            amount = 1.0
    
    return float(amount)

# Currently not used - this function is called when the user imports a csv file
# As explained below, the dataframe is created from previous steps after extracting the results from the PrOMMiS model
def validate_dataframe_columns(df: pd.DataFrame) -> bool:
    """
    Validate that the DataFrame has the required columns for LCA data.
    
    Args:
        df (pd.DataFrame): DataFrame to validate
    
    Returns:
        bool: True if DataFrame is valid, False otherwise
    """
    required_columns = ['Flow_Name', 'LCA_Unit', 'LCA_Amount']
    optional_columns = ['Is_Input', 'Reference_Product', 'Flow_Type', 'Description']
    
    missing_required = []
    for col in required_columns:
        if col not in df.columns:
            missing_required.append(col)
    
    if missing_required:
        logger.error(f"DataFrame missing required columns: {missing_required}")
        return False
    
    logger.info(f"DataFrame validation passed. Required columns found: {required_columns}")
    
    missing_optional = []
    for col in optional_columns:
        if col not in df.columns:
            missing_optional.append(col)
    
    if missing_optional:
        logger.info(f"Optional columns not found (will use defaults): {missing_optional}")
    
    return True

# Optional - not needed - currently the dataframe is created from 
# previous steps after extracting the results from the PrOMMiS model
def load_dataframe_from_csv(file_path: str) -> pd.DataFrame:
    """
    Load DataFrame from CSV file with validation.
    
    Args:
        file_path (str): Path to the CSV file
    
    Returns:
        pd.DataFrame: Loaded DataFrame
    
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If DataFrame validation fails
    """
    try:
        df = pd.read_csv(file_path)
        logger.info(f"✅ Loaded DataFrame from {file_path} with {len(df)} rows")
        
        if validate_dataframe_columns(df):
            return df
        else:
            raise ValueError("DataFrame validation failed")
            
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading DataFrame from {file_path}: {e}")
        raise




