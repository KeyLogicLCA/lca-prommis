"""
OpenLCA Process Creation Package

This package provides modular functionality for creating processes in openLCA from DataFrames.

Modules:
- olca_utils: Basic utilities, ID generation, connection management
- flow_manager: Flow creation, searching, and management
- unit_manager: Unit creation, searching, and management
- process_manager: Process creation and management
- dataframe_utils: DataFrame processing utilities
- search_utils: Search functionality for flows and processes
- user_interface: User interaction and menu systems
- main_script: Main orchestration script

Requirements:
- openLCA software must be running with IPC developer tool activated
- olca-ipc package: pip install olca-ipc
- olca-schema package: pip install olca-schema
- pandas package: pip install pandas

Usage:
    python main_script.py

Author: Automated Script Modularization
Version: 1.0.0
"""

__version__ = "1.0.0"
__author__ = "Automated Script Modularization"

# Import main components for easier access
try:
    from .olca_utils import initialize_client, generate_id, print_database_summary
    from .dataframe_utils import create_sample_dataframe, load_dataframe_from_csv
    from .process_manager import create_process_from_dataframe_with_selection
    
    __all__ = [
        'initialize_client',
        'generate_id', 
        'print_database_summary',
        'create_sample_dataframe',
        'load_dataframe_from_csv',
        'create_process_from_dataframe_with_selection'
    ]
    
except ImportError as e:
    print(f"Warning: Could not import all modules: {e}")
    __all__ = []