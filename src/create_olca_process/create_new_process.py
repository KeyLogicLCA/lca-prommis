########################################################################################################
# This script creates a new process in openLCA
# This code builds on three main existing libraries:
    # 1. netlolca
    # 2. olca_schema
    # 3. olca_ipc

# Import libraries
########################################################################################################
# Import libraries
import json
from sys import exception
import pandas as pd
import olca_schema as olca
import olca_ipc
import netlolca
from netlolca import NetlOlca
import logging
import re
import uuid
import datetime
from typing import List, Optional, Tuple, Union
# Import functions from other modules
from src.create_olca_process.search_flows_and_providers import search_and_select
from src.create_olca_process.create_exchange_elementary_flow import create_exchange_elementary_flow
from src.create_olca_process.create_exchange_pr_wa_flow import create_exchange_pr_wa_flow
from src.create_olca_process.create_exchange_database import create_exchange_database
from src.create_olca_process.create_exchange_ref_flow import create_exchange_ref_flow
import olca_schema.units as o_units

logger = logging.getLogger(__name__)


########################################################
# Define main function
########################################################
# Arguments:
    # a. Dataframe with process data
    # b. Process name
    # c. Process description

def create_new_process(client, df, process_name, process_description):
    # client is initialized before running this function
    # client = olca_ipc.Client()
    # 1. Read dataframe and review its structure
    df = read_dataframe(df)

    # 2. Create empty process
    process = create_empty_process(client, process_name, process_description)
    # TODO: use function from netlolca to create a new process

    # 3. Create exchange database
    print('Creating exchange database, this may take a couple minutes...')
    exchange_database = create_exchange_database(client)

    # 4. Create exchanges
    exchanges = []

    # Loop through the dataframe, find reference product, and create exchange for it
    for index, row in df.iterrows():
        # Gives you the option to try again if you make a mistake
        while True:
            try:
                product = row['Flow_Name']
                unit = row['LCA_Unit']
                amount = row['LCA_Amount']
                is_input = row['Is_Input']
                flow_uuid = row['UUID']
                # TODO: add a check to see if there is more than one reference product. Just want to have a warning printed.
                if row['Reference_Product']:
                    exchange = create_exchange_ref_flow(client, product, amount, unit, is_input, row['Reference_Product'])
                    exchanges.append(exchange)
                    break                     # If reference flow, then we don't need to search for a process.
                else:
                    # If not elementary flow, the we need to identify flow category, search for a flow and process/provider to create an exchange
                    if row['Category'].lower() == 'elementary flows':
                        exchange = create_exchange_elementary_flow(client, flow_uuid, unit, amount, is_input)
                        exchanges.append(exchange)
                        break  # If the flow is an elementary flow, the we create an exchange and move to the next row
                    
                    # If product flow, then we need to search for a process 
                    elif row['Category'].lower() == 'technosphere flows' or row['Category'].lower() == 'product flows':
                        flow_uuid, provider_uuid = search_and_select(exchanges_df=exchange_database, keywords=product, flow_type_str='product', client=client, unit=unit)
                        # Allows user to skip the flow
                        if flow_uuid == 'skip':
                            print(f"Skipping flow: {product}")
                            break
                        
                        exchange = create_exchange_pr_wa_flow(client, flow_uuid, provider_uuid, amount, unit, is_input)
                        exchanges.append(exchange)
                        break  # If the flow is an technosphere flow, the we create an exchange and move to the next row
                    
                    # If waste flow, then we need to search for a process.
                    elif row['Category'].lower() == 'waste flows':
                        flow_uuid, provider_uuid = search_and_select(exchanges_df=exchange_database, keywords=product, flow_type_str='waste', client=client, unit=unit)
                        # Allows user to skip the flow
                        if flow_uuid == 'skip':
                            print(f"Skipping flow: {product}")
                            break
                        
                        exchange = create_exchange_pr_wa_flow(client, flow_uuid, provider_uuid, amount, unit, is_input)
                        exchanges.append(exchange)
                        break  # If the flow is an waste flow, the we create an exchange and exit loop  
                    else:
                        raise ValueError(f"Invalid category: {row['Category']}. Must be one of: elementary flows, product flows, technosphere flows, waste flows.")                    
            #Add handle errors if the row is missing a required column: product, amount, unit, is_input, reference_product, and/or category         
            except Exception as e:
                print(f"Error creating exchange for flow: {e}")
                retry_response = input("Do you want to try again? (y/n): ").strip()
                if retry_response.lower().startswith('y'):
                    continue
                elif retry_response.lower().startswith('n'):
                    break

    # 5. Create process
    process.exchanges = exchanges
    

    # 6. Save process to openLCA
    created_process = client.client.put(process)
    print(f"Successfully created process: {process_name}")
    print(f"Process saved successfully to openLCA database!")    
    return created_process

########################################################
# Define helper functions
########################################################

# Read dataframe and review its structure
########################################################

def read_dataframe(df):
    # Read dataframe - handle both file path and DataFrame object
    if isinstance(df, str):
        # If df is a string (file path), read the CSV file
        df = pd.read_csv(df)
    elif isinstance(df, pd.DataFrame):
        # If df is already a DataFrame, use it directly
        pass
    else:
        raise TypeError("df must be either a file path (string) or a pandas DataFrame")
    
    # Validate structure
    # The dataframe should have the following columns:
    # Flow_Name, LCA_Amount, LCA_Unit, Is_Input, Reference_Product, Flow_Type

    required_columns = ['Flow_Name', 'LCA_Amount', 'LCA_Unit', 'Is_Input', 'Reference_Product', 'Flow_Type']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"The dataframe must have the following columns: {required_columns}")
    return df

# Create empty process 
########################################################
def create_empty_process(client, process_name, process_description):
    process_id = generate_id("process")
    process = olca.Process(
        id=process_id,
        name=process_name,
        description=process_description,
        process_type=olca.ProcessType.UNIT_PROCESS,
        version="1.0.0",
        last_change=datetime.datetime.now().isoformat()
    )
    
    return process

# Generate ID
########################################################

def generate_id(prefix: str = "entity") -> str:
    """
    Generate a unique ID for openLCA entities.
    
    Args:
        prefix (str): Prefix for the ID (e.g., 'process', 'flow', 'unit') - Note: prefix is ignored to comply with database VARCHAR(36) limit
    
    Returns:
        str: Unique ID (36-character UUID string)
    """
    return str(uuid.uuid4())

