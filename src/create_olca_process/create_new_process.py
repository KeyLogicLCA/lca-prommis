#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# create_new_process.py
#
###############################################################################
# DEPENDENCIES
###############################################################################
import logging
import uuid
import datetime

import pandas as pd
import olca_schema as olca
from olca_schema import ParameterScope

from src.create_olca_process.search_flows_and_providers import search_and_select
from src.create_olca_process.create_exchange_elementary_flow import create_exchange_elementary_flow
from src.create_olca_process.create_exchange_pr_wa_flow import create_exchange_pr_wa_flow
from src.create_olca_process.create_exchange_database import create_exchange_database
from src.create_olca_process.create_exchange_ref_flow import create_exchange_ref_flow


###############################################################################
# DOCUMENTATION
###############################################################################
__doc__ = """
This script creates a new process in openLCA.

This code builds on three main existing libraries:

1.  netlolca
2.  olca_schema
3.  olca_ipc
"""
__all__ = [
    "create_empty_process",
    "create_new_process",
    "generate_id",
    "read_dataframe",
]


###############################################################################
# GLOBALS
###############################################################################
logger = logging.getLogger(__name__)



###############################################################################
# FUNCTIONS
###############################################################################
def create_new_process(client, df, process_name, process_description):
    """Create a new process in openLCA.

    Parameters
    ----------
    client : NetlOlca
        A NetlOlca class instance, connected to IPC service.
    df : pandas.DataFrame
        A data frame with process data.
    process_name : str
        Process name.
    process_description : str
        Process description.

    Returns
    -------
    olca-schema.Ref
        A reference object for the newly created process.

    Raises
    ------
    ValueError
        Invalid category found in data frame.
    """
    # Note: client is initialized before running this function, for example:
    #   client = olca_ipc.Client()

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
    parameters = []
    parameters_table = pd.DataFrame(columns=['parameter_name', 'parameter_description', 'parameter_value'])
    count = 0

    # Loop through the dataframe, find reference product, and create exchanges
    for _, row in df.iterrows():
        count+=1
        # Gives you the option to try again if you make a mistake
        while True:
            try:
                product = row['Flow_Name']
                unit = row['LCA_Unit']
                amount = row['LCA_Amount']
                is_input = row['Is_Input']
                flow_uuid = row['UUID']
                parameter = create_parameter(f"p{count}", f"Reference parameter for {product}",'', True, ParameterScope("PROCESS_SCOPE"), amount)
                parameters.append(parameter)                
                # TODO: add a check to see if there is more than one reference
                # product. Just want to have a warning printed.
                if row['Reference_Product']:
                    print("\n")
                    print(f"Creating exchange for reference product: {product}")
                    print("----------------------------------------")
                    exchange = create_exchange_ref_flow(client, product, amount, parameter.name, unit, is_input, row['Reference_Product'])
                    exchanges.append(exchange)
                    parameters_table.loc[count-1, ('parameter_name', 'parameter_description', 'parameter_value')] = (f"p{count}", product, amount)
                    # If reference flow, then we don't need to search for a
                    # process.
                    break
                else:
                    # If not elementary flow, the we need to identify flow
                    # category, search for a flow and process/provider to
                    # create an exchange.
                    if row['Category'].lower() == 'elementary flows':
                        print("\n")
                        print(f"Creating exchange for elementary flow: {product}")
                        print("--------------------------------------")
                        try:
                            exchange = create_exchange_elementary_flow(
                                client, flow_uuid, unit, amount, parameter.name, is_input
                            )
                            print(
                                "Exchange created for elementary flow: "
                                f"{product}"
                            )
                            exchanges.append(exchange)
                            parameters_table.loc[count-1, ('parameter_name', 'parameter_description', 'parameter_value')] = (f"p{count}", product, amount)
                            break
                        except Exception as e:
                            print(
                                "Error creating exchange for elementary "
                                f"flow: {e}"
                            )
                            break

                    # If product flow, then we need to search for a process
                    elif (row['Category'].lower() == 'technosphere flows'
                            or row['Category'].lower() == 'product flows'):
                        print("\n")
                        print(f"Creating exchange for product flow: {product}")
                        print("-----------------------------------")
                        flow_uuid, provider_uuid = search_and_select(
                            exchanges_df=exchange_database,
                            keywords=product,
                            flow_type_str='product',
                            client=client,
                            unit=unit
                        )
                        # Allows user to skip the flow
                        if flow_uuid == 'skip':
                            print(f"Skipping flow: {product}")
                            break
                        try:
                            exchange = create_exchange_pr_wa_flow(
                                client,
                                flow_uuid,
                                provider_uuid,
                                amount,
                                parameter.name,
                                unit,
                                is_input
                            )
                            print(
                                "Exchange created for product "
                                f"flow: {product}"
                            )
                            exchanges.append(exchange)
                            parameters_table.loc[count-1, ('parameter_name', 'parameter_description', 'parameter_value')] = (f"p{count}", product, amount)
                            break
                        except Exception as e:
                            print(
                                f"Error creating exchange for product flow: {e}"
                            )
                            break
                        # If the flow is an technosphere flow, the we create an
                        # exchange and move to the next row.

                    # If waste flow, then we need to search for a process.
                    elif row['Category'].lower() == 'waste flows':
                        print("\n")
                        print(f"Creating exchange for waste flow: {product}")
                        print("---------------------------------")
                        flow_uuid, provider_uuid = search_and_select(
                            exchanges_df=exchange_database,
                            keywords=product,
                            flow_type_str='waste',
                            client=client,
                            unit=unit
                        )
                        # Allows user to skip the flow
                        if flow_uuid == 'skip':
                            print(f"Skipping flow: {product}")
                            break
                        try:
                            exchange = create_exchange_pr_wa_flow(
                                client,
                                flow_uuid,
                                provider_uuid,
                                amount,
                                parameter.name,
                                unit,
                                is_input
                            )
                            print(
                                "Exchange created for waste "
                                f"flow: {product}"
                            )
                            exchanges.append(exchange)
                            parameters_table.loc[count-1, ('parameter_name', 'parameter_description', 'parameter_value')] = (f"p{count}", product, amount)
                            break
                        except Exception as e:
                            print(
                                f"Error creating exchange for waste flow: {e}"
                            )
                            break
                    else:
                        raise ValueError(
                            f"Invalid category: {row['Category']}. "
                            "Must be one of: elementary flows, product flows, "
                            "technosphere flows, waste flows."
                        )
            # Add handle errors if the row is missing a required column:
            # product, amount, unit, is_input, reference_product, and/or
            # category.
            except Exception as e:
                print(f"Error creating exchange for flow: {e}")
                retry_response = input(
                    "Do you want to try again? (y/n): "
                ).strip()
                if retry_response.lower().startswith('y'):
                    continue
                elif retry_response.lower().startswith('n'):
                    break

    # 5. Create process
    process.parameters = parameters
    process.exchanges = exchanges

    # 6. Save process to openLCA
    created_process = client.client.put(process)
    print(f"Successfully created process: {process_name}")
    print(f"Process saved successfully to openLCA database!")
    return created_process, parameters_table


def read_dataframe(df):
    """Helper function to read data frame and review its structure."""
    # Read dataframe - handle both file path and DataFrame object
    if isinstance(df, str):
        # If df is a string (file path), read the CSV file
        df = pd.read_csv(df)
    elif isinstance(df, pd.DataFrame):
        # If df is already a DataFrame, use it directly
        pass
    else:
        raise TypeError(
            "Data frame must be either a file path (string) or a pandas "
            "DataFrame"
        )

    # Validate structure
    # The dataframe should have the following columns:
    # Flow_Name, LCA_Amount, LCA_Unit, Is_Input, Reference_Product, Flow_Type
    required_columns = [
        'Flow_Name',
        'LCA_Amount',
        'LCA_Unit',
        'Is_Input',
        'Reference_Product',
        'Flow_Type'
    ]
    if not all(col in df.columns for col in required_columns):
        raise ValueError(
            "The dataframe must have the following "
            f"columns: {required_columns}"
        )
    return df


def create_empty_process(client, process_name, process_description):
    """Helper function to create an empty process."""
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


def generate_id(prefix: str = "entity") -> str:
    """
    Generate a unique ID for openLCA entities.

    Parameters
    ----------
    prefix : str
        Prefix for the ID (e.g., 'process', 'flow', 'unit').
        Note: prefix is ignored to comply with database VARCHAR(36) limit

    Returns
    -------
    str
        Unique ID (36-character UUID string).
    """
    return str(uuid.uuid4())

def create_parameter(
    name, 
    description, 
    formula, 
    is_input, 
    scope, 
    value):
    """ Helper function to create a parameter in openLCA.
    The function is generic and can be used to create local, 
    global, or impact parameters. 

    Parameters
    ----------
    name : str
        The name of the parameter.
    description : str
        The description of the parameter.
    formula : str
        The formula of the parameter.
        only applicable if the parameter is not an input parameter
    is_input : bool
        Whether the parameter is an input parameter.
    scope : str
        The scope of the parameter.
        Options: 'PROCESS_SCOPE', 'IMPACT_SCOPE', 'GLOBAL_SCOPE'
    value : float
        The value of the parameter.
    """ 
    parameter = olca.Parameter(
        name = name,
        description = description,
        formula = formula,
        is_input_parameter = is_input,
        parameter_scope = scope,
        value = value
    )
    return parameter

#
# TESTING
#

if __name__ == "__main__":
    
    sample_df = pd.read_csv("/home/franc/lca-prommis-francis/src/create_olca_process/lca_df_finalized.csv")
    
    netl = NetlOlca()
    netl.connect()
    netl.read()
   
    process_name = "p_test_final"
    process_description = "This is a test process"
    process = create_empty_process(netl, process_name, process_description)

    exchange_database = create_exchange_database(netl)

    exchanges = []
    parameters = []
    count = 0
    for _, row in sample_df.iterrows():
        count+=1
        # Gives you the option to try again if you make a mistake
        while True:
            try:
                product = row['Flow_Name']
                unit = row['LCA_Unit']
                amount = row['LCA_Amount']
                is_input = row['Is_Input']
                flow_uuid = row['UUID']
                parameter = create_parameter(f"p{count}", f"Reference parameter for {product}",'', True, ParameterScope("PROCESS_SCOPE"), amount)
                parameters.append(parameter)
                # TODO: add a check to see if there is more than one reference
                # product. Just want to have a warning printed.
                if row['Reference_Product']:    
                    print("\n")
                    print(f"Creating exchange for reference product: {product}")
                    print("----------------------------------------")
                    exchange = create_exchange_ref_flow(netl, product, amount, parameter.name, unit, is_input, row['Reference_Product'])
                    exchanges.append(exchange)
                    # If reference flow, then we don't need to search for a
                    # process.
                    break
                else:
                    # If not elementary flow, the we need to identify flow
                    # category, search for a flow and process/provider to
                    # create an exchange.
                    if row['Category'].lower() == 'elementary flows':
                        print("\n")
                        print(f"Creating exchange for elementary flow: {product}")
                        print("--------------------------------------")
                        try:
                            exchange = create_exchange_elementary_flow(
                                netl, flow_uuid, unit, amount, parameter.name, is_input
                            )
                            print(
                                "Exchange created for elementary flow: "
                                f"{product}"
                            )
                            exchanges.append(exchange)
                            break
                        except Exception as e:
                            print(
                                "Error creating exchange for elementary "
                                f"flow: {e}"
                            )
                            break
                    # If product flow, then we need to search for a process
                    elif (row['Category'].lower() == 'technosphere flows'
                            or row['Category'].lower() == 'product flows'):
                        print("\n")
                        print(f"Creating exchange for product flow: {product}")
                        print("-----------------------------------")
                        flow_uuid, provider_uuid = search_and_select(
                            exchanges_df=exchange_database,
                            keywords=product,
                            flow_type_str='product',
                            client=netl,
                            unit=unit
                        )
                        # Allows user to skip the flow
                        if flow_uuid == 'skip':
                            print(f"Skipping flow: {product}")
                            break
                        try:
                            exchange = create_exchange_pr_wa_flow(
                                netl,
                                flow_uuid,
                                provider_uuid,
                                amount,
                                parameter.name,
                                unit,
                                is_input
                            )
                            print(
                                "Exchange created for product "
                                f"flow: {product}"
                            )
                            exchanges.append(exchange)
                            break
                        except Exception as e:
                            print(
                                f"Error creating exchange for product flow: {e}"
                            )
                            break
                    # If waste flow, then we need to search for a process.
                    elif row['Category'].lower() == 'waste flows':
                        print("\n")
                        print(f"Creating exchange for waste flow: {product}")
                        print("---------------------------------")
                        flow_uuid, provider_uuid = search_and_select(
                            exchanges_df=exchange_database,
                            keywords=product,
                            flow_type_str='waste',
                            client=netl,
                            unit=unit
                        )
                        # Allows user to skip the flow
                        if flow_uuid == 'skip':
                            print(f"Skipping flow: {product}")
                            break
                        try:
                            exchange = create_exchange_pr_wa_flow(
                                netl,
                                flow_uuid,
                                provider_uuid,
                                amount,
                                parameter.name,
                                unit,
                                is_input
                            )
                            print(
                                "Exchange created for waste "
                                f"flow: {product}"
                            )
                            exchanges.append(exchange)
                            break
                        except Exception as e:
                            print(
                                f"Error creating exchange for waste flow: {e}"
                            )
                            break
                    else:
                        raise ValueError(
                            f"Invalid category: {row['Category']}. "
                            "Must be one of: elementary flows, product flows, "
                            "technosphere flows, waste flows."
                        )
            # Add handle errors if the row is missing a required column:
            # product, amount, unit, is_input, reference_product, and/or
            # category.
            except Exception as e:
                print(f"Error creating exchange for flow: {e}")
                retry_response = input(
                    "Do you want to try again? (y/n): "
                ).strip()
                if retry_response.lower().startswith('y'):
                    continue
                elif retry_response.lower().startswith('n'):
                    break
    # 5. Create process
    process.parameters = parameters
    process.exchanges = exchanges


    # 6. Save process to openLCA
    created_process = netl.client.put(process)
    print(f"Successfully created process: {process_name}")
    print(f"Process saved successfully to openLCA database!")
    