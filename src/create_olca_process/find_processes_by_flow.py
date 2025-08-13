# This script includes a function that searches the database (connected through IPC)
# for processes that produce or are associated with a given flow

# Code assumptions
########################################################################################################
    # The user has openLCA running with an open database
    # The open database includes databases (e.g., databases imported by the user from LCACommons)
    # The user is connected to the openLCA database through IPC
    # The user has used the flow_search_function.py script to search for flows and has selected a flow 
    # The user knows the uuid of the selected flow

# Code logic
########################################################################################################
    # The function takes three main arguments/inputs:
        # 1. client object (IPC client)
        # 2. flow_uuid: UUID of the flow to search for
        
    # The function iterates through all processes in the database
    # The descriptors of the processes are retrieved using the .get_descriptors(olca.Process) 
    #   function (from netlolca)
    # The function then iterates through the descriptors and retrieved the full process objects 
    #   for processes with process exchange uuid matching the flow_uuid (the uuid extracted using 
    #   the flow_search_function and given in the function input by the user)
    # It categorizes exchanges as inputs, outputs, or both based on the is_input flag
    # The function returns two dataframes:
        # 1. producers_df: DataFrame with processes that produce (output) the flow
        # 2. consumers_df: DataFrame with processes that consume (input) the flow
    # Returns a comprehensive list of processes that use the specified flow

    ###UPDATE###
    # While the above logic is correct, and the function works, it is not efficient.
    # The function is slow and takes a long time to run for large databases.
    # To modiify it, the loop is changed. 
    # The input and output exchanges now only include the process uuid
    # One shortlisted, the function will amend the input and output exchanges dfs to include the process name, category, description, location, and exchange internal id
    # The function will then return the two dataframes

    ###UPDATE###
    # The logic above works but still takes a long time to run for large databases.
    # The function is now modified to only include the process uuid in the input and output exchanges.
    # Additionally, if the process uses the flow as an input, the process is skipped.


    # Comparison (searching using the uuid of and electricity flow --> ~2000 processes)
    # Approach 1: > 2 minutes
    # Approach 2: < 1.5'
    # Approach 3: < 1.5'

    # There was not much difference between approach 2 and 3.
    # but approach 3 provides a more clear result - it is unlikely that a consumer process would be used in an exchange

# Dependencies
########################################################################################################
import olca_ipc
import olca_schema as olca
import netlolca
from netlolca import NetlOlca
import pandas as pd
import logging
import re
from typing import List, Optional, Tuple, Union, Dict

# Helpers
########################################################################################################
def _clean_name(obj):
    # Handles cases where the attribute may be a Ref object with .name or a plain string.

    if obj is None:
        return None
    if isinstance(obj, str):
        return obj
    try:
        return obj.name
    except Exception:
        try:
            return str(obj)
        except Exception:
            return None

# Main function
#####################################################################################

def find_processes_by_flow(client, flow_uuid: str):
    """
    Search for processes that produce or use a given flow by examining process exchanges.
    
    Args:
        client: The openLCA IPC client object
        flow_uuid (str): UUID of the flow to search for
    
    Returns two dataframes:
            - producers_df: DataFrame with processes that produce (output) the flow
            - consumers_df: DataFrame with processes that consume (input) the flow
    """
    try:
        print(f"Searching for processes associated with flow UUID: {flow_uuid}...")
        
        # Get all process descriptors
        process_descriptors = client.get_descriptors(olca.Process)
        if not process_descriptors:
            print("No processes found in database")
            return pd.DataFrame(), pd.DataFrame()
        
        print(f"Scanning {len(process_descriptors)} processes for flow associations...")
        
        producer_data = []
        #consumer_data = []
        
        for i, descriptor in enumerate(process_descriptors):
            try:
                # Get full process object
                process = client.query(olca.Process, descriptor.id)
                if not process or not process.exchanges:
                    continue
                
                # Check exchanges for the target flow
                # input_exchanges = []
                output_exchanges = []
                
                for exchange in process.exchanges:
                    if exchange.flow and exchange.flow.id == flow_uuid:
                        exchange_info = {
                            'internal_id': exchange.internal_id,
                            # 'amount': exchange.amount,
                            # 'amount_formula': exchange.amount_formula,
                            # 'unit': _clean_name(exchange.unit),
                            # 'flow_property': _clean_name(exchange.flow_property),
                            # 'is_quantitative_reference': getattr(exchange, 'is_quantitative_reference', False),
                            # 'is_avoided_product': getattr(exchange, 'is_avoided_product', False),
                            'provider': _clean_name(getattr(exchange, 'default_provider', None))
                        }
                        
                        if not  exchange.is_input:
                            output_exchanges.append(exchange_info)
                        else:
                            #Skip process
                            # input_exchanges.append(exchange_info)
                            continue
                
                # Add to producer data if process produces the flow (output)
                if output_exchanges:
                    for ex in output_exchanges:
                        producer_data.append({
                            'Process_Name': process.name,
                            'Process_UUID': process.id,
                            # 'Category': _clean_name(process.category),
                            # 'Description': process.description,
                            # 'Location': _clean_name(process.location),
                            'Exchange_Internal_ID': ex['internal_id'],
                            # 'Amount': ex['amount'],
                            # 'Amount_Formula': ex['amount_formula'],
                            # 'Unit': ex['unit'],
                            # 'Flow_Property': ex['flow_property'],
                            # 'Is_Quantitative_Reference': ex['is_quantitative_reference'],
                            # 'Is_Avoided_Product': ex['is_avoided_product'],
                            'Default_Provider': ex['provider'],
                            'Flow_UUID': flow_uuid
                        })
                
                # Add to consumer data if process consumes the flow (input)
                # if input_exchanges:
                #     for ex in input_exchanges:
                #         consumer_data.append({
                #             'Process_Name': process.name,
                #             'Process_UUID': process.id,
                #             # 'Category': _clean_name(process.category),
                #             # 'Description': process.description,
                #             # 'Location': _clean_name(process.location),
                #             'Exchange_Internal_ID': ex['internal_id'],
                #             # 'Amount': ex['amount'],
                #             # 'Amount_Formula': ex['amount_formula'],
                #             # 'Unit': ex['unit'],
                #             # 'Flow_Property': ex['flow_property'],
                #             # 'Is_Quantitative_Reference': ex['is_quantitative_reference'],
                #             # 'Is_Avoided_Product': ex['is_avoided_product'],
                #             'Default_Provider': ex['provider'],
                #             'Flow_UUID': flow_uuid
                #         })
                    
            except Exception as e:
                logging.warning(f"Could not retrieve process {descriptor.id}: {e}")
                continue
        
        # Create DataFrames
        producers_df = pd.DataFrame(producer_data)
        # consumers_df = pd.DataFrame(consumer_data)
        
        # Print summary
        print(f"\nFound {len(producers_df)} producer entries")
        
        if not producers_df.empty:
            print(f"\nProducers ({len(producers_df.groupby('Process_UUID'))} unique processes):")
            print("-" * 80)
            for idx, (process_uuid, group) in enumerate(producers_df.groupby('Process_UUID'), 1):
                process_name = group.iloc[0]['Process_Name']
                exchange_count = len(group)
                print(f"{idx:3d}. {process_name}")
                print(f"     UUID: {process_uuid}")
                print(f"     Output exchanges: {exchange_count}")
        
        # if not consumers_df.empty:
        #     print(f"\nConsumers ({len(consumers_df.groupby('Process_UUID'))} unique processes):")
        #     print("-" * 80)
        #     for idx, (process_uuid, group) in enumerate(consumers_df.groupby('Process_UUID'), 1):
        #         process_name = group.iloc[0]['Process_Name']
        #         exchange_count = len(group)
        #         print(f"{idx:3d}. {process_name}")
        #         print(f"     UUID: {process_uuid}")
        #         print(f"     Input exchanges: {exchange_count}")
        
        return producers_df
        # return consumers_df
        
    except Exception as e:
        logging.error(f"Error searching for processes by flow: {e}")
        return pd.DataFrame(), pd.DataFrame()


