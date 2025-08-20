# This script includes a function that searches the database (connected through IPC)
# for processes that produce or are associated with a given flow

# Code assumptions
########################################################################################################
    # The user has openLCA running with an open database
    # The open database includes databases (e.g., databases imported by the user from LCACommons)
    # The user is connected to the openLCA database through IPC
    # The user has used the flow_search_function.py script to search for flows and has selected a flow 
    # The user knows the uuid of the selected flow
    # the user has already run the create_exchange_database.py script to create a database with all exchanges that are outputs and their respective process uuid

# Code logic
########################################################################################################
    # The function takes three main arguments/inputs:
        # 1. exchanges_df: dataframe containing exchanges
        # 2. flow_uuid: UUID of the flow to search for
        
    # The function filters rows from the database that have a flow uuid that matches the flow_uuid
    # The function returns the process uuid column

    # the function would then:
        # filter rows from the database that have a flow uuid that matches the flow_uuid
        # return the process uuid column

# Dependencies
########################################################################################################
import pandas as pd

# Main function
#####################################################################################

def find_processes_by_flow(exchanges_df, flow_uuid: str):

    # dataframe containing exchanges
    df = pd.DataFrame(exchanges_df)

    # filter rows from the database that have a flow uuid that matches the flow_uuid
    df.drop(df[df['exchange_uuid'] != flow_uuid].index, inplace=True)


    return df
