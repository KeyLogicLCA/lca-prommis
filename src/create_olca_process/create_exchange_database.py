# This script includes a function that searches the database (connected through IPC)
# for processes that produce or are associated with a given flow

# Code assumptions
########################################################################################################
    # The user has openLCA running with an open database
    # The open database includes databases (e.g., databases imported by the user from LCACommons)
    # The user is connected to the openLCA database through IPC

# Code logic
########################################################################################################
    # The function takes one main argument/input:
        # 1. client object (IPC client)

    # The function creates a database with all exchanges that are outputs and their respective process uuid

# Dependencies
########################################################################################################
import olca_ipc
import olca_schema as olca
import netlolca
from netlolca import NetlOlca
import pandas as pd

# Function to create database with all exchanges that are outputs and their respective process uuid
########################################################################################################

def create_exchange_database(client):
    # get all processes
    process_descriptors = client.get_descriptors(olca.Process)

    exchange_database = []

    # get all exchanges
    for process in process_descriptors:
        process = client.query(olca.Process, process.id)
        for exchange in process.exchanges:
            # Only include output exchanges that have a flow attached
            if (not exchange.is_input) and getattr(exchange, 'flow', None) is not None:
                exchange_database.append({
                    'process_uuid': process.id,
                    'exchange_uuid': exchange.flow.id,
                    'process_name': process.name,
                })
    exchange_database = pd.DataFrame(exchange_database)

    return exchange_database
