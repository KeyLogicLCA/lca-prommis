#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# create_exchange_database.py
#
###############################################################################
# DEPENDENCIES
###############################################################################
import olca_schema as olca
import pandas as pd


###############################################################################
# DOCUMENTATION
###############################################################################
__doc__ = """
This module has a function to create a data frame with all exchanges that are
outputs and their respective process universally unique identifiers (UUIDs).

**Code assumptions**

-   The user has openLCA running with an open database
-   The open database includes databases (e.g., databases imported by the user
    from LCACommons)
-   The user is connected to the openLCA database through IPC

**Logic**

The function takes one main argument/input:

1.  client object (IPC client)

"""
__all__ = [
    "create_exchange_database",
]


###############################################################################
# FUNCTIONS
###############################################################################
def create_exchange_database(client):
    """Create a data frame with all exchanges that are outputs and their
    respective process universally unique identifiers."""
    # get all processes
    process_descriptors = client.get_descriptors(olca.Process)

    exchange_database = []

    # get all exchanges
    for process in process_descriptors:
        process = client.query(olca.Process, process.id)
        for exchange in process.exchanges:
            # Only include output exchanges that have a flow attached
            if (not exchange.is_input
                    and getattr(exchange, 'flow', None) is not None):
                exchange_database.append({
                    'process_uuid': process.id,
                    'exchange_uuid': exchange.flow.id,
                    'process_name': process.name,
                })
    exchange_database = pd.DataFrame(exchange_database)

    return exchange_database
