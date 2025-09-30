#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# find_processes_by_flow.py
#
###############################################################################
# DEPENDENCIES
###############################################################################
import pandas as pd


###############################################################################
# DOCUMENTATION
###############################################################################
__doc__ = """
This script includes a function that searches the database (connected through
IPC) for processes that produce or are associated with a given flow.

**Assumptions**

-   The user has openLCA running with an open database.
-   The open database includes databases (e.g., databases imported by the user
    from LCACommons).
-   The user is connected to the openLCA database through IPC.
-   The user has used the flow_search_function.py script to search for flows
    and has selected a flow.
-   The user knows the uuid of the selected flow.
-   The user has already run the create_exchange_database.py script to create a
    database with all exchanges that are outputs and their respective process
    uuid.

**Logic**

The function takes two main arguments/inputs:

1.  exchanges_df: dataframe containing exchanges
2.  flow_uuid: UUID of the flow to search for

The function filters rows from the database that have a matching flow UUID and
returns the process UUID column.

The function then:

1.  Filters rows from the database that have a flow uuid that matches the
    flow_uuid.
2.  Return the process uuid column.
"""
__all__ = [
    "find_processes_by_flow",
]


###############################################################################
# FUNCTIONS
###############################################################################
def find_processes_by_flow(exchanges_df, flow_uuid: str):
    """Filter a data frame to find processes that contain an exchange flow
    that matches a given flow UUID.

    Parameters
    ----------
    exchanges_df : pandas.DataFrame
        A data frame of exchange flows.
    flow_uuid : str
        The universally unique identifier for a flow.

    Returns
    -------
    pandas.DataFrame
        A reduced data frame where rows contain the flow UUID in the exchanges.
    """
    # Dataframe containing exchanges
    df = pd.DataFrame(exchanges_df)

    # Filter rows from the database that have a flow uuid that matches the
    # flow_uuid
    df.drop(df[df['exchange_uuid'] != flow_uuid].index, inplace=True)

    return df
