#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# flow_search_function.py
#
###############################################################################
# DEPENDENCIES
###############################################################################
import logging
import re
from typing import Optional

import olca_schema as olca
import pandas as pd


###############################################################################
# DOCUMENTATION
###############################################################################
__doc__ = """
This script includes a function that searches the database (connected through
IPC) for flows that match a given keyword.

**Assumptions**

-   The user has openLCA running with an open database.
-   The open database includes databases (e.g., databases imported by the user
    from LCACommons).
-   The user is connected to the openLCA database through IPC.

**Logic**

The main function takes three main arguments/inputs

1.  the keyword(s) to search for
2.  the flow type to search for
3.  the client object

The keyword is modified using the re.escape function to handle special regex
characters (if input by the user).
The function uses the netlolca function .get_descriptors(olca.Flow) to get all
flow descriptors.
The function uses the netlolca function .query(olca.Flow, descriptor.id) to get
the full flow object.
The ``matching_flows`` variable is the first list returned by the function -->
it is a list of flow objects.
The ``clean_df`` variable is the second dataframe returned by the function -->
it is a dataframe with just the flow names and UUIDs.
The ``full_df`` variable is the third dataframe returned by the function -->
it is a dataframe with all flow attributes.

The function returns three outputs:

1.  matching_flows: list of matching flows
2.  clean_df: dataframe with just the flow names and UUIDs
3.  full_df: dataframe with all flow attributes
"""
__all__ = [
    "search_Flows_by_keywords",
]


###############################################################################
# FUNCTIONS
###############################################################################
def search_Flows_by_keywords(client,
                             keywords: str,
                             flow_type: Optional[olca.FlowType] = None):
    """
    Search for processes by keywords using netlolca functions.

    The function focuses primarily on process names with smart matching and
    sorting. Optionally, it filters by flow type in process exchanges.

    Parameters
    ----------
    netl_client : NetlOlca
        The netlolca client instance
    keywords : str
        Keywords to search for
    flow_type : olca.FlowType, optional
        Flow type to filter by (e.g., olca.FlowType.PRODUCT_FLOW,
        olca.FlowType.ELEMENTARY_FLOW, olca.FlowType.WASTE_FLOW).

    Returns
    -------
    tuple or list
        A tuple of length three:

        - list, a list of matching flows
        - pandas.DataFrame, a data frame with just the flow names and UUIDs
        - pandas.DataFrame, a data frame with all flow attributes

        An empty list is returned for a failed search.
    """
    try:
        print (f"Searching for flows containing '{keywords}'...")

        # Modify keywords for better matching.
        # Using re.escape to handle special regex characters in keywords.
        escaped_keywords = re.escape(keywords)
        pattern = re.compile(f".*{escaped_keywords}.*", re.IGNORECASE)

        # Get all flow descriptors
        flow_descriptors = client.get_descriptors(olca.Flow)
        if not flow_descriptors:
            print("No flows found in database")
            return []

        matching_descriptors = []
        for descriptor in flow_descriptors:
            if pattern.search(descriptor.name.lower()):
                matching_descriptors.append(descriptor)

        if not matching_descriptors:
            print(f"No flows found matching '{keywords}'")
            return []

        print(f"Found {len(matching_descriptors)} flows matching '{keywords}'")

        # Get full flow objects and filter by type if specified
        matching_flows = []
        for descriptor in matching_descriptors:
            try:
                flow = client.query(olca.Flow, descriptor.id)
                if flow:
                    # Filter by flow type if specified
                    if flow_type is None or flow.flow_type == flow_type:
                        matching_flows.append(flow)
            except Exception as e:
                logging.warning(f"Could not retrieve flow {descriptor.id}: {e}")
                continue

        if flow_type:
            print(f"Filtered to {len(matching_flows)} {flow_type.name} flows")

        # Create clean dataframe with just names and UUIDs
        clean_data = []
        for i, flow in enumerate(matching_flows, 1):
            clean_data.append({
                'Number': i,
                'Flow_Name': flow.name,
                'UUID': flow.id
            })
        clean_df = pd.DataFrame(clean_data)

        # Create full dataframe with all flow attributes
        full_data = []
        for i, flow in enumerate(matching_flows, 1):
            full_data.append({
                'Number': i,
                'Flow_Name': flow.name,
                'UUID': flow.id,
                'Category': flow.category,
                'Description': flow.description,
                'Flow_Type': str(flow.flow_type) if flow.flow_type else None,
                'CAS': flow.cas,
                'Formula': flow.formula,
                'Is_Infrastructure_Flow': flow.is_infrastructure_flow,
                'Last_Change': flow.last_change,
                'Library': flow.library,
                'Location': flow.location.name if flow.location else None,
                'Synonyms': flow.synonyms,
                'Tags': flow.tags,
                'Version': flow.version,
                'Flow_Properties_Count': len(flow.flow_properties) if flow.flow_properties else 0
            })
        full_df = pd.DataFrame(full_data)

        return matching_flows, clean_df, full_df

    except Exception as e:
        logging.warning(f"Could not search for flows: {e}")
