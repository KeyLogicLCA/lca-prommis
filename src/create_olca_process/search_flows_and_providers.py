#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# search_flows_and_providers.py
#
###############################################################################
# DEPENDENCIES
###############################################################################
from typing import Optional, Tuple, List
import sys
import logging

try:
    import olca_schema as olca
except Exception as e:
    # Don't hard crash on import when reading the file; surface a clearer error
    # later when used.
    olca = None
import olca_schema.units as o_units

from netlolca import NetlOlca
from src.create_olca_process.flow_search_function import search_Flows_by_keywords
from src.create_olca_process.find_processes_by_flow import find_processes_by_flow
from src.create_olca_process.create_exchange_database import create_exchange_database


###############################################################################
# DOCUMENTATION
###############################################################################
__doc__ = """
An interactive search to:

1.  find a Flow by keyword and type (product, waste, elementary)
2.  optionally find a provider Process for that Flow (if not elementary)

Returns (flow_uuid, process_uuid) as a tuple from :func:`main`.

Relies on:

-   NetlOlca.NetlOlca to establish the olca-ipc client
-   flow_search_function.search_Flows_by_keywords(client, keywords, flow_type)
-   find_processes_by_flow.find_processes_by_flow(client, flow_uuid)

You can import and call `search_and_select(...)` programmatically,
or run this as a script for a CLI experience.
"""
__all__ = [
    "main",
    "search_and_select",
]


###############################################################################
# FUNCTIONS
###############################################################################
def _ensure_client(existing_client=None):
    """Return a connected olca client. Use the provided one if valid, else
    connect via NetlOlca."""
    if existing_client is not None:
        return existing_client
    netl = NetlOlca()
    netl.connect()  # uses default port from NetlOlca
    if not getattr(netl, "client", None):
        raise RuntimeError(
            "Failed to connect to openLCA via IPC. Is openLCA running with "
            "a database open?"
        )
    return netl.client


def _flowtype_from_string(s: str):
    """Map a user string to olca.FlowType.*_FLOW"""
    if olca is None:
        raise ImportError(
            "The 'olca' package is required but could not be imported."
        )
    if not s:
        raise ValueError("Flow type string is empty.")
    key = s.strip().lower()
    mapping = {
        "product": olca.FlowType.PRODUCT_FLOW,
        "product flow": olca.FlowType.PRODUCT_FLOW,
        "waste": olca.FlowType.WASTE_FLOW,
        "waste flow": olca.FlowType.WASTE_FLOW,
    }
    if key not in mapping:
        raise ValueError(f"Unknown flow type '{s}'. Expected one of: product, waste, elementary.")
    return mapping[key]


def _prompt_select(rows: List[dict],
                   display_keys: List[str],
                   uuid_key: str,
                   prompt: str) -> Optional[str]:
    """Simple CLI selector over a list of dictionaries.

    Parameters
    ----------
    rows : list
        A list of dicts to display.
    display_keys : list
        A list of keys to show in each row.
    uuid_key : str
        A key that contains the UUID to return.
    prompt : str
        Input prompt string.

    Returns
    -------
    str, NoneType
        The selected UUID string, or None if user aborts.
    """
    if not rows:
        print("No options to select from.")
        return None

    for i, row in enumerate(rows, 1):
        parts = []
        for k in display_keys:
            val = row.get(k, "")
            parts.append(f"{k}: {val}")
        print(f"{i:3d}. " + " | ".join(parts))

    while True:
        choice = input(f"{prompt} (1-{len(rows)} or 'q' to quit): ").strip()
        if choice.lower() in ("q", "quit", "exit"):
            return None
        if not choice.isdigit():
            print("Please enter a valid number.")
            continue
        idx = int(choice)
        if not (1 <= idx <= len(rows)):
            print(f"Please enter a number between 1 and {len(rows)}.")
            continue
        return rows[idx - 1].get(uuid_key)


def search_and_select(exchanges_df,
                      keywords: Optional[str] = None,
                      flow_type_str: Optional[str] = None,
                      client=None,
                      unit: Optional[str] = None,
                      ) -> Tuple[Optional[str], Optional[str]]:
    """Search for a flow and (if applicable) a provider process.

    Parameters
    ----------
    exchanges_df : pandas.DataFrame
        A dataframe containing exchanges.
    keywords : str, optional
        Keyword(s) to search flow names.
        Defaults to none.
    flow_type_str : str, optional
        A flow type (e.g., 'product', 'waste', or 'elementary').
        Defaults to none.
    client : NetlOlca, optional
        A pre-connected olca-ipc client.
    unit : str, optional
        Unit name. Defaults to none.

    Returns
    -------
    tuple
        A tuple of length two:

        - str, flow UUID
        - str, process UUID

        Note, the process UUID will be None for elementary flows or if user
        aborts.
    """
    client = _ensure_client(client)

    # If no keywords provided, prompt for them
    if keywords is None:
        keywords = input(
            "Enter flow name keyword(s). Type 'skip' to skip this flow. "
        ).strip()
    elif keywords.lower() == 'skip':
        return ('skip', None)
    # If keywords provided, prompt, but allow user to press enter to use the
    # default keywords.
    else:
        keywords_response = input(
            "Enter flow name keyword(s). "
            "Type 'skip' to skip this flow. "
            f"Press enter to use {keywords}: "
        ).strip()
        if len(keywords_response) == 0:
            keywords = keywords
        elif keywords_response.lower() == 'skip':
            return ('skip', None)
        else:
            keywords = keywords_response

    # If no keywords or flow type provided, raise an error
    if keywords is None or len(keywords) == 0:
        raise ValueError("No keywords provided.")
    if flow_type_str is None or len(flow_type_str) == 0:
        raise ValueError("No flow type provided.")

    flow_type = _flowtype_from_string(flow_type_str)

    # 1) Search for flows by keyword and type
    results = search_Flows_by_keywords(client, keywords, flow_type)
    if not results:
        print("No flows found matching the criteria.")
        return (None, None)

    # Expect matching_flows (list of olca.Flow), clean_df with ['Number',
    # 'Flow_Name','UUID'], and full_df; only need clean_df:
    _, clean_df, _ = results

    if clean_df is None or len(clean_df) == 0:
        print("No flows found matching the criteria.")
        return (None, None)

    # Build rows to display
    rows = []
    for _, row in clean_df.iterrows():
        rows.append({
            "Number": int(row.get("Number", len(rows)+1)),
            "Flow_Name": str(row.get("Flow_Name", "")),
            "UUID": str(row.get("UUID", "")),
        })

    selected_flow_uuid = None
    selected_flow_uuid = _prompt_select(
        rows, display_keys=["Flow_Name", "UUID"], uuid_key="UUID",
        prompt="Select a flow"
    )
    if selected_flow_uuid is None:
        return (None, None)

    ids=[]
    flow = client.query(olca.Flow, selected_flow_uuid)
    flow_property = o_units.property_ref(unit)
    if flow_property is None:
        flow_property = o_units.property_ref(unit.lower())
    if flow_property is None:
        raise ValueError(
            "The flow property is not found in the flow. "
            "Adjust your unit or select another flow"
        )

    # 2) Find processes associated with the selected flow (producers/providers)
    proc_result = find_processes_by_flow(exchanges_df, selected_flow_uuid)
    producers_df = None

    # Handle either a single DF return or a tuple of (producers_df,
    # consumers_df).
    if isinstance(proc_result, tuple) and len(proc_result) > 0:
        producers_df = proc_result[0]
    else:
        producers_df = proc_result

    if producers_df is None or len(producers_df) == 0:
        print("No provider processes found for the selected flow.")
        return (selected_flow_uuid, None)

    proc_rows = []
    # Expected columns include 'process_name' and 'process_uuid'
    for _, row in producers_df.iterrows():
        proc_rows.append({
            "Process_Name": str(row.get("process_name", "")),
            "Process_UUID": str(row.get("process_uuid", "")),
        })

    selected_process_uuid = None
    selected_process_uuid = _prompt_select(
        proc_rows,
        display_keys=["Process_Name", "Process_UUID"],
        uuid_key="Process_UUID",
        prompt="Select a provider process"
    )
    print(f"Selected process UUID: {selected_process_uuid}")
    if selected_process_uuid is None:
        return (selected_flow_uuid, None)

    return (selected_flow_uuid, selected_process_uuid)


def main(argv: Optional[List[str]] = None):
    """Run an interactive session and print the result tuple."""
    netl = NetlOlca()
    netl.connect()
    netl.read()
    argv = argv or sys.argv[1:]
    exchange_database = create_exchange_database(netl)
    try:
        flow_uuid, process_uuid = search_and_select(
            exchanges_df=exchange_database,
            flow_type_str = 'product',
            client = netl
        )
        print("\nResult:")
        print(f"Flow UUID    : {flow_uuid}")
        print(f"Process UUID : {process_uuid}")
    except Exception as e:
        logging.exception("Error running search_flows: %s", e)
        print(f"Error: {e}")


###############################################################################
# MAIN
###############################################################################
if __name__ == "__main__":
    main()
