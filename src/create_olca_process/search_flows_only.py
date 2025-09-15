#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
search_flows.py

Interactive search to:
  1) find a Flow by keyword and type (product, waste, elementary)
  2) optionally find a provider Process for that Flow (if not elementary)
Returns (flow_uuid, process_uuid) as a tuple from the main function.

Relies on:
- NetlOlca.NetlOlca to establish the olca-ipc client
- flow_search_function.search_Flows_by_keywords(client, keywords, flow_type)
- find_processes_by_flow.find_processes_by_flow(client, flow_uuid)

You can import and call `search_and_select(...)` programmatically,
or run this as a script for a CLI experience.
"""
from typing import Optional, Tuple, Union, List
import sys
import logging

try:
    import olca_schema as olca  # type: ignore
except Exception as e:
    # Don't hard crash on import when reading the file; surface a clearer error later when used.
    olca = None  # type: ignore

from netlolca import NetlOlca
from src.create_olca_process.flow_search_function import search_Flows_by_keywords
from src.create_olca_process.find_processes_by_flow import find_processes_by_flow
from src.create_olca_process.create_exchange_database import create_exchange_database

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def _ensure_client(existing_client=None):
    """Return a connected olca client. Use the provided one if valid, else connect via NetlOlca."""
    if existing_client is not None:
        return existing_client
    netl = NetlOlca()
    netl.connect()  # uses default port from NetlOlca
    if not getattr(netl, "client", None):
        raise RuntimeError("Failed to connect to openLCA via IPC. Is openLCA running with a database open?")
    return netl.client

def _prompt_select(rows: List[dict], display_keys: List[str], uuid_key: str, prompt: str) -> Optional[str]:
    """Simple CLI selector over a list of dictionaries.
    
    Args:
        rows: list of dicts to display
        display_keys: keys to show in each row
        uuid_key: key that contains the UUID to return
        prompt: input prompt string
    
    Returns:
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


# -----------------------------------------------------------------------------
# Core functionality
# -----------------------------------------------------------------------------

def search_and_select_flows(keywords,client):
    """Search for a product flow 

    Args:
        exchanges_df: dataframe containing exchanges
        keywords: keyword(s) to search flow names
        client: optional pre-connected olca-ipc client

    Returns:
        (flow_uuid)
    """
    client = _ensure_client(client)
    
    # If no keywords provided, prompt for them
    if keywords is None:
        keywords = input("Enter flow name keyword(s). Type 'skip' to skip this flow. ").strip()
    elif keywords.lower() == 'skip':
        return ('skip', None)
    # If keywords provided, prompt, but allow user to press enter to use the default keywords
    else:
        keywords_response = input(f"Enter flow name keyword(s). Type 'skip' to skip this flow. Press enter to use {keywords}: ").strip()
        if len(keywords_response) == 0:
            keywords = keywords
        elif keywords_response.lower() == 'skip':
            return ('skip', None)
        else:
            keywords = keywords_response

    # If no keywords or flow type provided, raise an error
    if keywords is None or len(keywords) == 0:
        raise ValueError("No keywords provided.")

    # 1) Search for flows by keyword -- only report product flows
    results = search_Flows_by_keywords(client, keywords, olca.FlowType.PRODUCT_FLOW)
    if not results:
        print("No flows found matching the criteria.")
        return (None, None)

    # Expect: matching_flows (list of olca.Flow), clean_df with ['Number','Flow_Name','UUID'], full_df
    matching_flows, clean_df, full_df = results

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

    return (selected_flow_uuid)


# -----------------------------------------------------------------------------
# CLI entry point
# -----------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None):
    """Run an interactive session and print the result tuple."""
    netl = NetlOlca()
    netl.connect()
    netl.read()
    argv = argv or sys.argv[1:]
    exchange_database = create_exchange_database(netl)
    try:
        flow_uuid = search_and_select_flows(exchanges_df=exchange_database, keywords = None, client = netl)
        print("\nResult:")
        print(f"Flow UUID    : {flow_uuid}")
    except Exception as e:
        logging.exception("Error running search_flows_only: %s", e)
        print(f"Error: {e}")


if __name__ == "__main__":
    main()

