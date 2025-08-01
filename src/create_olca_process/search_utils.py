#!/usr/bin/env python3
"""
Search Utilities Module

This module provides search functionality for flows, processes, and other entities in openLCA.
"""

import logging
from typing import List

# Import the required modules
try:
    from olca_ipc import Client
    import olca_schema as olca
except ImportError as e:
    logging.error(f"Failed to import required packages: {e}")
    raise

logger = logging.getLogger(__name__)


def search_processes_by_keywords(client: Client, keywords: str) -> List[olca.Process]:
    """
    Search for processes by keywords using efficient descriptor-based search.
    Focuses primarily on process names with smart matching and sorting.
    
    Args:
        client (Client): The openLCA IPC client
        keywords (str): Keywords to search for
    
    Returns:
        List[olca.Process]: List of processes matching the keywords, sorted by relevance
    """
    try:
        # Get all process descriptors first (much faster)
        process_descriptors = client.get_descriptors(olca.Process)
        
        if not process_descriptors:
            logger.info("No processes found in database")
            return []
        
        print(f"   🔍 Searching through {len(process_descriptors)} processes...")
        
        # Find matches using descriptor names only (much faster)
        matches = []
        keywords_lower = keywords.lower()
        
        for descriptor in process_descriptors:
            try:
                # Ensure both keywords and process name are lowercase for case-insensitive matching
                name = descriptor.name.lower() if descriptor.name else ""
                
                # Check if keywords appear in the process name
                if keywords_lower in name:
                    matches.append(descriptor)
                    
            except Exception as e:
                logger.warning(f"Could not check descriptor {descriptor.id}: {e}")
                continue
        
        if not matches:
            print(f"   ❌ No processes found matching '{keywords}' in process names")
            return []
        
        # Sort matches by relevance (exact matches first, then alphabetical)
        def sort_key(descriptor):
            # Ensure process name is lowercase for consistent sorting
            name = descriptor.name.lower() if descriptor.name else ""
            # Exact match gets highest priority
            if name == keywords_lower:
                return (0, name)
            # Starts with keywords gets second priority
            elif name.startswith(keywords_lower):
                return (1, name)
            # Contains keywords gets third priority
            else:
                return (2, name)
        
        matches.sort(key=sort_key)
        
        # Get full process objects for the matches
        matching_processes = []
        for descriptor in matches:
            try:
                process = client.get(olca.Process, descriptor.id)
                if process:
                    matching_processes.append(process)
            except Exception as e:
                logger.warning(f"Could not retrieve process {descriptor.id}: {e}")
                continue
                
        print(f"   ✅ Found {len(matching_processes)} processes matching '{keywords}'")
        return matching_processes
        
    except Exception as e:
        logger.warning(f"Could not search for processes by keywords: {e}")
        return []



def get_existing_processes(client: Client) -> List[olca.Process]:
    """
    Get all existing processes from the database using efficient descriptor-based retrieval.
    
    Args:
        client (Client): The openLCA IPC client
    
    Returns:
        List[olca.Process]: List of existing processes
    """
    try:
        process_descriptors = client.get_descriptors(olca.Process)
        processes = []
        
        if not process_descriptors:
            logger.info("No processes found in database")
            return []
        
        print(f"   📊 Retrieving {len(process_descriptors)} processes...")
        
        for descriptor in process_descriptors:
            try:
                process = client.get(olca.Process, descriptor.id)
                if process:
                    processes.append(process)
            except Exception as e:
                logger.warning(f"Could not retrieve process {descriptor.id}: {e}")
                continue
                
        return processes
    except Exception as e:
        logger.warning(f"Could not retrieve existing processes: {e}")
        return []