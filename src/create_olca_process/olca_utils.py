#!/usr/bin/env python3
"""
OpenLCA Utilities Module

This module provides basic utilities for working with openLCA, including:
- ID generation
- Connection testing
- Basic entity operations

Requirements:
- olca-ipc package: pip install olca-ipc
- olca-schema package: pip install olca-schema
"""

import sys
import logging
import uuid
from typing import List, Optional, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import the required modules
try:
    from olca_ipc import Client
    import olca_schema as olca
    print("✅ olca-ipc and olca-schema imported successfully")
except ImportError as e:
    logger.error(f"Failed to import required packages: {e}")
    logger.info("Please install: pip install olca-ipc olca-schema")
    sys.exit(1)


def generate_id(prefix: str = "entity") -> str:
    """
    Generate a unique ID for openLCA entities.
    
    Args:
        prefix (str): Prefix for the ID (e.g., 'process', 'flow', 'unit') - Note: prefix is ignored to comply with database VARCHAR(36) limit
    
    Returns:
        str: Unique ID (36-character UUID string)
    """
    return str(uuid.uuid4())


def test_connection(client: Client) -> bool:
    """
    Test the connection to openLCA and return connection status.
    
    Args:
        client (Client): The openLCA IPC client
    
    Returns:
        bool: True if connection is successful, False otherwise
    """
    try:
        descriptors = client.get_descriptors(olca.Process)
        logger.info(f"✅ Connection test successful. Found {len(descriptors)} processes.")
        return True
    except Exception as e:
        logger.error(f"❌ Connection test failed: {e}")
        return False


def get_database_info(client: Client) -> dict:
    """
    Get basic information about the connected database.
    
    Args:
        client (Client): The openLCA IPC client
    
    Returns:
        dict: Dictionary containing database statistics
    """
    info = {
        'processes': 0,
        'flows': 0,
        'units': 0,
        'flow_properties': 0,
        'unit_groups': 0
    }
    
    try:
        info['processes'] = len(client.get_descriptors(olca.Process))
        info['flows'] = len(client.get_descriptors(olca.Flow))
        
        # Handle @type issues with units
        try:
            info['units'] = len(client.get_descriptors(olca.Unit))
        except Exception:
            info['units'] = "Unknown (@type error)"
        
        try:
            info['flow_properties'] = len(client.get_descriptors(olca.FlowProperty))
        except Exception:
            info['flow_properties'] = "Unknown (@type error)"
        
        try:
            info['unit_groups'] = len(client.get_descriptors(olca.UnitGroup))
        except Exception:
            info['unit_groups'] = "Unknown (@type error)"
        
        logger.info(f"Database info: {info}")
        return info
        
    except Exception as e:
        logger.warning(f"Could not retrieve complete database info: {e}")
        return info


def find_entity_by_name(entities: List, name: str, entity_type: str = "entity") -> Optional[Any]:
    """
    Find an entity by name from a list of entities.
    
    Args:
        entities (List): List of entities to search
        name (str): Name to search for
        entity_type (str): Type of entity for logging
    
    Returns:
        Optional: Found entity or None
    """
    name_lower = name.lower()
    for entity in entities:
        if hasattr(entity, 'name') and entity.name.lower() == name_lower:
            return entity
    logger.warning(f"No {entity_type} found with name: {name}")
    return None


def initialize_client() -> Optional[Client]:
    """
    Initialize and test the openLCA client connection.
    
    Returns:
        Optional[Client]: Connected client or None if connection failed
    """
    try:
        client = Client()
        print("✅ Connected to openLCA")
        
        if test_connection(client):
            return client
        else:
            return None
            
    except Exception as e:
        logger.error(f"❌ Failed to connect to openLCA: {e}")
        print("Make sure openLCA is running with IPC developer tool activated")
        return None


def print_database_summary(client: Client):
    """
    Print a summary of the database contents.
    
    Args:
        client (Client): The openLCA IPC client
    """
    print("\n" + "=" * 50)
    print("DATABASE SUMMARY")
    print("=" * 50)
    
    info = get_database_info(client)
    
    print(f"Processes: {info['processes']}")
    print(f"Flows: {info['flows']}")
    print(f"Units: {info['units']}")
    print(f"Flow Properties: {info['flow_properties']}")
    print(f"Unit Groups: {info['unit_groups']}")
    print("=" * 50)