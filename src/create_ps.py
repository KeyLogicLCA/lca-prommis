#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# create_ps.py
#
###############################################################################
# DEPENDENCIES
###############################################################################
import olca_schema as olca


###############################################################################
# DOCUMENTATION
###############################################################################
__doc__ = """
This script includes a function to create a product system in openLCA.

**Assumptions**

-   The user has openLCA running with an open database.
-   The open database includes databases (e.g., databases imported by the user
    from LCACommons).
-   The user is connected to the openLCA database through IPC.
-   The user already used the create_olca_process package to create a process
    in openLCA.

**Logic**

The function takes two main arguments/inputs:

1.  client object (IPC client)
2.  process_uuid

The function returns a product system object.
"""
__all__ = [
    "create_ps",
]


###############################################################################
# FUNCTIONS
###############################################################################
def create_ps(client, process_uuid):
    """Create a product system in openLCA.

    Parameters
    ----------
    client : NetlOlca
        An NetlOlca class instance connected to openLCA via IPC service.
    process_uuid : str
        The universally unique identifier to a process, which will become
        the reference process to the created product system.

    Returns
    -------
    olca-schema.Ref
        A reference object to the newly created product system.
    """
    # create product system with name of process
    process_ref = client.query(olca.Process, process_uuid).to_ref()
    linking_config = olca.LinkingConfig(
        cutoff=None,
        prefer_unit_processes=True,
        provider_linking=olca.ProviderLinking.PREFER_DEFAULTS
    )
    product_system_ref = client.client.create_product_system(
        process_ref,
        linking_config
    )

    return product_system_ref
