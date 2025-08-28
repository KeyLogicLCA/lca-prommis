# This script includes a function to create a product system in openLCA

# Code assumptions
########################################################################################################
    # The user has openLCA running with an open database
    # The open database includes databases (e.g., databases imported by the user from LCACommons)
    # The user is connected to the openLCA database through IPC
    # The user already used the create_olca_process package to create a process in openLCA

# Code logic
########################################################################################################
    # The function takes two main arguments/inputs:
        # 1. client object (IPC client)
        # 2. process_uuid

    # The function returns a product system object

# Dependencies
########################################################################################################
import olca_ipc
import olca_schema as olca
import netlolca
from netlolca import NetlOlca

def create_ps(client, process_uuid):

    # create product system with name of process
    process_ref = client.query(olca.Process, process_uuid).to_ref()
    linking_config = olca.LinkingConfig(
        cutoff=None,
        prefer_unit_processes=True,
        provider_linking=olca.ProviderLinking.PREFER_DEFAULTS
    )
    product_system_ref = client.client.create_product_system(process_ref, linking_config)

    return product_system_ref

    
    