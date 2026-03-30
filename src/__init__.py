#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import src.finalize_LCA_flows as final_lca
import src.create_olca_process as create_lca
import src.prommis_LCA_conversions as convert_lca
import src.prommis_LCA_data as data_lca
import src.create_ps as create_ps
import src.run_analysis as run_analysis
import src.generate_total_results as generate_total_results
import src.generate_contribution_tree as generate_contribution_tree
import src.import_db as import_db
import src.plot_results as plot_results

# Helper Function
#---------------------------------------------------------------------------------------
output_dir = os.path.join(os.path.expanduser("~"), ".netl")

def setup_output_directory(working_dir):
    """
    Helper method to check if the working directory exists and create it if it doesn't.
    This method:
        * includes a fallback to simple mkdir if the makedirs function fails.
        * sets the output directory to home directory if it fails to create 
        the working directory.
    
    Note: This method is retrieved from eLCI 
    
    """
    if not os.path.isdir(working_dir):
        try:
            os.makedirs(working_dir)
        except:
            logging.warning("Failed to create folder %s!" % working_dir)
            try:
                # Revert to simple mkdir
                os.mkdir(working_dir)
            except:
                logging.error("Could not create folder, %s" % working_dir)
            else:
                logging.info("Created %s" % working_dir)
        else:
            logging.info("Created %s" % working_dir)

    if os.path.isdir(working_dir):
        output_dir = working_dir
    else:
        output_dir = os.path.expanduser("~")

    return output_dir