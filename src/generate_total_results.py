#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# generate_total_results.py
#
###############################################################################
# DEPENDENCIES
###############################################################################
import pandas as pd


###############################################################################
# DOCUMENTATION
###############################################################################
__doc__ = """
This script includes a function to extract the total LCIA results from a openLCA result object.

**Assumptions**

-   The user has openLCA running with an open database
-   The open database includes databases (e.g., databases imported by the user
    from LCACommons)
-   The user is connected to the openLCA database through IPC
-   The user already used previous functions to create a product system, run
    the analysis and return a result object

**Logic**

The function takes one argument (the result object returned from
Run_analysis.py) and returns a data frame with the total environmental impacts.

"""
__all__ = [
    "generate_total_results",
]


###############################################################################
# FUNCTIONS
###############################################################################
def extract_impacts(ps):
    return (ps['name'], ps['ref_unit'], ps['id'])

def generate_total_results(result):
    # Extract results - total impacts
    total_impacts = result.get_total_impacts()
    total_impacts_df = pd.DataFrame(total_impacts)
    # Parse the name, units, and UUID from impact categories
    total_impacts_df['temp'] = total_impacts_df['impact_category'].apply(extract_impacts)
    total_impacts_df['name'] = total_impacts_df['temp'].str.get(0)
    total_impacts_df['units'] = total_impacts_df['temp'].str.get(1)
    total_impacts_df['uuid'] = total_impacts_df['temp'].str.get(2)
    total_impacts_df = total_impacts_df.drop(columns=['temp', 'impact_category'])    
    # Save results
    total_impacts_df.to_csv("output/total_impacts.csv", index=False)

    return total_impacts_df
