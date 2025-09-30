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
def generate_total_results(result):
    # Extract results - total impacts
    total_impacts = result.get_total_impacts()
    total_impacts_df = pd.DataFrame(total_impacts)
    total_impacts_df.to_excel("output/total_impacts.xlsx", index=False)

    return total_impacts_df
