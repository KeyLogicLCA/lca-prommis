# This script includes a function to extract the results by category from a given openLCA result object

# Code assumptions
########################################################################################################
    # The user has openLCA running with an open database
    # The open database includes databases (e.g., databases imported by the user from LCACommons)
    # The user is connected to the openLCA database through IPC
    # The user already used previous functions to create a product system, run the analysis and return a result object

# Code logic
########################################################################################################
    # The function takes three arguments: 
    #   1. the result object returned from run_analysis.py
    #   2. the maximum number of levels to expand the tree 
    #       - this represents the number of steps away from the main product
    #   3. the maximum number of nodes to expand the tree
    #       - this represents the number of child nodes to expand for each level
    #       - for example: electricity, natural gas, etc.

    # Read impact category
    # for each impact cateogory, extract the results (combination tree) 
    # export the results to an excel file holding the name of the impact category

    # Note: this function heavily relies on the olca_ipc utree function
    # https://greendelta.github.io/openLCA-ApiDoc/results/impacts/upstream_trees.html

# Dependencies
########################################################################################################
import pandas as pd
import olca_schema as olca
import olca_ipc
from olca_ipc import utree
import re

def generate_contribution_tree(result, max_expand_levels, max_expand_nodes, download_results):
    """
    This function generates the results contribution tree for each impact category. 
    The function returns a dataframe for each impact category within the impact assessment method used.

    Parameters
    ----------
    result : result object
    max_expand_levels : int
    max_expand_nodes : int
    download_results : bool
    
    Returns
    -------
    dataframe for each impact category within the impact assessment method used.
    """
    # check if result is an olca object
    if not isinstance(result, olca_ipc.Result):
        raise ValueError("Result is not an olca object")
    # check if result is empty
    if result is None:
        raise ValueError("Result is empty")
    # get all impact categories in the result object
    impact_categories = result.get_impact_categories()
    impact_categories_df = pd.DataFrame(impact_categories)

    # for each impact category generate the results contribution tree
    n=0 # this is defined here and used below to refer to the row of the impact category being analyzed
    # n is updated (+1) after every loop

    df_to_return = pd.DataFrame()

    for impact_category in impact_categories:
        impact_category_name = impact_categories_df.iloc[n, 6]
        root = utree.of(result,impact_category)
        # create empty dataframe
        df = pd.DataFrame()
        # store the results in the dataframe
        df = expand(root,0, max_expand_levels, max_expand_nodes)
        # convert the results to a dataframe - for some reason i 
        # had to add this line to covert the results AGAIN to a dataframe
        df = pd.DataFrame(df)
        df = df.rename(columns={0: "Level", 1: "Provider", 2: "Result", 3: "Direct_Contribution"})
        df["Impact_Category"] = impact_category_name
        # generate clean file name
        filename = re.sub(r'[<>:"/\\|?*]', "_", str(impact_category_name))
        # export the results to a csv file and store in output folder
        if download_results:
            df.to_csv(f"output/{filename}.csv")
        n+=1
        df_to_return = pd.concat([df_to_return, df])
    return df_to_return
    
#############################################################################################
# Helper Function
# Note: this function has been imported from 
# "https://greendelta.github.io/openLCA-ApiDoc/results/impacts/upstream_trees.html" 
# and modified as shown below; the original method printed the result but ddnt return it
def expand(node: utree.Node, level: int, max_expand_levels: int, max_expand_nodes: int):
    results = []
    indent = (level, node.provider.name, node.result, node.direct_contribution) 
    results.append(indent)
    if level < max_expand_levels:
        for c in node.childs[0:max_expand_nodes]:
            results.extend(expand(c, level + 1, max_expand_levels, max_expand_nodes))
    return results
