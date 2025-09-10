# LCA-PrOMMiS Integrated Application
This jupyter lab application falls at the intersection of life cycle assessment (LCA) and the process optimization and modeling for minerals sustainability (PrOMMiS). This application (in it's current version) allows users to evaluate critical mineral processing flowsheets via PrOMMiS, and use the corresponding results to evaluate the environmental impact of the flowsheet using life cycle assessment. 
PrOMMiS is an open-source code, that enables design choices with costing, to perform process optimization, and to accelerate development and deployment of extraction and purification processes for critical minerals/rare earth elements at reduced risk. ([.html])(https://github.com/prommis/prommis)

## Disclaimer
The National Energy Technology Lanboratory (NETL) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use. 

## Repository Organization

    lca-prommis/
    ├── images/  
    |   ├── uky_flowsheet.png    
    │
    ├── output/             
    │   ├── lca_df.csv                              <- csv containing the raw data from prommis 
    |   ├── lca_df_converted.csv                    <- csv containing the prommis data converted to lca format 
    |   ├── lca_df_finalized.csv                    <- csv containing the finalized lca data (normalized to FU) 
    |
    |
    ├── src/ 
    │   ├── create_olca_process
    │   │   ├── __init__.py
    │   │   ├── create_new_process.py               <- main function to create new process in openLCA
    │   │   ├── create_exchange_elementary_flow.py  <- function to create an exchange for an elementary flow
    │   │   ├── create_exchange_pr_wa_flow.py       <- function to create an exchange for product and waste flows
    │   │   ├── create_exchange_database.py         <- function to create an exchange database
    │   │   ├── find_processes_by_flow.py           <- function to query an openLCA database and find the 
                                                        provider for specific flows
    |   |   ├── flow_search_function.py             <- function to query an openLCA database and find a flow by 
                                                        keyword
    │   │   └── search_flows.py                     <- user interface code to search for flows
    │   ├── __init__.py
    │   ├── prommis_LCA_data.py                     <- code to run PrOMMiS model and extract data
    │   ├── prommis_LCA_conversions.py              <- code to convert PrOMMiS data to LCA relevant units
    │   ├── finalize_LCA_flows.py                   <- code to normalize data to FU and assign UUIDs to 
                                                        elementary flows
    │   ├── create_ps.py                            <- function to create product system given a unit process
    │   ├── run_analysis.py                         <- function to assign impact assessment method and run 
                                                        analysis
    │   ├── generate_total_results.py               <- function to generate total LCA results
    │   └── generate_contribution_tree.py           <- function to generate results by category 
                                                        (contribution tree)
    │
    ├── .gitignore                                  <- Git repo ignore list
    ├── Notes.txt                                   <- Notes summarizing approach to develop the PrOMMiS LCA model
    ├── README.md                                   <- The top-level README.
    ├── requirements.txt                            
    └── PrOMMiS_LCA_Model.ipynb                     <- Jupyter notebook with steps to develop LCA model

## Setup

The instructions for setup are based on those found [here](https://idaes-pse.readthedocs.io/en/stable/tutorials/getting_started/mac_osx.html).

1. Create new virtual environment

    ```bash
    conda create -n prommis python=3.11 -y
    ```

2. Activate

    ```bash
    activate prommis
    ```

3. Install prommis

    ```bash
    pip install prommis
    ```

4. (Optional) Check the version of IDAES

    ```bash
    idaes --version
    ```

5. Install the extensions

    ```bash
    idaes get-extensions --extra petsc
    ```

6. Test the installation (and be prepared to wait)

    ```bash
    pytest --pyargs idaes -W ignore
    ```
    
7. Download or clone the repository

    To download, go to the "Code" page of the repository, click the green "Code" button and click "Download ZIP".

    To clone:
    ```bash
    git clone https://github.com/KeyLogicLCA/lca-prommis.git
    ```

8. Install JupyterLab (Recommended)

    ```bash
    pip install jupyterlab
    ```

## Install and Run

1. Ensure you're in the correct directory

    ```bash
    cd lca-prommis
    ```

2. Launch Jupyter Notebook

    ```bash
    jupyter lab
    ```

3. Open PrOMMiS_LCA_Model.ipynb and run the cells in order, following the provided instructions as you go

## 
