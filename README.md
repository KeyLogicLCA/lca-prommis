# LCA-PrOMMiS Integrated Application
The PrOMMiS–LCA toolkit includes two Jupyter Notebooks that integrate process modeling for mineral sustainability (PrOMMiS) with life cycle assessment (LCA). Together, they allow users to evaluate critical mineral processing flowsheets and assess their environmental impacts.
The original notebook, PrOMMiS_LCA_Model.ipynb, provides an integrated workflow for modeling mineral processing flowsheets using PrOMMiS and evaluating their environmental performance through life cycle assessment.
A newly added notebook extends this functionality by incorporating flowsheet optimization in addition to integration with LCA. This version enables users to explore optimized processing configurations while quantifying their corresponding environmental impacts.
Each notebook includes its own setup instructions and usage guidance in a dedicated section below.
PrOMMiS is an open-source code, that enables design choices with costing, to perform process optimization, and to accelerate development and deployment of extraction and purification processes for critical minerals/rare earth elements at reduced risk ([.html](https://github.com/prommis/prommis)).

## Disclaimer
The National Energy Technology Laboratory (NETL) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use.

## Repository Organization

    lca-prommis/
    ├── images/
    │   ├── system_boundary_1.png
    |   ├── penalty_forms.png   
    │   └── uky_flowsheet.png
    │
    ├── output/
    │   ├── lca_df.csv                     <- PrOMMiS raw data CSV
    │   ├── lca_df_converted.csv           <- Unit-converted and aggregated data
    │   └── lca_df_finalized.csv           <- Normalized to functional unit
    │
    ├── resources                          <- directory for openLCA database
    │   |                                     downloaded from EDX
    |   └──foqus_node_scripts.py           <- scripts used in the PrOMMiS and olca nodes in foqus_class.py
    │
    ├── src/
    │   ├── create_olca_process        <-  Submodule for creating unit processes
    │   │   ├── __init__.py
    │   │   ├── create_exchange_elementary_flow.py  <- function to create an exchange for an elementary flow
    │   │   ├── create_exchange_pr_wa_flow.py       <- function to create an exchange for product and waste flows
    │   │   ├── create_exchange_database.py         <- function to create an exchange database
    │   │   ├── create_exchange_ref_flow.py         <- function to create an exchange for the quantitative reference
    │   │   │                                              flow
    │   │   ├── create_new_process.py               <- main function to create new process in openLCA
    │   │   ├── find_processes_by_flow.py           <- function to query an openLCA database and find the
    │   │   │                                              provider for specific flows
    |   │   ├── flow_search_function.py             <- function to query an openLCA database and find a flow by
    │   │   │                                              keyword
    │   │   ├── search_flows_and_providers.py       <- user interface code to search for flows and their associated
    │   │   │                                              providers
    │   │   └── search_flows_only.py                <- user interface code to search and extract only flows
    │   │
    │   ├── __init__.py
    │   ├── prommis_LCA_data.py                     <- code to run PrOMMiS model and extract data
    │   ├── prommis_LCA_conversions.py              <- code to convert PrOMMiS data to LCA relevant units
    │   ├── finalize_LCA_flows.py                   <- code to normalize data to FU and assign UUIDs to
    │   │                                                 elementary flows
    │   ├── create_ps.py                            <- function to create product system given a unit process
    │   ├── run_analysis.py                         <- function to assign impact assessment method and run
    │   │                                                 analysis
    │   ├── import_db.py                            <- function to import openLCA database from EDX
    │   ├── generate_total_results.py               <- function to generate total LCA results
    │   ├── generate_contribution_tree.py           <- function to generate results by category
    │   |                                                (contribution tree)
    │   ├── plot_results.py                         <- code to visualize the results
    |   └── foqus_class.py                          <- module containing the netlfoqus class
    |                                                    This class contains methods to develop an optimization 
    |                                                    framework using FOQUS and that interacts with PrOMMiS and
    |                                                    openLCA
    |
    ├── .gitignore                                  <- Git repo ignore list
    ├── Notes.txt                                   <- Notes summarizing approach to develop the PrOMMiS LCA model
    ├── README.md                                   <- The top-level README.
    ├── requirements.txt
    ├── setup.py                                    <- Python packaging script used to install and configure the package
    ├── pyproject.toml                              <- toml file defining project configuration
    ├── PrOMMiS_LCA_Model.ipynb                     <- Jupyter notebook with steps to develop LCA model
    └── IDAES_LCA_Integration.ipynb                 <- Jupyter notebook with steps to develop IDAES-LCA 
                                                         optimization model

## PrOMMiS-LCA One-Directional Integration 

### Setup

The instructions for setup are based on those found [here](https://idaes-pse.readthedocs.io/en/stable/tutorials/getting_started/mac_osx.html).

1. Create new virtual environment

    ```bash
    conda create -n prommis python=3.12 -y
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

    If this step fails, pip-install pyargs in the virtual environment and try again.

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

### Install and Run

1. Ensure you're in the correct directory

    ```bash
    cd lca-prommis
    ```

2. Launch Jupyter Notebook

    ```bash
    jupyter lab
    ```

3. Open PrOMMiS_LCA_Model.ipynb and run the cells in order, following the provided instructions as you go.

## PrOMMiS-LCA Bi-Directional Integration - Optimization Model Development

At present, the FOQUS environment has been successfully configured only on WSL Ubuntu due to unresolved dependency issues on Windows and macOS.

The instructions below focus on setting up openLCA, FOQUS, and the required python environment in Ubuntu.

### Setup

1. Install conda in Ubuntu

    ```bash
    `wget https://repo.anaconda.com/archive/Anaconda3-2025.12-1-Linux-x86_64.sh`
    `bash Anaconda3-2025.12-1-Linux-x86_64.sh`
    ```

2. Setup conda environment
    
    ``` bash
    conda create -n pflca python=3.11 or 3.12
    ```

3. Install fedelmflowlist

    ``` bash
    pip install git+https://github.com/FLCAC-admin/fedelemflowlist
    ```

4. Install netlolca

    ``` bash
    pip install git+https://github.com/NETL-RIC/netlolca#egg=netlolca
    ```

5. Install prommis

    ``` bash
    pip install git+https://github.com/prommis/prommis.git
    ```

6. Install foqus

    ``` bash
    pip install git+https://github.com/CCSI-Toolset/FOQUS
    ```

7. Install lca-prommis

    ``` bash
    pip install git+https://github.com/KeyLogicLCA/lca-prommis.git
    ```

8. Install psuade

    ``` bash
    conda install --yes -c conda-forge -c CCSI-Toolset psuade-lite=1.9
    ```

9. Install NLopt

    ``` bash
    conda install -y -c conda-forge nlopt
    ```

10. Get IDAES extensions

    ``` bash
    idaes get-extensions
    ```

11. Install ipopt

    ``` bash
    conda install -y -c conda-forge ipopt
    ```

12. Install tabulate

    ``` bash
    pip install tabulate==0.9.0
    ```

13. Install pyqt

    ``` bash
    pip install pyqt
    ```

14. Install openLCA for Linux 
    
    ``` bash
    wget https://share.greendelta.com/index.php/s/hcl5JAB1p0FxfFe/download
    ```

    ``` bash    
    mv download openLCA_mkl_Linux_x64_2.5.0_2025-06-16.tar.gz
    ```

    ``` bash    
    gunzip openLCA_mkl_Linux_x64_2.5.0_2025-06-16.tar.gz
    ```

    ``` bash    
    tar -xvf openLCA_mkl_Linux_x64_2.5.0_2025-06-16.tar
    ```

    ``` bash    
    mkdir Downloads
    ```
    
    ``` bash    
    mv openLCA_mkl_Linux_x64_2.5.0_2025-06-16.tar Downloads/
    ```

15. Test run openLCA
    
    ``` bash
    cd openLCA/
    ```

    ``` bash
    ./openLCA
    ```

16. Install Jupyter Lab
    ``` bash
    pip install jupyterlab
    ```

### Install and Run

3. Launch Jupyter Notebook

    ```bash
    jupyter lab
    ```

3. Open PrOMMiS_LCA_Model.ipynb and run the cells in order, following the provided instructions as you go.