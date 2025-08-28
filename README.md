# LCA-PrOMMiS Integrated Application
This jupyter lab application falls at the intersection of life cycle assessment (LCA) and the process optimization and modeling for minerals sustainability (PrOMMiS). This application (in it's current version) allows users to evaluate critical mineral processing flowsheets via PrOMMiS, and use the corresponding results to evaluate the environmental impact of the flowsheet using life cycle assessment. 
PrOMMiS is an open-source code, that enables design choices with costing, to perform process optimization, and
to accelerate development and deployment of extraction and purification processes for critical minerals/rare earth
elements at reduced risk. ([.html])(https://github.com/prommis/prommis)

## Disclaimer
The National Energy Technology Lanboratory (NETL) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use. 

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
