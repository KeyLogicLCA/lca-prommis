# README
Find this resource on NETL's [Energy Data eXchange](https://edx.netl.doe.gov/dataset/lca-prommis)!

## What's included
I don't know if any other sections are necessary?

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

## Running PrOMMiS_LCA_Model.ipynb

1. Ensure you're in the correct directory

    ```bash
    cd lca-prommis
    ```

2. Launch Jupyter Notebook

    ```bash
    jupyter lab
    ```

3. Open PrOMMiS_LCA_Model.ipynb and run the cells in order, following the provided instructions as you go



