# README
Find this resource on NETL's [Energy Data eXchange](https://edx.netl.doe.gov/dataset/lca-prommis)!

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
