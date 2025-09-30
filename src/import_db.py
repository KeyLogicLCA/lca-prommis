#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# import_db.py
#
###############################################################################
# DEPENDENCIES
###############################################################################
import requests
import logging
import os


###############################################################################
# DOCUMENTATION
###############################################################################
__doc__ = """
This script includes a code that downloads an openLCA database from
`EDX <http://edx.netl.doe.gov/>_`.

Source:
https://github.com/NETL-RIC/ElectricityLCI/blob/71a3f6dd20ed36bfcea3ff64b5e27d117ca7adc5/electricitylci/utils.py#L517

"""
__all__ = [
    "check_output_dir",
    "download_edx",
    "import_db",
]


###############################################################################
# FUNCTIONS
###############################################################################
def import_db(resource_id):
    """Helper function to download a file from EDX to resources folder."""
    api_key = input("Enter your EDX API key: ")
    resources_path = os.path.abspath(os.path.join(os.getcwd(), '../resources'))
    if not os.path.exists(resources_path):
        os.makedirs(resources_path)
    output_dir = resources_path
    download_edx(resource_id, api_key, output_dir)
    return True


def download_edx(resource_id, api_key, output_dir):
    """Download a resource from EDX to a given folder.

    Parameters
    ----------
    resource_id : str
        Resource ID (see id field) for a file on EDX.
    api_key : str
        User's API key (found on user's home page on EDX).
    output_dir : str
        A folder path to save the resource (will be created, if it does not
        exist).

    Returns
    -------
    tuple
        A tuple of length two:

        - bool, Whether the download was successful (or file already exists).
        - str, the resource file name (does not include the path).

    Notes
    -----
    Methods based on EDX API documentation
    https://edx.netl.doe.gov/sites/edxapidocs/downloadResourceFiles.html
    """
    # Catch missing API key.
    if api_key is None or api_key ==  '':
        api_key = input("Enter your EDX API key: ")

    # Define API headers and parameters.
    headers = {
        "EDX-API-Key": api_key,
        "User-Agent": 'EDX-USER',
    }
    params = {'resource_id': resource_id}
    url = 'https://edx.netl.doe.gov/api/3/resource_download'

    # Get filename from headers.
    logging.info("Sending request to EDX for resource data...")
    response_head = requests.head(url, headers=headers, params=params)
    if response_head.status_code != 200:
        err_str = (
            f"Failed to get EDX {resource_id} resource data. "
            f"Status code: {response_head.status_code}. "
        )
        logging.error(err_str)
        return (False, None)

    # Set the filename from the Content-Disposition header if available
    filename = None
    content_disposition = response_head.headers.get('Content-Disposition')
    if content_disposition and 'filename=' in content_disposition:
        filename = content_disposition.split('filename=')[-1].strip('"')

    # Get the content length from headers and determine resource size.
    content_length = response_head.headers.get('Content-Length')
    resource_size = int(content_length) if content_length is not None else None

    logging.debug("Resource Name:", filename)
    logging.debug(f"Resource Size: {resource_size} bytes")

    # HOTFIX: assign the output directory
    if filename is not None and check_output_dir(output_dir):
        filename = os.path.join(output_dir, filename)

    # Determine if partial file exists
    existing_size = 0
    if os.path.exists(filename):
        existing_size = os.path.getsize(filename)
        logging.warning(
            "File already exists. "
            f"The current file size is: {existing_size} bytes."
        )

        if resource_size is not None:
            logging.info(f"Resource file size: {resource_size} bytes")
            if existing_size >= resource_size:
                logging.info(
                    f"File already fully downloaded in {output_dir}."
                )
                return (True, os.path.basename(filename))

        headers['Range'] = f'bytes={existing_size}-'
        logging.info(f"Resuming download from byte: {existing_size}")
    else:
        logging.info(f"Starting download for: {filename}")

    # Begin download stream
    logging.debug(" ".join([str(headers), url]))
    response = requests.get(url, headers=headers, params=params, stream=True)

    logging.debug(f"Download response status code: {response.status_code}")
    if response.status_code in (200, 206):
        # If the server returns a 206 (for partial content), use 'ab' mode to
        # append
        mode = 'ab' if response.status_code == 206 else 'wb'
        total_bytes = existing_size

        logging.info(f"Saving to: {os.path.abspath(filename)}")
        with open(filename, mode) as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    total_bytes += len(chunk)
                    if resource_size:
                        percent = (total_bytes / resource_size) * 100
                        print(
                            f"\rDownloaded: {total_bytes} bytes "
                            f"({percent:.2f}%)",
                            end='',
                            flush=True
                        )
                    else:
                        # If resource size is unknown, just show bytes
                        # downloaded
                        print(
                            f"\rDownloaded: {total_bytes} bytes",
                            end='',
                            flush=True
                        )

        print(f"\nDownload complete.")
        logging.debug(f"Total bytes downloaded: {total_bytes}")
        return (True, os.path.basename(filename))
    else:
        logging.error(f"Download Failed. Status code: {response.status_code}")
        try:
            logging.debug("Response:", response.json())
        except Exception:
            logging.debug("Non-JSON response:", response.text)
        finally:
            return (False, None)


def check_output_dir(out_dir):
    """Helper method to ensure a directory exists.

    If a given directory does not exist, this method attempts to create it.

    Parameters
    ----------
    out_dir : str
        A path to a directory.

    Returns
    -------
    bool
        Whether the directory exists.
    """
    if not os.path.isdir(out_dir):
        try:
            # Start with super mkdir
            os.makedirs(out_dir)
        except:
            logging.warning("Failed to create folder %s!" % out_dir)
            try:
                # Revert to simple mkdir
                os.mkdir(out_dir)
            except:
                logging.error("Could not create folder, %s" % out_dir)
            else:
                logging.info("Created %s" % out_dir)
        else:
            logging.info("Created %s" % out_dir)

    return os.path.isdir(out_dir)
