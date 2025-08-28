# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'LCA_PrOMMiS Integration'
copyright = '2025, Tyler Davis, Francis Hanna, Daniel Naiman, Shirley Sam'
author = 'Tyler Davis, Francis Hanna, Daniel Naiman, Shirley Sam'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = []

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']

import os
import sys
sys.path.insert(0, os.path.abspath('../src'))

extensions = ['myst_parser', 'sphinx.ext.autodoc']

source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}
