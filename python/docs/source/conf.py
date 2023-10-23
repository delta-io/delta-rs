# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

import toml

sys.path.insert(0, os.path.abspath("../deltalake/"))
sys.path.insert(0, os.path.abspath("./_ext"))


def get_release_version() -> str:
    """Get the release version from the Cargo.toml file.

    :return:
    """
    cargo_content = toml.load("../../Cargo.toml")
    return cargo_content["package"]["version"]


# -- Project information -----------------------------------------------------

project = "delta-rs"
copyright = "2021 delta-rs contributors"
author = "delta-rs contributors"
version = get_release_version()


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.napoleon",
    "sphinx_rtd_theme",
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "edit_on_github",
]
autodoc_typehints = "description"
nitpicky = True
nitpick_ignore = [
    ("py:class", "pyarrow.lib.Schema"),
    ("py:class", "pyarrow._dataset.Dataset"),
    ("py:class", "pyarrow.lib.Table"),
    ("py:class", "pyarrow.lib.DataType"),
    ("py:class", "pyarrow.lib.Field"),
    ("py:class", "pyarrow.lib.NativeFile"),
    ("py:class", "pyarrow.lib.RecordBatchReader"),
    ("py:class", "pyarrow._fs.FileSystem"),
    ("py:class", "pyarrow._fs.FileInfo"),
    ("py:class", "pyarrow._fs.FileSelector"),
    ("py:class", "pyarrow._fs.FileSystemHandler"),
    ("py:class", "deltalake._internal.RawDeltaTable"),
    ("py:class", "pandas.DataFrame"),
    ("py:class", "pyarrow._dataset_parquet.ParquetFileWriteOptions"),
    ("py:class", "pathlib.Path"),
    ("py:class", "datetime.datetime"),
    ("py:class", "datetime.timedelta"),
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

edit_on_github_project = "delta-io/delta-rs"
edit_on_github_branch = "main"
page_source_prefix = "python/docs/source"


intersphinx_mapping = {
    "pyarrow": ("https://arrow.apache.org/docs/", None),
    "pyspark": ("https://spark.apache.org/docs/latest/api/python/", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
}
