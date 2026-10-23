"""Sphinx configuration for the schedr documentation site.

The pages from repository files (README, PLAN, the PROJECT.md chapters, the Rust crate list)
and the reference pages from the code (configuration, CLI, metrics, REST API) are generated
into ``_generated/`` by ``cargo xtask docs-sources`` (``make docs``); the guide (``guide/``) is
written by hand. Only this file and the index pages here are specific to Sphinx (see
PROJECT.md, Repository and tooling: Documentation).
"""

from importlib.metadata import version as _version

project = "schedr"
author = "Piotr Tutak"
release = _version("schedr")
version = release

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinxcontrib.mermaid",
]

source_suffix = {".md": "markdown", ".rst": "restructuredtext"}
exclude_patterns = ["_build"]
templates_path = ["_templates"]

# Markdown (MyST): anchors for headings so pages can link to sections.
myst_heading_anchors = 3
myst_enable_extensions = ["colon_fence", "deflist"]

# Diagrams: plain ```mermaid fences (as GitHub draws them) become the mermaid directive. The
# browser loads this mermaid.js version from a CDN.
myst_fence_as_directive = ["mermaid"]
mermaid_version = "12.1.0"

# Python API: generated from Google-style docstrings and type hints.
autosummary_generate = True
autodoc_default_options = {"members": True, "show-inheritance": True}
autodoc_typehints = "description"
autodoc_member_order = "bysource"
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_attr_annotations = True

html_theme = "furo"
html_title = f"schedr {release}"
