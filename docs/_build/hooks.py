import re

from mkdocs.config import Config
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page

# These could be used to replace parts of the Python API reference links in `links.yml` when API docs are built here   
PYTHON_API_URL_TOKEN = "PYTHON_API_URL"
PYTHON_API_URL_CONFIG = "python_api_url"



def on_page_markdown(markdown: str, page: Page, config: Config, files: Files) -> str:
    return replace_local_links(
        markdown, config.get("extra", {}).get(PYTHON_API_URL_CONFIG, "")
    )


def replace_local_links(markdown: str, python_api_url: str) -> str:
    """Static references to Python API elements created as part of these docs may use a special
    placeholder for host and prefix instead of a static URL
    """
    return re.sub(PYTHON_API_URL_TOKEN, python_api_url, markdown, flags=re.M)
