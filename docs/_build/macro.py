from collections import OrderedDict
from pathlib import Path
from typing import List, Optional, Set
import yaml
import logging

LANGUAGES = OrderedDict(
    python={"extension": ".py", "display_name": "Python", "icon_name": "python"},
    rust={"extension": ".rs", "display_name": "Rust", "icon_name": "rust"},
)

EXAMPLES_BASE_PATH = Path("docs/src")

with open("docs/_build/links.yml", "r") as f:
    API_LINKS = yaml.load(f, Loader=yaml.CLoader)


def build_api_link(language: str, api_ref: str) -> Optional[str]:
    link = API_LINKS.get(language, {}).get(api_ref)

    if link:
        return f"[:material-api:  `{api_ref}`]({link})"
    return None


def build_code_tab(
    file_path: Path, section: str, language: str, api_refs: List[str]
) -> str:
    """Create a code tab a specific language"""
    if not Path.exists(file_path):
        return None

    language_info = LANGUAGES.get(language)

    # Create API links
    api_links = [link for l in api_refs if (link := build_api_link(language, l))]
    tab_header = " ·".join(api_links)

    # Use section if defined
    code = file_path if not section else f"{file_path}:{section}"

    return f"""=== \":fontawesome-brands-{language_info['icon_name']}: {language_info['display_name']}\"
    {tab_header}
    ```{language}
    --8<-- \"{code}\"
    ```
    """


def get_file_path(example: str, language: str) -> Path:
    return EXAMPLES_BASE_PATH.joinpath(language).joinpath(
        f"{example}{LANGUAGES.get(language)['extension']}"
    )


def define_env(env):
    @env.macro
    def code_example(
        example: str, section: str = None, api_refs: List[str] = None
    ) -> str:
        """Build a markdown with code examples for each supported language. No content will be rendered
        for languages which do not have a corresponding example

        Args:
            example(str): name of the example, matching the file name under `EXAMPLES_BASE_PATH/<language>` without the file extension
            section(str, optional): optional named section within the example file. Each example file may contain multiple examples,
                                    marked by `# --8<-- [start/end:<section>]`. Ignored if None.
            api_refs(List[str], optional): optional list of references to API resources as defined in `links.yml`

        Returns:
            Markdown containing tabbed code blocks
        """
        return "\n".join(
            [
                code
                for lang in LANGUAGES.keys()
                if (
                    code := build_code_tab(
                        get_file_path(example, lang), section, lang, api_refs
                    )
                )
            ]
        )
