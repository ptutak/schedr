"""PYPI.md, the package's page on PyPI: its DAG is examples/sync.py, word for word."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def _python_block(markdown: str) -> str:
    start = markdown.index("```python\n") + len("```python\n")
    return markdown[start : markdown.index("\n```", start)]


def test_the_pypi_dag_is_the_example_without_its_docstring() -> None:
    example = (ROOT / "examples" / "sync.py").read_text()
    if example.startswith('"""'):
        example = example.split("\n", 2)[2]
    page = (ROOT / "PYPI.md").read_text()
    assert _python_block(page) == example.strip()


def test_the_package_shows_the_pypi_page() -> None:
    assert 'readme = "PYPI.md"' in (ROOT / "pyproject.toml").read_text()
