from schedr import greet


def test_greet_formats_name() -> None:
    assert greet("schedr") == "Hello, schedr!"
