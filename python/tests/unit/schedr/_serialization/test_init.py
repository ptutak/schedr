import schedr._serialization as serialization


def test_every_exported_name_exists() -> None:
    for name in serialization.__all__:
        assert hasattr(serialization, name), name
