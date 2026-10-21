import schedr._model as model


def test_every_exported_name_exists() -> None:
    for name in model.__all__:
        assert hasattr(model, name), name


def test_exports_are_sorted_and_unique() -> None:
    assert list(model.__all__) == sorted(set(model.__all__))
