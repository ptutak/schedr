from schedr.types import KiB, MiB


def test_size_constants() -> None:
    assert KiB == 1024
    assert MiB == 1024 * 1024
    assert 8 * MiB == 8_388_608
