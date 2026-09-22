"""Phase 1 checks for the installable project package boundary."""


def test_package_is_importable() -> None:
    """The container must resolve the src-layout package used by future jobs and tests."""
    import nyc_taxi_lakehouse

    assert nyc_taxi_lakehouse.__doc__ is not None
