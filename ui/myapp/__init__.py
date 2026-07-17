"""Internal CSV import console."""


def create_app(*args, **kwargs):
    """Create the Flask app without importing web/database dependencies eagerly."""
    from .app import create_app as factory

    return factory(*args, **kwargs)


__all__ = ["create_app"]
