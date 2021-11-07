import app.constants as const
from app.utils.registry import registry_entry


@registry_entry(const.INTERFACE_NONE, registry_key=const.DEFAULT_INTERFACE_REGISTRY_KEY)
def dummy_interface(*args, **kwargs):
    """A fake decorator (no/identity transform).
    Purely config-based interfacing, less risk of key collisions.
    """

    def _dummy_deco(func):
        return func

    return _dummy_deco
