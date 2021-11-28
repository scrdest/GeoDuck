import os
import typing

REGISTRY_COLLISION_MSG = "Key {key} collides with existing entry {entry} in the {repo} registry!"
DEFAULT_REGISTRY_KEY = 'default'

_registries = {}


def identity_deco(target: typing.Any):
    """A no-op decorator"""
    return target


def registry_entry(
    as_key: typing.Hashable,
    registry_key: typing.Optional[typing.Hashable] = None,
    registry_data: typing.Optional[dict] = None,
    warn_on_collision: bool = True,
    halt_on_collision: bool = True,
    enabled: bool = None
) -> typing.Callable:

    _enabled = ((
        # Look up an envvar that disables it...
        False if os.getenv("GEOSCRAPE_DISABLE_REGISTRIES", "").upper() == "FALSE"
        # ...defaulting to True (enabled/not disabled)...
        else True
    ) if enabled is None  # ...if 'enabled' is not set explicitly...
      else bool(enabled)  # ...because if it is, we just take the user's word for it.
    )

    _repokey = registry_key or DEFAULT_REGISTRY_KEY

    def _registry_deco(target: typing.Any):
        repo = get_registry(
            registry_key=_repokey,
            raise_on_missing=False
        )

        if not repo:
            if registry_data:
                put_registry(registry_data, _repokey)

            else:
                from app.parsers._registry_backend import _parser_registry as new_registry
                put_registry(new_registry, _repokey)

        target_registry = get_registry(_repokey) or dict()
        curr_entry = target_registry.get(as_key, NotImplemented)

        if curr_entry is NotImplemented:
            target_registry[as_key] = target
            put_registry(target_registry, _repokey)

        else:
            if halt_on_collision:
                raise RuntimeError(
                    REGISTRY_COLLISION_MSG.format(
                        key=as_key,
                        entry=curr_entry,
                        repo=_repokey
                    )
                )

            elif warn_on_collision:
                print(
                    REGISTRY_COLLISION_MSG.format(
                        key=as_key,
                        entry=curr_entry,
                        repo=_repokey
                    )
                )

        return target

    return _registry_deco if _enabled else identity_deco


def get_registry(registry_key: typing.Optional[typing.Hashable] = None, raise_on_missing: bool = True):
    # Black magic to ensure autoscan of the modules
    # noinspection PyUnresolvedReferences
    from app import parsers, processing_backends, interface

    _repokey = registry_key or DEFAULT_REGISTRY_KEY
    try: retrieved = _registries[_repokey]
    except KeyError as KEr:
        if raise_on_missing: raise KEr
        else: retrieved = None
    return retrieved


def put_registry(registry_data, registry_key: typing.Optional[typing.Hashable] = None):
    _repokey = registry_key or DEFAULT_REGISTRY_KEY
    _registries[_repokey] = registry_data
    return True
