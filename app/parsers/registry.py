import typing

import parsers._registry_backend as backend

REGISTRY_COLLISION_MSG = "Key {key} collides with existing entry {entry}!"


def register_parser(
    as_key: typing.Hashable,
    warn_on_collision=True,
    halt_on_collision=True
) -> typing.Callable:

    def _registry_deco(target: typing.Any):
        curr_entry = backend.parser_registry.get(as_key, NotImplemented)

        if curr_entry is NotImplemented:
            backend.parser_registry[as_key] = target

        else:
            if halt_on_collision:
                raise RuntimeError(
                    REGISTRY_COLLISION_MSG.format(
                        key=as_key, entry=curr_entry
                    )
                )

            elif warn_on_collision:
                print(
                    REGISTRY_COLLISION_MSG.format(
                        key=as_key,
                        entry=curr_entry
                    )
                )

        return target

    return _registry_deco
