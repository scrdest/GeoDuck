from functools import wraps

from prompt_toolkit import Application

import app.constants as const
from app.utils.registry import registry_entry


def build_layout():
    from prompt_toolkit.layout.layout import Layout
    from prompt_toolkit.layout.containers import VSplit, Window
    from prompt_toolkit.buffer import Buffer
    from prompt_toolkit.layout.controls import BufferControl, FormattedTextControl

    buffer = Buffer()

    root_container = VSplit([
        Window(content=BufferControl(buffer=buffer)),
        Window(width=1, char='|'),
        Window(content=FormattedTextControl(text='Ctrl+Q to Quit'))
    ])
    layout = Layout(root_container)
    return layout


def build_key_bindings():
    from prompt_toolkit.key_binding import KeyBindings
    binds = KeyBindings()

    def quit_app(event):
        event.app.exit()

    binds.add('c-q')(quit_app)
    return binds


def build_app(layout=None, key_bindings=None):
    _layout = layout or build_layout()
    _key_bindings = key_bindings or build_key_bindings()
    app = Application(full_screen=True, layout=_layout, key_bindings=_key_bindings)

    return app


def read_args(app=None, preloaded_args=None):
    _app = app or build_app()
    _app.run()
    return


@registry_entry('rich_cli', registry_key=const.DEFAULT_INTERFACE_REGISTRY_KEY)
def rich_cli_parser(cli_args, *args, **kwargs):

    def _rich_cli_deco(func):

        @wraps(func)
        def _rich_cli_wrapper(*fargs, **fkwargs):
            amended_kwargs = fkwargs.copy()
            cli_args = read_args(app=None)
            amended_kwargs.update(cli_args or {})

            result = func(*fargs, **amended_kwargs)
            return result

        return _rich_cli_wrapper

    return _rich_cli_deco
