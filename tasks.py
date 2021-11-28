from app.interface.definitions.invoke_cli import *

if __name__ == '__main__':
    from invoke import Context
    ctx = Context()
    fetch_normalized(ctx)
