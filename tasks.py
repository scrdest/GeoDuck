from invoke import task
from app.core import mainloop

import app.constants as const


@task()
def run(c):
    pass


@task()
def fetch_raw(c):
    run_kwargs = dict()
    run_kwargs[const.MAINARG_SAVE_DOWNLOADED] = True

    loop = mainloop.coreloop(**run_kwargs)
    for savepath in loop:
        print(f"Savepath: {savepath}")


@task()
def fetch_normalized(c):
    run_kwargs = dict()
    run_kwargs[const.MAINARG_SAVE_NORMALIZED] = True

    loop = mainloop.coreloop(**run_kwargs)
    for savepath in loop:
        print(f"Savepath: {savepath}")


if __name__ == '__main__':
    from invoke import Context
    ctx = Context()
    fetch_normalized(ctx)