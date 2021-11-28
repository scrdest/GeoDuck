import os
from setuptools import setup, find_packages

SETUP_ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
VERSION_FILEPATH = os.path.join(SETUP_ROOT_DIR, "version.txt")
PROMPT_TOOLKIT_AVAILABLE = True


def get_version():
    version = None

    with open(VERSION_FILEPATH) as version_file:
        for line in version_file:
            version = line.strip()
            if version:
                break

    return version


def main():
    invoke_entrypoint = "app.interface.definitions.invoke_cli:run_program"
    cli_entrypoint = "app.main:entrypoint"

    console_scripts = (
        f"geoduck-invoke = {invoke_entrypoint}",
        f"geoduck = {cli_entrypoint}",
    )

    setup_attrs = {
        "name": "geoduck",
        "packages": find_packages(),
        "url": "https://github.com/scrdest/GeoDuck",
        "entry_points": {
            "console_scripts": console_scripts
        }
    }

    version = get_version()
    if version:
        setup_attrs["version"] = version

    setup(**setup_attrs)


if __name__ == '__main__':
    main()

