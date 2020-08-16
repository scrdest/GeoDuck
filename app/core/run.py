from core.app import main


def run():
    """Entrypoint. Wraps the main method with an interface (e.g. CLI or GUI),
    based on a bootstrap CLI argument for interface type (defaults to CLI).
    """
    from interface import get_interface
    interface = get_interface()
    wrapped_main = interface(main)
    status = wrapped_main()
    return status


if __name__ == '__main__':
    run()
