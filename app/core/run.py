from core import main

def run():
    from interface import get_interface
    interface = get_interface()
    wrapped_main = interface(main)
    status = wrapped_main()
    return status
