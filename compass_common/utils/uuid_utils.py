from perceval.backend import uuid

def get_uuid(*args):
    """ Production of uuid according to args """
    args_list = []
    for arg in args:
        if arg is None or arg == '':
            continue
        args_list.append(arg)
    return uuid(*args_list)