def list_sub(list_value, start_value, end_value, start_include=True, end_include=False):
    """ Returns the value in the range of start_value and end_value """
    if start_include and end_include:
        return [x for x in sorted(list_value) if start_value <= x <= end_value]
    elif start_include and not end_include:
        return [x for x in sorted(list_value) if start_value <= x < end_value]
    elif not start_include and end_include:
        return [x for x in sorted(list_value) if start_value < x <= end_value]
    elif not start_include and not end_include:
        return [x for x in sorted(list_value) if start_value < x < end_value]
