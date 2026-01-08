def msg(key, **params):
    """
    Emit i18n message payload for CLI / UI.
    """
    if params:
        return {"key": key, "params": params}
    return {"key": key}

def print_msg(key, **params):
    print(msg(key, **params))

