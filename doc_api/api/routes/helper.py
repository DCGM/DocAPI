
def render_msg(msg_map: dict, code, **params) -> str:
    tmpl = msg_map.get(code)
    if tmpl is None:
        raise KeyError(f"No message template for {code}")
    try:
        return tmpl.format(**params)
    except KeyError as e:
        raise KeyError(f"Missing placeholder {e} for {code}") from e

class RouteInvariantError(RuntimeError):
    """Raised when a route reaches an impossible or unhandled state."""
    pass