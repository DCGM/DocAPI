
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

    def __init__(self, request=None, code=None, message=None):
        import inspect
        frame = inspect.currentframe().f_back
        func_name = frame.f_code.co_name
        route_path = request.url.path if request else "unknown route"
        method = request.method if request else "?"
        base = f"[{method} {route_path}] Unexpected code '{code}' in {func_name}"
        if message:
            base += f": {message}"
        super().__init__(base)
        self.code = code
        self.caller = func_name
        self.route = route_path
        self.method = method