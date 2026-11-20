from fastapi.routing import APIRoute, RouteContext, iter_route_contexts


def app_routes(app) -> list[RouteContext]:
    """Every route the app serves, those of included routers too, as FastAPI's
    RouteContext: it reads like the route, with the effective path, methods and
    dependencies. app.routes itself holds included routers as single entries."""
    return list(iter_route_contexts(app.routes))


def api_routes(app) -> list[RouteContext]:
    """The app's API routes (APIRoute), as in app_routes()."""
    return [route for route in app_routes(app) if isinstance(route.original_route, APIRoute)]


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
