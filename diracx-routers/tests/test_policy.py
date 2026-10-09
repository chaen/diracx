from __future__ import annotations

import inspect
from collections import defaultdict
from collections.abc import Iterator
from typing import TYPE_CHECKING

from fastapi.routing import APIRoute

from diracx.core.extensions import DiracEntryPoint, select_from_extension
from diracx.routers.access_policies import (
    BaseAccessPolicy,
)

try:
    # FastAPI >= 0.137 nests the routes of the routers added with
    # include_router() behind _IncludedRouter objects
    from fastapi.routing import _IncludedRouter
except ImportError:
    # Placeholder which never matches any route for older FastAPI versions
    class _IncludedRouter:  # type: ignore[no-redef]
        pass


if TYPE_CHECKING:
    from diracx.routers.fastapi_classes import DiracxRouter


def iter_auth_required_routes(router: DiracxRouter) -> Iterator[APIRoute]:
    """Yield the API routes of a router, recursively descending into
    the routers added with include_router().

    Routers created with "require_auth=False" are skipped, as well as
    the routers they include.
    """
    if not getattr(router, "diracx_require_auth", True):
        return

    for route in router.routes:
        if isinstance(route, _IncludedRouter):
            yield from iter_auth_required_routes(route.original_router)
        elif isinstance(route, APIRoute):
            yield route


def test_all_routes_have_policy():
    """Loop over all the routers, loop over every route.

    Make sure there is a dependency on a BaseAccessPolicy class.

    If the router is created with "require_auth=False", we skip it.
    We also skip routes that have the "diracx_open_access" decorator

    """
    missing_security: defaultdict[list[str]] = defaultdict(list)
    for entry_point in select_from_extension(group=DiracEntryPoint.SERVICES):
        router: DiracxRouter = entry_point.load()

        for route in iter_auth_required_routes(router):
            # If the route is decorated with the diracx_open_access
            # decorator, we skip it
            if getattr(route.endpoint, "diracx_open_access", False):
                continue

            for dependency in route.dependant.dependencies:
                if inspect.ismethod(dependency.call) and issubclass(
                    dependency.call.__self__, BaseAccessPolicy
                ):
                    # We found a dependency on check_permissions
                    break
            else:
                # We looked at all dependency without finding
                # check_permission
                missing_security[entry_point.name].append(route.name)

    assert not missing_security
