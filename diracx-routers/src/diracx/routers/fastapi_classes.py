from __future__ import annotations

__all__ = ["DiracxRouter"]

import asyncio
import contextlib
from collections.abc import Iterator
from typing import Any, Callable, TypeVar

from fastapi import APIRouter, FastAPI
from fastapi.routing import APIRoute, _IncludedRouter

from diracx.tasks.plumbing.depends import auto_inject

T = TypeVar("T")


def _downgrade_openapi_schema(data):
    """Modify an openapi schema in-place to be compatible with AutoRest."""
    if isinstance(data, dict):
        for k, v in list(data.items()):
            if k == "anyOf":
                if {"type": "null"} in v:
                    v.pop(v.index({"type": "null"}))
                    data["nullable"] = True
                    if len(v) == 1:
                        data |= v[0]
            elif k == "const":
                data.pop(k)
            # https://github.com/fastapi/fastapi/discussions/12984
            elif k == "propertyNames":
                data.pop(k)

            _downgrade_openapi_schema(v)
    if isinstance(data, list):
        for v in data:
            _downgrade_openapi_schema(v)


def _iter_routers(router: APIRouter) -> Iterator[APIRouter]:
    """Yield the router, then recursively the routers it includes.

    FastAPI guarantees that a router cannot include itself, directly or
    indirectly, so the recursion always terminates.
    """
    yield router
    for route in router.routes:
        if isinstance(route, _IncludedRouter):
            yield from _iter_routers(route.original_router)


class DiracFastAPI(FastAPI):
    def __init__(self):
        @contextlib.asynccontextmanager
        async def lifespan(app: DiracFastAPI):
            async with contextlib.AsyncExitStack() as stack:
                await asyncio.gather(
                    *(stack.enter_async_context(f()) for f in app.lifetime_functions)
                )
                yield

        self.lifetime_functions = []
        super().__init__(
            swagger_ui_init_oauth={
                "clientId": "myDIRACClientID",
                "scopes": "property:NormalUser",
                "usePkceWithAuthorizationCodeGrant": True,
            },
            generate_unique_id_function=lambda route: f"{route.tags[0]}_{route.name}",
            title="Dirac",
            lifespan=lifespan,
            openapi_url="/api/openapi.json",
            docs_url="/api/docs",
            swagger_ui_oauth2_redirect_url="/api/docs/oauth2-redirect",
        )
        # FIXME: when autorest will support 3.1.0
        # From 0.99.0, FastAPI is using openapi 3.1.0 by default
        # This version is not supported by autorest yet
        self.openapi_version = "3.0.2"

    def openapi(self, *args, **kwargs):
        if not self.openapi_schema:
            super().openapi(*args, **kwargs)
            _downgrade_openapi_schema(self.openapi_schema)

            # Remove 422 responses as we don't want autorest to use it
            for _, method_item in self.openapi_schema.get("paths").items():
                for _, param in method_item.items():
                    responses = param.get("responses")
                    if "422" in responses:
                        del responses["422"]

        return self.openapi_schema


class DiracxRouter(APIRouter):
    def __init__(
        self,
        *,
        dependencies=None,
        require_auth: bool = True,
        include_in_schema: bool = True,
        path_root: str = "/api",
    ):
        super().__init__(dependencies=dependencies, include_in_schema=include_in_schema)
        self.diracx_require_auth = require_auth
        self.diracx_path_root = path_root

    ####
    # This method is needed to overwrite routes
    # https://github.com/tiangolo/fastapi/discussions/8489

    def add_api_route(self, path: str, endpoint: Callable[..., Any], **kwargs):
        endpoint = auto_inject(endpoint)

        self._remove_overridden_route(path, set(kwargs.get("methods", [])))

        return super().add_api_route(path, endpoint, **kwargs)

    def _remove_overridden_route(self, path: str, methods: set[str]) -> None:
        """Remove the route overridden by the route being added.

        The overridden route can belong to this router or to one of the
        routers added with include_router(): since FastAPI 0.137.0,
        include_router() stores the included router in router.routes
        (behind an _IncludedRouter object) instead of copying its routes.
        """
        for router in _iter_routers(self):
            for index, route in enumerate(router.routes):
                if (
                    isinstance(route, APIRoute)
                    and route.path == path
                    and route.methods == methods
                ):
                    router.routes.pop(index)
                    return

    ######
