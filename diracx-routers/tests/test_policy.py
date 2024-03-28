import inspect
from collections import defaultdict

from diracx.core.extensions import select_from_extension
from diracx.routers.access_policies import (
    BaseAccessPolicy,
)


def test_all_routes_have_policy():
    """
    Loop over all the routers, loop over every route,
    and make sure there is a dependency on a BaseAccessPolicy class
    """
    missing_security = defaultdict(list)
    for entry_point in select_from_extension(group="diracx.services"):
        router = entry_point.load()

        for route in router.routes:

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

    # TODO ASSERT FOR REAL
    assert not missing_security
    assert True
