import functools
import os
from typing import Annotated, Self

from fastapi import Depends

from diracx.core.extensions import select_from_extension
from diracx.routers.auth import AuthorizedUserInfo, verify_dirac_access_token


class BaseAccessPolicy:
    @classmethod
    def check(cls) -> Self:
        raise NotImplementedError("This should never be called")

    @classmethod
    def available_implementations(cls, access_policy_name: str):
        """Return the available implementations of the AccessPolicy in reverse priority order."""
        policy_classes: list[type[BaseAccessPolicy]] = [
            entry_point.load()
            for entry_point in select_from_extension(
                group="diracx.access_policies", name=access_policy_name
            )
        ]
        if not policy_classes:
            raise NotImplementedError(
                f"Could not find any matches for {access_policy_name=}"
            )
        return policy_classes


def check_permissions(
    policy,
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
):

    has_been_called = False

    @functools.wraps(policy)
    async def wrapped_policy(**kwargs):
        """This wrapper is just to update the has_been_called flag"""
        nonlocal has_been_called
        has_been_called = True
        return await policy(user_info, **kwargs)

    try:
        yield wrapped_policy
    finally:
        if not has_been_called:
            # TODO nice error message with inspect
            # That should really not happen
            print(
                "THIS SHOULD NOT HAPPEN, ALWAYS VERIFY PERMISSION",
                "(PS: I hope you are in a CI)",
                flush=True,
            )
            os._exit(1)
