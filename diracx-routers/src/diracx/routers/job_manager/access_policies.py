import functools
import os
from enum import StrEnum, auto
from typing import Annotated, Callable

from fastapi import Depends, HTTPException, status

from diracx.core.properties import JOB_ADMINISTRATOR, NORMAL_USER
from diracx.db.sql import JobDB

from ..auth import AuthorizedUserInfo, verify_dirac_access_token


class ActionType(StrEnum):
    CREATE = auto()
    READ = auto()
    MANAGE = auto()
    QUERY = auto()


async def default_wms_policy(
    user_info: AuthorizedUserInfo,
    /,
    *,
    action: ActionType,
    job_db: JobDB,
    job_ids: list[int] | None = None,
):
    if action == ActionType.CREATE:
        if job_ids is not None:
            raise NotImplementedError(
                "job_ids is not None with ActionType.CREATE. This shouldn't happen"
            )
        if NORMAL_USER not in user_info.properties:
            raise HTTPException(status.HTTP_403_FORBIDDEN)
        return

    if JOB_ADMINISTRATOR in user_info.properties:
        return

    if NORMAL_USER not in user_info.properties:
        raise HTTPException(status.HTTP_403_FORBIDDEN)

    if action == ActionType.QUERY:
        if job_ids is not None:
            raise NotImplementedError(
                "job_ids is not None with ActionType.QUERY. This shouldn't happen"
            )
        return

    if job_ids is None:
        raise NotImplementedError("job_ids is None. his shouldn't happen")

    # TODO: check the CS global job monitoring flag

    job_owners = await job_db.summary(
        ["Owner", "VO"],
        [{"parameter": "JobID", "operator": "in", "values": job_ids}],
    )

    expected_owner = {
        "Owner": user_info.preferred_username,
        "VO": user_info.vo,
        "count": len(set(job_ids)),
    }
    # All the jobs belong to the user doing the query
    # and all of them are present
    if job_owners == [expected_owner]:
        return

    raise HTTPException(status.HTTP_403_FORBIDDEN)


def check_permissions(
    user_info: Annotated[AuthorizedUserInfo, Depends(verify_dirac_access_token)],
):

    has_been_called = False

    # TODO: query the CS to find the actual policy
    policy = default_wms_policy

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


CheckPermissionsCallable = Annotated[Callable, Depends(check_permissions)]

# router = DiracxRouter(dependencies=[has_properties(NORMAL_USER | JOB_ADMINISTRATOR)])
