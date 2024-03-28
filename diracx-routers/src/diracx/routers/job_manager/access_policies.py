from __future__ import annotations

from enum import StrEnum, auto
from typing import Annotated, Callable

from fastapi import Depends, HTTPException, status

from diracx.core.properties import JOB_ADMINISTRATOR, NORMAL_USER
from diracx.db.sql import JobDB
from diracx.routers.access_policies import BaseAccessPolicy

from ..auth import AuthorizedUserInfo


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


class WMSAccessPolicy(BaseAccessPolicy):
    # policy = staticmethod(policy_implementation)
    @staticmethod
    def policy(
        user_info: AuthorizedUserInfo,
        /,
        *,
        action: ActionType,
        job_db: JobDB,
        job_ids: list[int] | None = None,
    ):
        return default_wms_policy(
            user_info, action=action, job_db=job_db, job_ids=job_ids
        )


CheckPermissionsCallable = Annotated[Callable, Depends(WMSAccessPolicy.check)]

# router = DiracxRouter(dependencies=[has_properties(NORMAL_USER | JOB_ADMINISTRATOR)])
