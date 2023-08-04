from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from http import HTTPStatus
from typing import Annotated, Any, TypedDict
from unittest.mock import MagicMock

from fastapi import Body, Depends, HTTPException, Query
from pydantic import BaseModel, root_validator

from diracx.core.config import Config, ConfigSource
from diracx.core.job_status import get_job_status, set_job_status, set_job_status_bulk
from diracx.core.models import ScalarSearchOperator, SearchSpec, SortSpec, StatusModel
from diracx.core.properties import JOB_ADMINISTRATOR, NORMAL_USER
from diracx.core.utils import JobStatus

from ..auth import UserInfo, has_properties, verify_dirac_token
from ..dependencies import JobDB, JobLoggingDB
from ..fastapi_classes import DiracxRouter

MAX_PARAMETRIC_JOBS = 20

logger = logging.getLogger(__name__)

router = DiracxRouter(dependencies=[has_properties(NORMAL_USER | JOB_ADMINISTRATOR)])


class JobSummaryParams(BaseModel):
    grouping: list[str]
    search: list[SearchSpec] = []

    @root_validator
    def validate_fields(cls, v):
        # TODO
        return v


class JobSearchParams(BaseModel):
    parameters: list[str] | None = None
    search: list[SearchSpec] = []
    sort: list[SortSpec] = []

    @root_validator
    def validate_fields(cls, v):
        # TODO
        return v


class JobDefinition(BaseModel):
    owner: str
    group: str
    vo: str
    jdl: str


class InsertedJob(TypedDict):
    JobID: int
    Status: str
    MinorStatus: str
    TimeStamp: datetime


class JobID(BaseModel):
    job_id: int


class JobStatusUpdate(BaseModel):
    job_id: int
    status: JobStatus


class JobStatusReturn(TypedDict):
    job_id: int
    status: JobStatus


EXAMPLE_JDLS = {
    "Simple JDL": [
        """Arguments = "jobDescription.xml -o LogLevel=INFO";
Executable = "dirac-jobexec";
JobGroup = jobGroup;
JobName = jobName;
JobType = User;
LogLevel = INFO;
OutputSandbox =
    {
        Script1_CodeOutput.log,
        std.err,
        std.out
    };
Priority = 1;
Site = ANY;
StdError = std.err;
StdOutput = std.out;"""
    ],
    "Parametric JDL": ["""Arguments = "jobDescription.xml -o LogLevel=INFO"""],
}


@router.post("/")
async def submit_bulk_jobs(
    # FIXME: Using mutliple doesn't work with swagger?
    job_definitions: Annotated[list[str], Body(example=EXAMPLE_JDLS["Simple JDL"])],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    user_info: Annotated[UserInfo, Depends(verify_dirac_token)],
) -> list[InsertedJob]:
    from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
    from DIRAC.Core.Utilities.DErrno import EWMSJDL
    from DIRAC.WorkloadManagementSystem.Utilities.ParametricJob import (
        generateParametricJobs,
        getParameterVectorLength,
    )

    fixme_ownerDN = "ownerDN"
    fixme_ownerGroup = "ownerGroup"
    fixme_diracSetup = "diracSetup"

    # TODO: implement actual job policy checking
    # # Check job submission permission
    # result = JobPolicy(
    #     fixme_ownerDN, fixme_ownerGroup, fixme_userProperties
    # ).getJobPolicy()
    # if not result["OK"]:
    #     raise NotImplementedError(EWMSSUBM, "Failed to get job policies")
    # policyDict = result["Value"]
    # if not policyDict[RIGHT_SUBMIT]:
    #     raise NotImplementedError(EWMSSUBM, "Job submission not authorized")

    # TODO make it bulk compatible
    assert len(job_definitions) == 1

    jobDesc = f"[{job_definitions[0]}]"

    # TODO: that needs to go in the legacy adapter
    # jobDesc = jobDesc.strip()
    # if jobDesc[0] != "[":
    #     jobDesc = f"[{jobDesc}"
    # if jobDesc[-1] != "]":
    #     jobDesc = f"{jobDesc}]"

    # Check if the job is a parametric one
    jobClassAd = ClassAd(jobDesc)
    result = getParameterVectorLength(jobClassAd)
    if not result["OK"]:
        logger.error("Issue with getParameterVectorLength: %s", result["Message"])
        return result
    nJobs = result["Value"]
    parametricJob = False
    if nJobs is not None and nJobs > 0:
        # if we are here, then jobDesc was the description of a parametric job. So we start unpacking
        parametricJob = True
        if nJobs > MAX_PARAMETRIC_JOBS:
            raise NotImplementedError(
                EWMSJDL,
                "Number of parametric jobs exceeds the limit of %d"
                % MAX_PARAMETRIC_JOBS,
            )
        result = generateParametricJobs(jobClassAd)
        if not result["OK"]:
            return result
        jobDescList = result["Value"]
    else:
        # if we are here, then jobDesc was the description of a single job.
        jobDescList = [jobDesc]

    jobIDList = []

    if parametricJob:
        initialStatus = JobStatus.SUBMITTING
        initialMinorStatus = "Bulk transaction confirmation"
    else:
        initialStatus = JobStatus.RECEIVED
        initialMinorStatus = "Job accepted"

    for (
        jobDescription
    ) in (
        jobDescList
    ):  # jobDescList because there might be a list generated by a parametric job
        job_id = await job_db.insert(
            jobDescription,
            user_info.sub,
            fixme_ownerDN,
            fixme_ownerGroup,
            fixme_diracSetup,
            initialStatus,
            initialMinorStatus,
            user_info.vo,
        )

        logging.debug(
            f'Job added to the JobDB", "{job_id} for {fixme_ownerDN}/{fixme_ownerGroup}'
        )

        await job_logging_db.insert_record(
            int(job_id), initialStatus, initialMinorStatus, source="JobManager"
        )

        jobIDList.append(job_id)

    return jobIDList

    # TODO: is this needed ?
    # if not parametricJob:
    #     self.__sendJobsToOptimizationMind(jobIDList)
    # return result

    return await asyncio.gather(
        *(job_db.insert(j.owner, j.group, j.vo) for j in job_definitions)
    )


@router.delete("/")
async def delete_bulk_jobs(job_ids: Annotated[list[int], Query()]):
    return job_ids


@router.get("/{job_id}")
async def get_single_job(job_id: int):
    return f"This job {job_id}"


@router.delete("/{job_id}")
async def delete_single_job(job_id: int):
    return f"I am deleting {job_id}"


@router.post("/{job_id}/kill", dependencies=[has_properties(JOB_ADMINISTRATOR)])
async def kill_single_job(job_id: int):
    return f"I am killing {job_id}"


@router.get("/{job_id}/status")
async def get_single_job_status(job_id: int, job_db: JobDB) -> StatusModel:
    return await get_job_status(job_id, job_db)


@router.post("/{job_id}/status")
# TODO: use statusModel here ?
async def set_single_job_status(
    job_id: int, status: StatusModel, job_db: JobDB, job_logging_db: JobLoggingDB
):
    try:
        await set_job_status(job_id, status, job_db, job_logging_db, MagicMock())
    except ValueError as e:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e)) from e
    return f"Updating Job {job_id} to {status}"


@router.post("/{job_id}/statuses")
async def set_single_job_status_bulk(
    job_id: int,
    statuses: list[StatusModel],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
):
    try:
        await set_job_status_bulk(job_id, statuses, job_db, job_logging_db, MagicMock())
    except ValueError as e:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e)) from e
    return f"Updating Job {job_id} to {statuses}"  # TODO rework this (see current implementation)


@router.post("/kill")
async def kill_bulk_jobs(job_ids: Annotated[list[int], Query()]):
    return job_ids


@router.get("/status")
async def get_bulk_job_status(
    job_ids: Annotated[list[int], Query(max_items=10)], job_db: JobDB
) -> dict[int, JobStatus]:
    return get_bulk_job_status(job_ids, job_db)


@router.post("/status")
async def set_status_bulk(job_update: list[JobStatusUpdate]) -> list[JobStatusReturn]:
    return [{"job_id": job.job_id, "status": job.status} for job in job_update]


# @router.post("/statuses")
# async def set_multiple_job_status_bulk(
#     job_update: list[tuple[int, list[StatusModel]]],
#     job_db: JobDB,
#     job_logging_db: JobLoggingDB,
# ):
#     return await asyncio.gather(
#         *(
#             set_bulk_job_status(job_id, statuses, job_db, job_logging_db, MagicMock())
#             for job_id, statuses in job_update
#         )
#     )


EXAMPLE_SEARCHES = {
    "Show all": {
        "summary": "Show all",
        "description": "Shows all jobs the current user has access to.",
        "value": {},
    },
    "A specific job": {
        "summary": "A specific job",
        "description": "Search for a specific job by ID",
        "value": {"search": [{"parameter": "JobID", "operator": "eq", "value": "5"}]},
    },
    "Get ordered job statuses": {
        "summary": "Get ordered job statuses",
        "description": "Get only job statuses for specific jobs, ordered by status",
        "value": {
            "parameters": ["JobID", "Status"],
            "search": [
                {"parameter": "JobID", "operator": "in", "values": ["6", "2", "3"]}
            ],
            "sort": [{"parameter": "JobID", "direction": "asc"}],
        },
    },
}

EXAMPLE_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: {
        "description": "List of matching results",
        "content": {
            "application/json": {
                "example": [
                    {
                        "JobID": 1,
                        "JobGroup": "jobGroup",
                        "Owner": "myvo:my_nickname",
                        "SubmissionTime": "2023-05-25T07:03:35.602654",
                        "LastUpdateTime": "2023-05-25T07:03:35.602652",
                        "Status": "RECEIVED",
                        "MinorStatus": "Job accepted",
                        "ApplicationStatus": "Unknown",
                    },
                    {
                        "JobID": 2,
                        "JobGroup": "my_nickname",
                        "Owner": "myvo:cburr",
                        "SubmissionTime": "2023-05-25T07:03:36.256378",
                        "LastUpdateTime": "2023-05-25T07:10:11.974324",
                        "Status": "Done",
                        "MinorStatus": "Application Exited Successfully",
                        "ApplicationStatus": "All events processed",
                    },
                ]
            }
        },
    },
}


@router.post("/search", responses=EXAMPLE_RESPONSES)
async def search(
    config: Annotated[Config, Depends(ConfigSource.create)],
    job_db: JobDB,
    user_info: Annotated[UserInfo, Depends(verify_dirac_token)],
    page: int = 0,
    per_page: int = 100,
    body: Annotated[JobSearchParams | None, Body(examples=EXAMPLE_SEARCHES)] = None,
) -> list[dict[str, Any]]:
    """Retrieve information about jobs.

    **TODO: Add more docs**
    """
    if body is None:
        body = JobSearchParams()
    # TODO: Apply all the job policy stuff properly using user_info
    if not config.Operations["Defaults"].Services.JobMonitoring.GlobalJobsInfo:
        body.search.append(
            {
                "parameter": "Owner",
                "operator": ScalarSearchOperator.EQUAL,
                "value": user_info.sub,
            }
        )
    # TODO: Pagination
    return await job_db.search(
        body.parameters, body.search, body.sort, page=page, per_page=per_page
    )


@router.post("/summary")
async def summary(
    config: Annotated[Config, Depends(ConfigSource.create)],
    job_db: JobDB,
    user_info: Annotated[UserInfo, Depends(verify_dirac_token)],
    body: JobSummaryParams,
):
    """Show information suitable for plotting"""
    # TODO: Apply all the job policy stuff properly using user_info
    if not config.Operations["Defaults"].Services.JobMonitoring.GlobalJobsInfo:
        body.search.append(
            {
                "parameter": "Owner",
                "operator": ScalarSearchOperator.EQUAL,
                "value": user_info.sub,
            }
        )
    return await job_db.summary(body.grouping, body.search)
