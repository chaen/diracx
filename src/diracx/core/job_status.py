import asyncio
from datetime import datetime

from diracx.core.models import StatusModel
from diracx.core.utils import JobStatus
from diracx.db.jobs.db import JobDB, JobLoggingDB


async def get_job_status(job_id: int, job_db: JobDB) -> StatusModel:
    """
    Get the status of a job specified by its jobId
    Although this method might seen useless, I created it in case we need to
    we wanted to move from getting the status from the JobDB to the JobLoggingDB
    or to the ElasticJobParametersDB (or something else entirely if we decide to
    use a time-series database in the future)
    """
    result = await job_db.get_job_status(job_id)
    return result


async def get_bulk_job_status(
    job_ids: list[int], job_db: JobDB
) -> dict[int, tuple[JobStatus, str, str]]:
    """
    Get the status of a job specified by its jobId
    Although this method might seen useless, I created it in case we need to
    we wanted to move from getting the status from the JobDB to the JobLoggingDB
    or to the ElasticJobParametersDB (or something else entirely)
    """
    result = await asyncio.gather(job_db.get_job_status(job_id) for job_id in job_ids)
    # Transform the list of dicts into a dict of tuples
    result = {
        job_id: (res["Status"], res["MinorStatus"], res["ApplicationStatus"])
        for job_id, res in zip(job_ids, result)
    }
    return result


async def set_job_status(
    job_id: int,
    status: StatusModel,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    elasticJobParametersDB,
    update_time: datetime = datetime.utcnow(),
    force: bool = False,
):
    """Set various status fields for job specified by its jobId.
    Set only the last status in the JobDB, updating all the status
    logging information in the JobLoggingDB. The statusDict has dateTime
    as a key and status information dictionary as values
    """

    pass


async def set_job_status_bulk(
    job_id: int,
    statuses: list[StatusModel],
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    elasticJobParametersDB,
    update_time: datetime = datetime.utcnow(),
    force: bool = False,
):
    pass


#     from DIRAC.WorkloadManagementSystem.Client.JobStatus import JOB_FINAL_STATES, JobsStateMachine

#     result = await job_db.get_job_status(job_id)

#     # If the current status is Stalled and we get an update, it should probably be "Running"
#     currentStatus = result["Status"]
#     if currentStatus == JobStatus.STALLED:
#         currentStatus = JobStatus.RUNNING

#     # Get chronological order of new updates
#     updateTimes = sorted(statusDict)
#     log.debug(
#         "*** New call ***",
#         f"Last update time {lastTime} - Sorted new times {updateTimes}",
#     )
#     # Get the status (if any) at the time of the first update
#     newStat = ""
#     firstUpdate = TimeUtilities.toEpoch(TimeUtilities.fromString(updateTimes[0]))
#     for ts, st in timeStamps:
#         if firstUpdate >= ts:
#             newStat = st

#     # Update the status in the JobDB only if the new status is more recent than the last one
#     if update_time >= await job_logging_db.get_latest_time_stamp(job_id):
#         if not force and status != currentStatus:
#             res = JobsStateMachine(currentStatus).getNextState(status)
#             if not res["OK"]:
#                 return res
#             newStat = res["Value"]
#             # If the JobsStateMachine does not accept the candidate, don't update
#             if newStat != status:
#                 raise InvalidStatusTransition(
#                   f"Job {job_id} can't move from {currentStatus} to {status}: using {newStat}"
#                )

#                 )
#                 # keeping the same status
#                 log.error(
#                     "Job Status Error",
#                     f"{job_id} can't move from {currentStatus} to {status}: using {newStat}",
#                 )
#                 status = newStat
#                 statusDict[updateTimes[0]]["Status"] = newStat
#                 # Change the source to indicate this is not what was requested
#                 source = "JobsStateMachine"
#         await job_db.set_job_attributes(
#             job_id,
#             {"Status": status, "MinorStatus": minor_status, "ApplicationStatus": application_status},
#         )

#     # Pick up the start date when the job starts running if not existing
#     if status == JobStatus.RUNNING and await job_db.get_start_exec_time(job_id) is None:
#         await job_db.set_start_exec_time(job_id, update_time)

#     # Pick up the end date when the job is in a final status
#     if status in JOB_FINAL_STATES and await job_db.get_end_exec_time(job_id) is None:
#         await job_db.set_end_exec_time(job_id, update_time)

#     # We should only update the status to the last one if its time stamp is more recent than the last update
#     attrNames = []
#     attrValues = []
#     if updateTimes[-1] >= lastTime:
#         minor = ""
#         application = ""
#         # Get the last status values looping on the most recent upupdateTimes in chronological order
#         for updTime in [dt for dt in updateTimes if dt >= lastTime]:
#             sDict = statusDict[updTime]
#             log.debug("\t", f"Time {updTime} - Statuses {str(sDict)}")
#             status = sDict.get("Status", currentStatus)
#             # evaluate the state machine if the status is changing
#             if not force and status != currentStatus:
#                 res = JobStatus.JobsStateMachine(currentStatus).getNextState(status)
#                 if not res["OK"]:
#                     return res
#                 newStat = res["Value"]
#                 # If the JobsStateMachine does not accept the candidate, don't update
#                 if newStat != status:
#                     # keeping the same status
#                     log.error(
#                         "Job Status Error",
#                         f"{job_id} can't move from {currentStatus} to {status}: using {newStat}",
#                     )
#                     status = newStat
#                     sDict["Status"] = newStat
#                     # Change the source to indicate this is not what was requested
#                     source = sDict.get("Source", "")
#                     sDict["Source"] = source + "(SM)"
#                 # at this stage status == newStat. Set currentStatus to this new status
#                 currentStatus = newStat

#             minor = sDict.get("MinorStatus", minor)
#             application = sDict.get("ApplicationStatus", application)

#         log.debug(
#             "Final statuses:",
#             f"status '{status}', minor '{minor}', application '{application}'",
#         )
#         if status:
#             attrNames.append("Status")
#             attrValues.append(status)
#         if minor:
#             attrNames.append("MinorStatus")
#             attrValues.append(minor)
#         if application:
#             attrNames.append("ApplicationStatus")
#             attrValues.append(application)
#         # Here we are forcing the update as it's always updating to the last status

#         result = job_db.setJobAttributes(
#             job_id, attrNames, attrValues, update=True, force=True
#         )
#         if not result["OK"]:
#             return result
#         if self.elasticJobParametersDB:
#             result = self.elasticJobParametersDB.setJobParameter(
#                 int(job_id), "Status", status
#             )
#             if not result["OK"]:
#                 return result

#     # Update start and end time if needed

#     # Update the JobLoggingDB records
#     heartBeatTime = None
#     for updTime in updateTimes:
#         sDict = statusDict[updTime]
#         status = sDict.get("Status", "idem")
#         minor = sDict.get("MinorStatus", "idem")
#         application = sDict.get("ApplicationStatus", "idem")
#         source = sDict.get("Source", "Unknown")
#         result = job_logging_db.insert_record(
#             job_id,
#             status=status,
#             minorStatus=minor,
#             applicationStatus=application,
#             date=updTime,
#             source=source,
#         )
#         if not result["OK"]:
#             return result
#         # If the update comes from a job, update the heart beat time stamp with this item's stamp
#         if source.startswith("Job"):
#             heartBeatTime = updTime

#     if heartBeatTime is not None:
#         await job_db.set_heartbeat_time(job_id, heartBeatTime)

#     return S_OK((attrNames, attrValues))
