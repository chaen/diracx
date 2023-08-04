from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import delete, func, insert, select, update

from diracx.core.exceptions import InvalidQueryError
from diracx.core.utils import JobStatus

from ..utils import BaseDB
from .schema import InputData, JobDBBase, JobJDLs, JobLoggingDBBase, Jobs, LoggingInfo


def apply_search_filters(table, stmt, search):
    # Apply any filters
    for query in search:
        column = table.columns[query["parameter"]]
        if query["operator"] == "eq":
            expr = column == query["value"]
        elif query["operator"] == "neq":
            expr = column != query["value"]
        elif query["operator"] == "gt":
            expr = column > query["value"]
        elif query["operator"] == "lt":
            expr = column < query["value"]
        elif query["operator"] == "in":
            expr = column.in_(query["values"])
        elif query["operator"] in "like":
            expr = column.like(query["values"])
        else:
            raise InvalidQueryError(f"Unknown filter {query=}")
        stmt = stmt.where(expr)
    return stmt


class JobDB(BaseDB):
    # This needs to be here for the BaseDB to create the engine
    metadata = JobDBBase.metadata

    # TODO: this is copied from the DIRAC JobDB
    # but is overwriten in LHCbDIRAC, so we need
    # to find a way to make it dynamic
    jdl2DBParameters = ["JobName", "JobType", "JobGroup"]

    async def summary(self, group_by, search) -> list[dict[str, str | int]]:
        columns = [Jobs.__table__.columns[x] for x in group_by]

        stmt = select(*columns, func.count(Jobs.JobID).label("count"))
        stmt = apply_search_filters(Jobs.__table__, stmt, search)
        stmt = stmt.group_by(*columns)

        # Execute the query
        return [
            dict(row._mapping)
            async for row in (await self.conn.stream(stmt))
            if row.count > 0  # type: ignore
        ]

    async def search(
        self, parameters, search, sorts, *, per_page: int = 100, page: int | None = None
    ) -> list[dict[str, Any]]:
        # Find which columns to select
        columns = [x for x in Jobs.__table__.columns]
        if parameters:
            if unrecognised_parameters := set(parameters) - set(
                Jobs.__table__.columns.keys()
            ):
                raise InvalidQueryError(
                    f"Unrecognised parameters requested {unrecognised_parameters}"
                )
            columns = [c for c in columns if c.name in parameters]
        stmt = select(*columns)

        stmt = apply_search_filters(Jobs.__table__, stmt, search)

        # Apply any sort constraints
        for sort in sorts:
            column = Jobs.__table__.columns[sort["parameter"]]
            if sort["direction"] == "asc":
                column = column.asc()
            elif sort["direction"] == "desc":
                column = column.desc()
            else:
                raise InvalidQueryError(f"Unknown sort {sort['direction']=}")

        # Apply pagination
        if page:
            raise NotImplementedError("TODO Not yet implemented")

        # Execute the query
        return [dict(row._mapping) async for row in (await self.conn.stream(stmt))]

    async def _insertNewJDL(self, jdl) -> int:
        from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import compressJDL

        stmt = insert(JobJDLs).values(
            JDL="", JobRequirements="", OriginalJDL=compressJDL(jdl)
        )
        result = await self.conn.execute(stmt)
        # await self.engine.commit()
        return result.lastrowid

    async def _insertJob(self, jobData: dict[str, Any]):
        stmt = insert(Jobs).values(jobData)
        await self.conn.execute(stmt)

    async def _insertInputData(self, job_id: int, lfns: list[str]):
        stmt = insert(InputData).values([{"JobID": job_id, "LFN": lfn} for lfn in lfns])
        await self.conn.execute(stmt)

    async def setJobAttributes(self, job_id, jobData):
        """
        TODO: add myDate and force parameters
        """
        if "Status" in jobData:
            jobData = jobData | {"LastUpdateTime": datetime.now(tz=timezone.utc)}
        stmt = update(Jobs).where(Jobs.JobID == job_id).values(jobData)
        await self.conn.execute(stmt)

    async def _checkAndPrepareJob(
        self,
        jobID,
        class_ad_job,
        class_ad_req,
        owner,
        owner_dn,
        owner_group,
        dirac_setup,
        job_attrs,
        vo,
    ):
        """
        Check Consistency of Submitted JDL and set some defaults
        Prepare subJDL with Job Requirements
        """
        from DIRAC.Core.Utilities.DErrno import EWMSSUBM, cmpError
        from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
        from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import (
            checkAndPrepareJob,
        )

        retVal = checkAndPrepareJob(
            jobID,
            class_ad_job,
            class_ad_req,
            owner,
            owner_dn,
            owner_group,
            dirac_setup,
            job_attrs,
            vo,
        )

        if not retVal["OK"]:
            if cmpError(retVal, EWMSSUBM):
                await self.setJobAttributes(jobID, job_attrs)

            returnValueOrRaise(retVal)

    async def setJobJDL(self, job_id, jdl):
        from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import compressJDL

        stmt = (
            update(JobJDLs).where(JobJDLs.JobID == job_id).values(JDL=compressJDL(jdl))
        )
        await self.conn.execute(stmt)

    async def insert(
        self,
        jdl,
        owner,
        owner_dn,
        owner_group,
        dirac_setup,
        initial_status,
        initial_minor_status,
        vo,
    ):
        from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
        from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
        from DIRAC.WorkloadManagementSystem.DB.JobDBUtils import (
            checkAndAddOwner,
            createJDLWithInitialStatus,
            fixJDL,
        )

        job_attrs = {
            "LastUpdateTime": datetime.now(tz=timezone.utc),
            "SubmissionTime": datetime.now(tz=timezone.utc),
            "Owner": owner,
            "OwnerDN": owner_dn,
            "OwnerGroup": owner_group,
            "DIRACSetup": dirac_setup,
        }

        jobManifest = returnValueOrRaise(
            checkAndAddOwner(jdl, owner, owner_dn, owner_group, dirac_setup)
        )

        jdl = fixJDL(jdl)

        job_id = await self._insertNewJDL(jdl)

        jobManifest.setOption("JobID", job_id)

        job_attrs["JobID"] = job_id

        # 2.- Check JDL and Prepare DIRAC JDL
        jobJDL = jobManifest.dumpAsJDL()

        # Replace the JobID placeholder if any
        if jobJDL.find("%j") != -1:
            jobJDL = jobJDL.replace("%j", str(job_id))

        class_ad_job = ClassAd(jobJDL)
        class_ad_req = ClassAd("[]")
        if not class_ad_job.isOK():
            job_attrs["Status"] = JobStatus.Failed

            job_attrs["MinorStatus"] = "Error in JDL syntax"

            await self._insertJob(job_attrs)

            return {
                "JobID": job_id,
                "Status": JobStatus.FAILED,
                "MinorStatus": "Error in JDL syntax",
            }

        class_ad_job.insertAttributeInt("JobID", job_id)

        await self._checkAndPrepareJob(
            job_id,
            class_ad_job,
            class_ad_req,
            owner,
            owner_dn,
            owner_group,
            dirac_setup,
            job_attrs,
            vo,
        )

        jobJDL = createJDLWithInitialStatus(
            class_ad_job,
            class_ad_req,
            self.jdl2DBParameters,
            job_attrs,
            initial_status,
            initial_minor_status,
            modern=True,
        )

        await self.setJobJDL(job_id, jobJDL)

        # Adding the job in the Jobs table
        await self._insertJob(job_attrs)

        # TODO: check if that is actually true
        if class_ad_job.lookupAttribute("Parameters"):
            raise NotImplementedError("Parameters in the JDL are not supported")

        # Looking for the Input Data
        inputData = []
        if class_ad_job.lookupAttribute("InputData"):
            inputData = class_ad_job.getListFromExpression("InputData")
            lfns = [lfn for lfn in inputData if lfn]
            if lfns:
                await self._insertInputData(job_id, lfns)

        return {
            "JobID": job_id,
            "Status": initial_status,
            "MinorStatus": initial_minor_status,
            "TimeStamp": datetime.now(tz=timezone.utc),
        }

    async def get_start_exec_time(self, job_id: int) -> datetime | None:
        """
        Get the start execution time of a job
        raises: ValueError if no row was found for the jobID
        """
        stmt = select(Jobs.StartExecTime).where(Jobs.JobID == job_id)
        result = await self.conn.execute(stmt)
        return result.one()[0]

    async def set_start_exec_time(self, job_id: int, start_time: datetime):
        """
        Set the start execution time of a job
        :raises ValueError: if no row was updated, meaning JobID is not valid or the job already started
        """
        stmt = (
            update(Jobs)
            .where(Jobs.JobID == job_id)
            .where(Jobs.StartExecTime.is_(None))
            .values(StartExecTime=start_time, HeartBeatTime=start_time)
        )
        result = await self.conn.execute(stmt)

        if result.rowcount == 0:
            raise ValueError(f"No row updated for jobID: {job_id}")

    async def get_end_exec_time(self, job_id: int) -> datetime | None:
        """
        Get the end execution time of a job
        raises: NoResultFound if no row was found for the jobID
        """
        stmt = select(Jobs.EndExecTime).where(Jobs.JobID == job_id)
        result = await self.conn.execute(stmt)
        return result.one()[0]

    async def set_end_exec_time(self, jobID: int, end_time: datetime):
        """
        Set the end execution time of a job
        raises: ValueError if no row was updated, meaning JobID is not valid or the job already ended
        """
        stmt = (
            update(Jobs)
            .where(Jobs.JobID == jobID)
            .where(Jobs.EndExecTime.is_(None))
            .values(EndExecTime=end_time)
        )
        result = await self.conn.execute(stmt)

        if result.rowcount == 0:
            raise ValueError(f"No row updated for jobID: {jobID}")

    async def get_heartbeat_time(self, job_id: int) -> datetime | None:
        """Get the heartbeat time of a job"""
        stmt = select(Jobs.HeartBeatTime).where(Jobs.JobID == job_id)
        result = await self.conn.execute(stmt)
        return result.one()[0]

    async def set_heartbeat_time(self, job_id: int, timestamp: datetime):
        """
        Set the heartbeat time of a job
        raises: ValueError if no row was updated, meaning JobID is not valid
        """
        stmt = update(Jobs).where(Jobs.JobID == job_id).values(HeartBeatTime=timestamp)
        result = await self.conn.execute(stmt)

        if result.rowcount == 0:
            raise ValueError(f"No row updated for jobID: {job_id}")

    async def get_job_status(self, job_id: int):
        stmt = select(Jobs.Status, Jobs.MinorStatus, Jobs.ApplicationStatus).where(
            Jobs.JobID == job_id
        )
        result = await self.conn.execute(stmt)
        return result.one()

    async def set_job_status(
        self, job_id: int, job_status: JobStatus, minor_status: str, app_status: str
    ):
        stmt = (
            update(Jobs)
            .where(Jobs.JobID == job_id)
            .values(
                Status=job_status,
                MinorStatus=minor_status,
                ApplicationStatus=app_status,
            )
        )
        result = await self.conn.execute(stmt)

        if result.rowcount == 0:
            raise ValueError(f"No row updated for jobID: {job_id}")


MAGIC_EPOC_NUMBER = 1270000000


class JobLoggingDB(BaseDB):
    """Frontend for the JobLoggingDB. Provides the ability to store changes with timestamps"""

    # This needs to be here for the BaseDB to create the engine
    metadata = JobLoggingDBBase.metadata

    async def insert_record(
        self,
        jobID: int,
        status: str = "idem",
        minorStatus: str = "idem",
        applicationStatus: str = "idem",
        date: datetime | None = None,
        source: str = "Unknown",
    ):
        """
        Add a new entry to the JobLoggingDB table. One, two or all the three status
        components (status, minorStatus, applicationStatus) can be specified.
        Optionally the time stamp of the status can
        be provided in a form of a string in a format '%Y-%m-%d %H:%M:%S' or
        as datetime.datetime object. If the time stamp is not provided the current
        UTC time is used.
        """

        if not date:
            date = datetime.utcnow()

        seqnum_stmt = (
            select(func.coalesce(func.max(LoggingInfo.SeqNum) + 1, 1))
            .where(LoggingInfo.JobID == jobID)
            .scalar_subquery()
        )

        epoc = (
            time.mktime(date.timetuple())
            + date.microsecond / 1000000.0
            - MAGIC_EPOC_NUMBER
        )

        stmt = insert(LoggingInfo).values(
            JobID=int(jobID),
            SeqNum=seqnum_stmt,
            Status=status,
            MinorStatus=minorStatus,
            ApplicationStatus=applicationStatus[:255],
            StatusTime=date,
            StatusTimeOrder=epoc,
            StatusSource=source[:32],
        )
        await self.conn.execute(stmt)

    async def get_records(self, job_id: int) -> list[tuple]:
        """Returns a Status,MinorStatus,ApplicationStatus,StatusTime,StatusSource tuple
        for each record found for job specified by its jobID in historical order
        """

        stmt = (
            select(
                LoggingInfo.Status,
                LoggingInfo.MinorStatus,
                LoggingInfo.ApplicationStatus,
                LoggingInfo.StatusTime,
                LoggingInfo.StatusSource,
            )
            .where(LoggingInfo.JobID == int(job_id))
            .order_by(LoggingInfo.StatusTimeOrder, LoggingInfo.StatusTime)
        )
        rows = await self.conn.execute(stmt)

        values = []
        for (
            status,
            minor_status,
            application_status,
            status_time,
            status_source,
        ) in rows:
            values.append(
                [status, minor_status, application_status, status_time, status_source]
            )

        # assert rows == values # TODO: check if this assumption is correct

        # If no value has been set for the application status in the first place,
        # We put this status to unknown
        res = []
        if values:
            if values[0][2] == "idem":
                values[0][2] = "Unknown"

            # We replace "idem" values by the value previously stated
            for i in range(1, len(values)):
                for j in range(3):
                    if values[i][j] == "idem":
                        values[i][j] = values[i - 1][j]

            # And we replace arrays with tuples
            for row in values:
                res.append(tuple(row))

        return res

    async def delete_records(self, job_ids: list[int]):
        """Delete logging records for given jobs"""

        stmt = delete(LoggingInfo).where(LoggingInfo.JobID.in_(job_ids))
        await self.conn.execute(stmt)

    async def get_time_stamps(self, job_id: int) -> dict[str, str]:
        """Get TimeStamps for job MajorState transitions
        :return: a {State:timestamp} dictionary
        """

        stmt = (
            select(LoggingInfo.Status, LoggingInfo.StatusTimeOrder)
            .where(LoggingInfo.JobID == job_id)
            .order_by(LoggingInfo.StatusTimeOrder.asc())
        )
        rows = await self.conn.execute(stmt)

        result = {}
        for event, etime in rows:
            result[str(event)] = str(etime + MAGIC_EPOC_NUMBER)

        return result

    async def get_latest_time_stamp(self, job_id: int) -> datetime:
        """
        :raises: ValueError if no time stamp has been found (which is most probably due to a bad job_id value)
        :return: a {jobID: timestamp} dictionary
        """
        # Get last date and time
        stmt = (
            select(LoggingInfo.StatusTime)
            .where(LoggingInfo.JobID == job_id)
            .order_by(LoggingInfo.StatusTimeOrder.desc())
            .limit(1)
        )
        rows = await self.conn.execute(stmt)
        row = rows.first()
        # This can only happend when the jobID provided is not valid
        if not row:
            raise ValueError

        return row[0]
