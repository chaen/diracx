from __future__ import annotations

import asyncio
from datetime import datetime

import pytest
from sqlalchemy.exc import NoResultFound, StatementError

from diracx.db.jobs.db import JobDB


@pytest.fixture
async def job_db(tmp_path):
    job_db = JobDB("sqlite+aiosqlite:///:memory:")
    async with job_db.engine_context():
        yield job_db


async def test_some_asyncio_code(job_db):
    async with job_db as job_db:
        result = await job_db.search(["JobID"], [], [])
        assert not result

        result = await asyncio.gather(
            *(
                job_db.insert(
                    f"JDL{i}",
                    "owner",
                    "owner_dn",
                    "owner_group",
                    "diracSetup",
                    "New",
                    "dfdfds",
                    "lhcb",
                )
                for i in range(100)
            )
        )

    async with job_db as job_db:
        result = await job_db.search(["JobID"], [], [])
        assert result


async def test_start_exec_time(job_db: JobDB):
    async with job_db as job_db:
        result = await job_db.insert(
            "JDL",
            "owner",
            "owner_dn",
            "owner_group",
            "diracSetup",
            "New",
            "dfdfds",
            "lhcb",
        )
        job_id = result["JobID"]

        res = await job_db.get_start_exec_time(job_id)
        assert res is None

        first_datetime = datetime.utcnow()
        await job_db.set_start_exec_time(job_id, first_datetime)
        res = await job_db.get_start_exec_time(job_id)
        assert res == first_datetime

        with pytest.raises(ValueError):
            await job_db.set_start_exec_time(job_id, datetime.utcnow())

        res = await job_db.get_start_exec_time(job_id)
        assert res == first_datetime


async def test_get_start_exec_time_raises_NoResultFound_when_job_id_is_incorrect(
    job_db: JobDB,
):
    async with job_db as job_db:
        with pytest.raises(NoResultFound):
            await job_db.get_start_exec_time(-1)


async def test_set_start_exec_time_raises_ValueError_when_job_id_is_incorrect(
    job_db: JobDB,
):
    async with job_db as job_db:
        with pytest.raises(ValueError):
            await job_db.set_start_exec_time(-1, datetime.utcnow())


async def test_set_start_exec_time_raises_StatementError_when_job_id_is_incorrect(
    job_db: JobDB,
):
    async with job_db as job_db:
        with pytest.raises(StatementError):
            result = await job_db.insert(
                "JDL",
                "owner",
                "owner_dn",
                "owner_group",
                "diracSetup",
                "New",
                "dfdfds",
                "lhcb",
            )
            job_id = result["JobID"]
            result = await job_db.set_start_exec_time(job_id, "not a datetime")
