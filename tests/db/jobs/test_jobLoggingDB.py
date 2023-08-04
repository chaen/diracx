from datetime import datetime

import pytest

from diracx.db import JobLoggingDB


@pytest.fixture
async def job_logging_db():
    job_logging_db = JobLoggingDB("sqlite+aiosqlite:///:memory:")
    async with job_logging_db.engine_context():
        yield job_logging_db


async def test_JobStatus(job_logging_db: JobLoggingDB):
    async with job_logging_db as job_logging_db:
        await job_logging_db.insert_record(
            1,
            status="testing",
            minorStatus="date=datetime.utcnow()",
            date=datetime.utcnow(),
            source="Unittest",
        )

        await job_logging_db.insert_record(
            1,
            status="testing",
            minorStatus="2006-04-25 14:20:17",
            date=datetime(2006, 4, 25, 14, 20, 17),
            source="Unittest",
        )

        await job_logging_db.insert_record(
            1, status="testing2", minorStatus="No date", source="Unittest"
        )

        await job_logging_db.insert_record(
            1, status="testing2", minorStatus="No date 2", source="Unittest"
        )

        res = await job_logging_db.get_records(1)
        assert res
        assert len(res) == 4
        assert type(res[0]) is tuple

        res = await job_logging_db.get_time_stamps(1)
        assert "testing", "testing2" in res
        assert len(res) == 2
        assert type(res) is dict

        res = await job_logging_db.get_latest_time_stamp(1)
        assert type(res) is datetime

        await job_logging_db.delete_records([1])

        res = await job_logging_db.get_records(1)
        assert len(res) == 0

        res = await job_logging_db.get_time_stamps(1)
        assert len(res) == 0

        with pytest.raises(ValueError):
            await job_logging_db.get_latest_time_stamp(1)
