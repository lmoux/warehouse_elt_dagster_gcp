from datetime import date

from src.dw01 import pjm
from .test_url_extraction import correct_base_path


class PjmFtrModelUpdateParseCase:
    def __init__(self, filename: str, count: int, version: date):
        self.filename = filename
        self.count = count
        self.version = version


def test_pjm_model_change_parsing():
    test_bed = [
        PjmFtrModelUpdateParseCase(
            "sample_payloads/ftr-model-update-03-12-25.csv", 10, date(2025, 3, 12)
        ),
        PjmFtrModelUpdateParseCase(
            "sample_payloads/ftr-model-update-12-01-18.csv", 3, date(2018, 12, 1)
        ),
        PjmFtrModelUpdateParseCase(
            "sample_payloads/ftr-model-update-05-24-17.csv", 22, date(2017, 5, 24)
        ),
        PjmFtrModelUpdateParseCase(  # a case where we only have aggregates and no buses
            "sample_payloads/ftr-model-update-10-01-20.csv", 1, date(2020, 10, 1)
        ),
    ]

    for test in test_bed:
        result = pjm.PjmFtrModelUpdateFile(
            correct_base_path(test.filename),
        )
        assert result is not None
        assert result.version == test.version
        assert result.changes["from_id"].count() == test.count


def test_pjm_schedule_parsing():
    result = pjm.PjmFtrScheduleFile(
        correct_base_path("sample_payloads/2025-ftr-arr-market-schedule.xlsx")
    )
    assert result is not None
    assert len(result.auctions) == 101
