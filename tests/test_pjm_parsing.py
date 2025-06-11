from datetime import date

from src.dw01 import pjm
from .test_url_extraction import correct_base_path


def test_pjm_model_change_parsing():
    result = pjm.PjmFtrModelUpdateFile(
        correct_base_path("sample_payloads/ftr-model-update-03-12-25.csv")
    )
    assert result is not None
    assert result.version == date(2025, 3, 12)
    assert result.changes["from_id"].count() == 10


def test_pjm_schedule_parsing():
    result = pjm.PjmFtrScheduleFile(
        correct_base_path("sample_payloads/2025-ftr-arr-market-schedule.xlsx")
    )
    assert result is not None
    assert len(result.auctions) == 101
