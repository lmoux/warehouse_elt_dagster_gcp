import os

import src.dw01.pjm


def correct_base_path(local_sample_file="sample_payloads/pjm.ftr.latest.html"):
    # I couldn't get coverage to handle paths correctly via uv as I did with pytest,
    # ergo this pseudo solution. With it, I am able to get pycharm to re-run tests automatically!

    if os.getcwd().endswith("tests"):
        return local_sample_file
    else:
        return f"tests/{local_sample_file}"


def test_url_extractions_unhappy_cases():
    def assert_here(prospect):
        assert prospect is not None and prospect == []

    assert_here(src.dw01.pjm.get_urls_all_schedule_files(None))
    assert_here(src.dw01.pjm.get_urls_all_model_update_files(None))
    assert_here(src.dw01.pjm.get_urls_all_schedule_files(os.__file__))
    assert_here(src.dw01.pjm.get_urls_all_model_update_files(os.__file__))

    with open(os.__file__, "r") as file:
        assert_here(src.dw01.pjm.get_urls_all_schedule_files(file))
        assert_here(src.dw01.pjm.get_urls_all_model_update_files(file))


def test_url_extraction_of_schedules():
    with open(correct_base_path()) as file:
        results = src.dw01.pjm.get_urls_all_schedule_files(file)
        assert len(results) == 9
        should_include_these_urls = {
            "https://www.pjm.com/-/media/DotCom/markets-ops/ftr/ftr-allocation/ftr-market-schedule/2025-ftr-arr-market-schedule.xlsx",
            "https://www.pjm.com/-/media/DotCom/markets-ops/ftr/ftr-allocation/ftr-market-schedule/2024-ftr-arr-market-schedule.xlsx",
            "https://www.pjm.com/-/media/DotCom/markets-ops/ftr/ftr-allocation/ftr-market-schedule/2022-ftr-arr-market-schedule.xls",
        }
        found_these = set(map(lambda x: x.url, results))
        assert should_include_these_urls.issubset(found_these)
        assert len(should_include_these_urls.difference(found_these)) == 0


def test_url_extraction_of_model_changes():
    with open(correct_base_path()) as file:
        results = src.dw01.pjm.get_urls_all_model_update_files(file)
        assert len(results) == 35

        should_include_these_urls = {  # sample taken directly from the browser via right-click & copy-link
            "https://www.pjm.com/-/media/DotCom/markets-ops/ftr/ftr-model-updates/ftr-model-update-06-11-25.csv",
            "https://www.pjm.com/-/media/DotCom/markets-ops/ftr/ftr-model-updates/ftr-model-update-03-12-25.csv",
            "https://www.pjm.com/-/media/DotCom/markets-ops/ftr/ftr-model-updates/ftr-model-update-09-14-16.csv",
            "https://www.pjm.com/-/media/DotCom/markets-ops/ftr/ftr-model-updates/ftr-model-update-05-28-20.csv",
            "https://www.pjm.com/-/media/DotCom/markets-ops/ftr/ftr-model-updates/ftr-model-update-06-01-21.csv",
        }
        found_these = set(map(lambda x: x.url, results))

        assert any(map(lambda x: x.url in should_include_these_urls, results))
        assert len(should_include_these_urls.difference(found_these)) == 0
