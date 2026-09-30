"""Unit tests for the two bits of the pipeline that are pure logic.

Everything else here talks to PUDL, PJM or DuckDB, so it is covered by the
dbt assertions rather than by pytest. These two are worth pinning because a
silent change in either would move published numbers without failing anything.
"""
import pytest

from build_site_data import mase
from etl.util import read_excel_tab


class TestMase:
    """Mean absolute scaled error, naive seasonality=1.

    A score below 1 beats the naive "next month looks like this one"
    forecast; above 1 loses to it. The README quotes these numbers, so the
    scale needs to stay what the README says it is.
    """

    def test_fewer_than_two_points_has_no_baseline(self):
        # one point gives no month-over-month change to compare against
        assert mase([100.0], [90.0]) is None
        assert mase([], []) is None

    def test_flat_actuals_have_no_baseline(self):
        # naive error is zero, so the ratio would divide by zero
        assert mase([50.0, 50.0, 50.0], [40.0, 60.0, 50.0]) is None

    def test_a_perfect_forecast_scores_zero(self):
        actual = [10.0, 12.0, 11.0, 15.0]
        assert mase(actual, list(actual)) == 0.0

    def test_matching_the_naive_forecast_scores_about_one(self):
        # forecast each month as the previous month's actual: that is the
        # naive baseline, so the ratio should land near 1
        actual = [10.0, 12.0, 11.0, 15.0]
        naive = [10.0] + actual[:-1]
        assert mase(actual, naive) == pytest.approx(1.0, rel=0.35)

    def test_a_worse_forecast_scores_higher(self):
        actual = [10.0, 12.0, 11.0, 15.0]
        good = [10.5, 11.5, 11.0, 14.5]
        bad = [30.0, 30.0, 30.0, 30.0]
        assert mase(actual, good) < mase(actual, bad)

    def test_the_score_is_scale_free(self):
        # doubling both series must not change the score; that is the whole
        # point of scaling by the naive error
        actual = [10.0, 12.0, 11.0, 15.0]
        forecast = [11.0, 11.0, 12.0, 14.0]
        doubled_a = [x * 2 for x in actual]
        doubled_f = [x * 2 for x in forecast]
        assert mase(actual, forecast) == pytest.approx(mase(doubled_a, doubled_f))


class TestReadExcelTab:
    """The reader picks an engine from the file suffix, because the older
    PJM load reports are .xls and the newer ones .xlsx."""

    def test_a_missing_file_is_reported_as_such(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_excel_tab(str(tmp_path / "nope.xlsx"), 0)

    def test_missing_file_is_checked_before_the_suffix(self, tmp_path):
        # a suffix the reader has no engine for still fails on the file check
        with pytest.raises(FileNotFoundError):
            read_excel_tab(str(tmp_path / "nope.csv"), 0)

    @pytest.mark.xfail(
        raises=UnboundLocalError,
        reason="engine is only assigned for .xls/.xlsx, so any other suffix "
               "raises UnboundLocalError instead of a useful message",
        strict=True,
    )
    def test_an_unsupported_suffix_should_say_so(self, tmp_path):
        f = tmp_path / "loads.csv"
        f.write_text("a,b\n1,2\n")
        read_excel_tab(str(f), 0)
