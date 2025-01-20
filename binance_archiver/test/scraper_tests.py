import pandas as pd

from binance_archiver.scraper import IndividualColumnChecker


class TestScraper:

    def test(self):
        ...

class TestIndividualColumnChecker:

    def test_given_only_one_unique_value_in_pandas_series_when_is_there_only_one_unique_value_in_series_check_then_true_is_being_returned(self):
        series = pd.Series(
            [
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms'
            ]
        )

        result_of_check = IndividualColumnChecker.is_there_only_one_unique_value_in_series(series=series)
        assert result_of_check == True

    def test_given_more_than_one_unique_value_in_pandas_series_when_check_if_is_there_only_one_unique_value_in_series_check_then_false_is_being_returned(self):
        series = pd.Series(
            [
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'adausdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms'
            ]
        )

        result_of_check = IndividualColumnChecker.is_there_only_one_unique_value_in_series(series=series)
        assert result_of_check == False

    def test_given_more_than_one_unique_value_in_pandas_series_when_check_if_is_whole_series_made_of_only_one_expected_value_check_then_false_is_being_returned(self):
        series = pd.Series(
            [
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'adausdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms'
            ]
        )

        result_of_check = IndividualColumnChecker.is_whole_series_made_of_only_one_expected_value(series=series, expected_value='btcusdt@depth@100ms')
        assert result_of_check == False

    def test_given_one_unique_value_in_pandas_series_when_check_if_is_whole_series_made_of_only_one_expected_value_check_then_false_is_being_returned(self):
        series = pd.Series(
            [
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms',
                'btcusdt@depth@100ms'
            ]
        )

        result_of_check = IndividualColumnChecker.is_whole_series_made_of_only_one_expected_value(series=series, expected_value='btcusdt@depth@100ms')
        assert result_of_check == True

    def test_given_pandas_series_with_non_descending_values_when_is_each_series_entry_greater_or_equal_to_previous_one_check_then_true_is_being_returned(self):
        series = pd.Series(
            [
                1,
                1,
                2,
                3,
                7,
                11,
                222,
                222
            ]
        )

        result_of_check = IndividualColumnChecker.is_each_series_entry_greater_or_equal_to_previous_one(series=series)
        assert result_of_check == True

    def test_given_pandas_series_with_non_ascending_values_when_is_each_series_entry_greater_or_equal_to_previous_one_check_then_false_is_being_returned(self):
        series = pd.Series(
            [
                1,
                1,
                2,
                1,
                7,
                11,
                222,
                222
            ]
        )

        result_of_check = IndividualColumnChecker.is_each_series_entry_greater_or_equal_to_previous_one(series=series)
        assert result_of_check == False
