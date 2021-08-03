from unittest import TestCase
from project.main import mp_avg_flight_time, mp_delay_time, filler


class MpAvgFlightTimeTest(TestCase):
    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_first_arg_not_a_str(self) -> None:
        self.assertRaises(TypeError, mp_avg_flight_time, (12, 'str'))

    def test_second_arg_not_a_string(self) -> None:
        self.assertRaises(TypeError, mp_avg_flight_time, ('12', 12))

    def test_first_arg_is_in_incorrect_format(self) -> None:
        self.assertRaises(TypeError, mp_avg_flight_time, ('12-1333-313', '2012-03-05 10:12:31'))

    def test_second_arg_is_in_incorrect_format(self) -> None:
        self.assertRaises(TypeError, mp_avg_flight_time, ('2012-03-05 10:12:31', '2012-03-05 10:12:91'))

    def test_everything_is_correct(self) -> None:
        date1 = '2020-06-13 20:06:13'
        date2 = '2019-05-12 19:05:12'
        self.assertEqual(type(mp_avg_flight_time(date1, date2)), float)

    def first_arg_is_bigger_than_second(self) -> None:
        date1 = '2019-05-12 19:05:12'
        date2 = '2020-06-13 20:06:13'
        self.assertRaises(ValueError, mp_avg_flight_time, (date1, date2))


class MpDelayTimeTest(TestCase):
    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_first_arg_not_a_str(self) -> None:
        self.assertRaises(TypeError, mp_delay_time, (12, 'str'))

    def test_second_arg_not_a_string(self) -> None:
        self.assertRaises(TypeError, mp_delay_time, ('12', 12))

    def test_first_arg_is_in_incorrect_format(self) -> None:
        self.assertRaises(TypeError, mp_delay_time, ('12-1333-313', '2012-03-05 10:12:31'))

    def test_second_arg_is_in_incorrect_format(self) -> None:
        self.assertRaises(TypeError, mp_delay_time, ('2012-03-05 10:12:31', '2012-03-05 10:12:91'))

    def test_everything_is_correct(self) -> None:
        date1 = '2020-06-13 20:06:13'
        date2 = '2019-05-12 19:05:12'
        self.assertEqual(type(mp_delay_time(date1, date2)), float)


class FillerTest(TestCase):
    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_main_delimiter_is_correct(self) -> None:
        res = f'\n' \
              f'{"=" * 50}' \
              f'\n'
        self.assertEqual(filler('='), res)

    def test_usual_delimiter_is_correct(self) -> None:
        res_dict = {
            '-': '-' * 50,
            '/': '/' * 50,
            '#': '#' * 50
        }
        for key in res_dict.keys():
            self.assertEqual(filler(key), res_dict[key])

    def test_arg_class_is_incorrect(self) -> None:
        self.assertRaises(TypeError, filler, 2)
        self.assertRaises(TypeError, filler, ('2', '4'))
