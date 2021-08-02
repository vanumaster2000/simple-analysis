from unittest import TestCase
from project.main import mp_avg_flight_time


class MpAvgFlightTimeTest(TestCase):
    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_first_arg_not_a_str(self):
        self.assertRaises(TypeError, mp_avg_flight_time, (12, 'str'))

    def test_second_arg_not_a_string(self):
        self.assertRaises(TypeError, mp_avg_flight_time, ('12', 12))

    def test_first_arg_is_in_incorrect_format(self):
        self.assertRaises(TypeError, mp_avg_flight_time, ('12-1333-313', '2012-03-05 10:12:31'))

    def test_second_arg_is_in_incorrect_format(self):
        self.assertRaises(TypeError, mp_avg_flight_time, ('2012-03-05 10:12:31', '2012-03-05 10:12:91'))

    def test_everything_is_correct(self):
        date1 = '2019-05-12 19:05:12'
        date2 = '2020-06-13 20:06:13'
        self.assertEqual(type(mp_avg_flight_time(date1, date2)), float)
