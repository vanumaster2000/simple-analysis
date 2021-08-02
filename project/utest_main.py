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
