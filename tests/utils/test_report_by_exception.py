from unittest import TestCase
from aleph_core.utils.report_by_exception import ReportByExceptionHelper


class ReportByExceptionTestCase(TestCase):

    def test_report_by_exception_with_ids(self):
        R = ReportByExceptionHelper()

        _data = [
            {"id_": "1", "a": "alpha", "b": 1},
            {"id_": "2", "a": "gamma", "b": 1},
            {"id_": "3", "a": "delta", "b": 1},
        ]
        data = R.compare(_data)
        self.assertEqual(_data, data)

        _data = [
            {"id_": "1", "a": "hello", "b": 1},
            {"id_": "2", "a": "gamma", "b": 2}
        ]
        data = R.compare(_data)
        self.assertEqual(data, [
            {"id_": "1", "a": "hello"},
            {"id_": "2", "b": 2}
        ])

        _data = [
            {"id_": "2", "b": 3}
        ]
        data = R.compare(_data)
        self.assertEqual(data, [{"id_": "2", "b": 3}])

        _data = [
            {"id_": "2", "b": 3}
        ]
        data = R.compare(_data)
        self.assertEqual(data, [])

    def test_report_by_exception_without_ids(self):
        R = ReportByExceptionHelper()

        _data = [{"a": "alpha", "b": 1, "t": 1}]
        data = R.compare(_data)
        self.assertEqual(data, _data)

        _data = [{"a": "gamma", "b": 1, "t": 2}]
        data = R.compare(_data)
        self.assertEqual(data, [{"a": "gamma", "t": 2}])

        _data = [{"a": "delta", "b": 2, "t": 3}]
        data = R.compare(_data)
        self.assertEqual(data, _data)

        _data = [{"c": True, "t": 4}]
        data = R.compare(_data)
        self.assertEqual(data, _data)

        _data = [{"a": "delta", "b": 2, "c": True, "t": 5}]
        data = R.compare(_data)
        self.assertEqual(data, [])
