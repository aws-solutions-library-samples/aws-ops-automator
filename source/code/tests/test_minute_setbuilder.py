import unittest

from scheduling.minute_setbuilder import MinuteSetBuilder


class TestMinuteSetBuilder(unittest.TestCase):
    def test_name(self):
        for i in range(0, 59):
            self.assertEquals(MinuteSetBuilder().build(str(i)), {i})

    def test_exceptions(self):
        self.assertRaises(ValueError, MinuteSetBuilder().build, "60")
