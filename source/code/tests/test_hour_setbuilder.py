import unittest

from scheduling.hour_setbuilder import HourSetBuilder


class TestHourSetBuilder(unittest.TestCase):
    def test_name(self):
        for i in range(0, 24):
            self.assertEquals(HourSetBuilder().build(str(i)), {i})

        for i in range(1, 11):
            self.assertEquals(HourSetBuilder().build(str(i) + "am"), {i})
            self.assertEquals(HourSetBuilder().build(str(i) + "AM"), {i})

        for i in range(1, 11):
            self.assertEquals(HourSetBuilder().build(str(i) + "pm"), {i + 12})
            self.assertEquals(HourSetBuilder().build(str(i) + "PM"), {i + 12})

        self.assertEquals(HourSetBuilder().build("12am"), {0})
        self.assertEquals(HourSetBuilder().build("12pm"), {12})

    def test_exceptions(self):
        for h in range(13, 25):
            self.assertRaises(ValueError, HourSetBuilder().build, (str(h) + "PM"))
            self.assertRaises(ValueError, HourSetBuilder().build, "PM")

        self.assertRaises(ValueError, HourSetBuilder().build, "24")
        self.assertRaises(ValueError, HourSetBuilder().build, "-1")
