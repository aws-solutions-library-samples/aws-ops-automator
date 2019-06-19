######################################################################################################################
#  Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           #
#                                                                                                                    #
#  Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance        #
#  with the License. A copy of the License is located at                                                             #
#                                                                                                                    #
#      http://aws.amazon.com/asl/                                                                                    #
#                                                                                                                    #
#  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    #
#  and limitations under the License.                                                                                #
######################################################################################################################

from datetime import datetime, timedelta, tzinfo

import pytz
from scheduling.hour_setbuilder import HourSetBuilder
from scheduling.minute_setbuilder import MinuteSetBuilder
from scheduling.month_setbuilder import MonthSetBuilder
from scheduling.monthday_setbuilder import MonthdaySetBuilder
from scheduling.weekday_setbuilder import WeekdaySetBuilder


class CronExpression(object):
    """
    Class for performing matching for datetimes using expressions in cron syntax. For a full description of the supported cron
    syntax and features see See https://en.wikipedia.org/wiki/Cron
    """

    macros = {
        "@yearly": "0 0 1 1 *",
        "@monthly": "0 0 1 * *",
        "@weekly": "0 0 * * 0",
        "@daily": "0 0 * * *",
        "@hourly": "0 * * * *"
    }

    def __init__(self, expression=None,
                 minutes="*", hours="*", day_of_month="*", month="*", day_of_week="?",
                 dt=None, tz=None):
        """
        :param expression: Cron expression including minutes, hours, day of month, month and day of week. This parameter and the
        separate parameters for these elements are mutually exclusive. See https://en.wikipedia.org/wiki/Cron for supported syntax
        for this expression and the individual fields. @yearly, @monthly, @weekly, @daily and @hourly macros can be used as well.
        :param minutes: Expression minutes, 00-59, '/' for increments, '-' for ranges, ',' for sets  or None for *.
        :param hours: Expression hours, 0-23, 0am-23pm,'/' for increments, '-' for ranges, ',' for sets or None for *
        :param day_of_month: Expression days of month,  1-(28-31), W for nearest weekday, L for last day of month,
        '/' for increments, '-' for ranges, ',' for sets or None for *
        :param month: Expression month, 1-12, jan-dec, '/' for increments, '-' for ranges, ',' for sets or None for *. Wraps at dec.
        :param day_of_week: Expression days of week, 0-6, mon-sun, '/' for increments, '-' for ranges, ',' for sets,
        L for last occurrence of day in month, # for nth occurrence in month or None for ?. Wraps at sunday
        :param dt: Optional datetime to test set against
        :param tz: Optional timezone, if parameter dt is localized this parameter is ignored. Default timezone is UTC
        """

        fields = []
        # split and test fields of expression
        self._expression = expression
        if self._expression:
            if self._expression in CronExpression.macros:
                self._expression = CronExpression.macros[self._expression]
            fields = expression.split(" ")
            if len(fields) != 5:
                ValueError("Cron expression must have 5 fields")

        # use fields in expression of field parameters
        self._minutes_str = fields[0] if expression else minutes
        self._hours_str = fields[1] if expression else hours
        self._day_of_month_str = fields[2] if expression else day_of_month
        self._month_str = fields[3] if expression else month
        self._day_of_week_str = fields[4] if expression else day_of_week

        # lists of date and time elements for matching events
        self._minutes = None
        self._hours = None
        self._day_of_month = None
        self._month = None
        self._day_of_week = None

        # builders for date and time elements
        self._minutes_builder = None
        self._hours_builder = None
        self._day_of_month_builder = None
        self._month_builder = None
        self._day_of_week_builder = None

        # this is an optional default time that can be set to test against
        self._date = dt

        # store timezone from dt or timezone parameter
        if dt and dt.tzinfo is not None:
            self._timezone = dt.tzinfo
        elif tz is not None:
            self._timezone = tz if isinstance(tz, tzinfo) else pytz.timezone(tz)
        else:
            self._timezone = pytz.timezone("UTC")

    # Displays the prepared expression in a readable way
    def str(self, dt=None):
        """
        Displays a parsed expression in a human readable way
        :param dt: Datetime to build expression, required for using features like last day in month, last weekday etc.
        Use None for localized current time
        :return: Human readable string for expression
        """
        str_dt = self._prepare_expression(self._localized_time(dt))
        return str({"date": str(str_dt.date()),
                    "minute": self._minutes_builder.str(self._minutes),
                    "hour": self._hours_builder.str(self._hours),
                    "day": self._minutes_builder.str(self._day_of_month),
                    "month": self._month_builder.str(self._month),
                    "weekday": self._day_of_week_builder.str(self._day_of_week)})

    # Tests if the specified dt matches an event as specified in the cron expression
    def match(self, dt=None):
        """
        Tests if a specified or the localized current time matches the expression
        :param dt: Datetime to test or None to use current datetime
        :return: The tested dt if it does match the expression, None if it does not
        """
        dtz = self._prepare_expression(self._localized_time(dt))
        return dtz if all([dtz.day in self._day_of_month,
                           dtz.month in self._month,
                           dtz.weekday() in self._day_of_week,
                           dtz.minute in self._minutes,
                           dtz.hour in self._hours]) else None

    def since(self, start_dt, end_dt=None, most_recent_first=True):
        """
        Returns the number of matches for an expression from a start datetime until an end datetime
        :param start_dt: Start datetime
        :param end_dt: End datetime, use None for current localized datetime
        :param most_recent_first: Set to true to return most recent match first
        :return: Matches since the start datetime (excluding), up and until the end datetime (including)
        """
        end_dtz = self._localized_time(end_dt)

        # for efficiency in there are optimized functions for moving back and forward through the date range
        return self._matches_backwards(start_dt, end_dtz) \
            if most_recent_first \
            else self._matches_forwards(start_dt, end_dtz)

    def within_last(self, timespan, end_dt=None, most_recent_first=True):
        """
        Returns matches for an expression in a timespan backwards from an end datetime
        :param timespan: Length of period backwards from end datetime
        :param end_dt: End datetime (included), use None for current localized datetime
        :param most_recent_first: Set to true to return most recent match first
        :return: Matches for an expression in a timespan backwards from an end-date (included), [] if there was no match
        """
        end_dtz = self._localized_time(end_dt)
        start_dtz = end_dtz - timespan
        return self.since(start_dtz, end_dtz, most_recent_first)

    def last_since(self, since_dt, end_dt=None):
        """
        Returns the most recent match for an expression since a start datetime up and until an end datetime
        :param since_dt: Start datetime (excluding)
        :param end_dt: End datetime (including), use None for localized local time
        :return: Most recent match in the period since the start datetime up and until the end datetime, None if there was no match
        """
        end_dtz = self._localized_time(end_dt)
        for match in self.since(since_dt, end_dtz, most_recent_first=True):
            return match
        return None

    def last_within_last(self, timespan, end_dt=None):
        """
        Returns the most recent match for an expression in a timespan backwards from the end datetime
        :param timespan: Length of period backwards from end datetime
        :param end_dt: End datetime (included), use None for current localized datetime
        :return: Most recent match in the timespan backwards from the end datetime (including), None if there was no match
        """
        end_dtz = self._localized_time(end_dt)
        for match in self.within_last(timespan=timespan, end_dt=end_dtz, most_recent_first=True):
            return match
        return None

    def first_since(self, since_dt, end_dt=None):
        """
        Returns the first match for an expression in a period since a start datetime (excluding) up and until the end datetime
        :param since_dt: Start of the period (excluding)
        :param end_dt: End of the period (including), use None for localized current time
        :return: First match for an expression for a period since a start datetime (excluding) up and until the end datetime,
        None if there was no match
        """
        end_dtz = self._localized_time(end_dt)
        for match in self.since(start_dt=since_dt, end_dt=end_dtz, most_recent_first=False):
            return match
        return None

    def first_within_last(self, timespan, end_dt=None):
        """
        Returns the first match for an expression in a timespan backwards from the end datetime
        :param timespan: Length of period backwards from end datetime
        :param end_dt: End datetime (included), use None for current localized datetime
        :return: First recent match in the timespan backwards from the end datetime (including), None if there was no match
        """
        end_dtz = self._localized_time(end_dt)
        for match in self.within_last(timespan=timespan, end_dt=end_dtz, most_recent_first=False):
            return match
        return None

    def until(self, end_dt, start_dt=None, earliest_first=True):
        """
        Returns all matches for an expression in a period from the start datetime (including) until the end datetime (excluding)
        :param end_dt: End datetime for the period (excluding)
        :param start_dt: Start datetime for the period (including), use None for localized current datetime
        :param earliest_first: Set to True to return earliest match first
        :return: Matches for an expression in a period from the start datetime until the end datetime, [] if there no matches
        """

        # for efficiency in there are optimized functions for moving back and forward through the date range
        start_dtz = self._localized_time(start_dt)
        return self._matches_forwards(start_dtz, end_dt) \
            if earliest_first \
            else self._matches_backwards(start_dtz, end_dt)

    def within_next(self, timespan, start_dt=None, earliest_first=True):
        """
        Returns all matches for an expression in a timespan starting at the start datetime (excluding)
        :param timespan: Length of the timespan forwards in time
        :param start_dt: Start datetime, use None for localized current datetime
        :param earliest_first: Set to True to return earliest match first
        :return: Matches for an expression in a timespan starting at the start datetime, [] if there are no matches
        """
        start_dtz = self._localized_time(start_dt)
        end_dtz = start_dtz + timespan
        return self.until(end_dtz, start_dtz, earliest_first)

    def last_until(self, end_dt, start_dt=None):
        """
        Returns last match for a period from the start datetime (excluding) until the end datetime (including)
        :param end_dt: End datetime (including)
        :param start_dt: Start datetime (excluding), use None for localized current datetime
        :return: Last match for a period from the start datetime until the end datetime, None if there was no match
        """
        start_dtz = self._localized_time(start_dt)
        for match in self.until(end_dt, start_dtz, earliest_first=False):
            return match

    def last_within_next(self, timespan, start_dt=None):
        """
        Return the last match in a timespan starting at the start date (excluding)
        :param timespan: Length of the timespan forwards in time
        :param start_dt: Start datetime, use None for localized current datetime
        :return: Last match for the period, None if there was no match
        """
        start_dtz = self._localized_time(start_dt)
        for match in self.within_next(timespan=timespan, start_dt=start_dtz, earliest_first=False):
            return match

    def first_until(self, end_dt, start_dt=None):
        """
        Returns first match for a period from the start datetime (excluding) until the end datetime (including)
        :param end_dt: End datetime (including)
        :param start_dt: Start datetime (excluding), use None for localized current datetime
        :return: First match for a period from the start datetime until the end datetime, None if there was no match
        """
        start_dtz = self._localized_time(start_dt)
        for match in self.until(end_dt, start_dt=start_dtz, earliest_first=True):
            return match

    def first_within_next(self, timespan, start_dt=None):
        """
        Return the first match in a timespan starting at the start date (excluding)
        :param timespan:  Length of the timespan forwards in time
        :param start_dt: Start datetime, use None for localized current datetime
        :return: First match for the period, None if there was no match
        """
        start_dtz = self._localized_time(start_dt)
        for match in self.within_next(timespan=timespan, start_dt=start_dtz, earliest_first=True):
            return match

    def validate(self):
        """
        Method to test if provided expression is valid before actually using the expression
        :return:
        """
        self._prepare_expression(datetime.now())

    def _localized_time(self, dt=None):
        """
        Checks if timezone information must be added to tz. If the dt parameter is not set then the default dt set in the parameter
        if used if it was set otherwise the  localized current time is used
        :param dt: Tested datetime or None for current datetime
        :return: Localized datetime
        """
        if dt:
            return dt.replace(tzinfo=self._timezone) if dt.tzinfo is None else dt
        return datetime.now(tz=self._timezone)

    def _prepare_expression(self, dt):
        """
        Prepares internal builders for expression elements for testing a datetime
        :param dt: Tested datetime
        :return: Tested datetime
        """

        # minute set builder
        if self._minutes is None:
            self._minutes_builder = MinuteSetBuilder()
            self._minutes = sorted(self._minutes_builder.build(self._minutes_str))

        # hours set builder
        if self._hours is None:
            self._hours_builder = HourSetBuilder()
            self._hours = sorted(self._hours_builder.build(self._hours_str))

        # month set builder
        if self._month is None:
            self._month_builder = MonthSetBuilder()
            self._month = sorted(MonthSetBuilder().build(self._month_str))

        # day of month and day in week builders, note that these depend on the date being tested
        if self._date is None or self._date.date() != dt.date():
            # first time or if date to be tested differs from previous test
            # day of month builder
            self._day_of_month_builder = MonthdaySetBuilder(year=dt.year, month=dt.month)
            self._day_of_month = sorted(self._day_of_month_builder.build(self._day_of_month_str))
            # day of week builder
            self._day_of_week_builder = WeekdaySetBuilder(year=dt.year, month=dt.month, day=dt.day)
            self._day_of_week = sorted(self._day_of_week_builder.build(self._day_of_week_str))
            # store the date for which the builders are prepared
            self._date = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return dt

    def _matches_backwards(self, start_dt, end_dt):

        """
        Find matches in a period specified by a start and end datetime. The search is performed backwards starting from the end
        datetime (including) until the start datetime (excluding). For efficiency there are separate methods for searching forwards
        and backwards.
        :param start_dt: Start datetime (excluding)
        :param end_dt: End datetime (including), use None for localized current datetime
        :return: Set of matching datetimes for the expression in the period, [] if there are no matches
        """
        start_dtz = self._localized_time(start_dt).replace(second=0, microsecond=0)
        dtz = self._localized_time(end_dt).replace(second=0, microsecond=0)

        # until the start of the period is reached move backwards
        while dtz > start_dtz:
            self._prepare_expression(dtz)
            # month does not match, move back previous month
            if dtz.month not in self._month:
                dtz = self._move_to_previous_month(dtz)
                continue

            # day or weekday does not match, move back to previous day
            if dtz.day not in self._day_of_month or dtz.weekday() not in self._day_of_week:
                dtz = self._move_to_previous_day(dtz)
                continue

            # hours do not match move to last event in previous hour
            if dtz.hour not in self._hours:
                dtz = self._move_to_previous_hour(dtz)
                continue

            # minutes do not match, move back to last event in previous minute
            if dtz.minute not in self._minutes:
                dtz = self._move_to_previous_minute(dtz)
                continue

            yield dtz
            dtz = self._move_to_previous_minute(dtz)

    def _matches_forwards(self, start_dt, end_dt):
        """
         Find matches in a period specified by a start and end datetime. The search is performed forwards starting from the start
        datetime (excluding) until the start datetime (including). For efficiency there are separate methods for searching forwards
        and backwards.
        :param start_dt: Start datetime (excluding)
        :param end_dt: End datetime (including)
        :return: Set of matching datetimes for the expression in the period, [] if there are no matches
        """

        dtz = self._localized_time(start_dt).replace(second=0, microsecond=0) + timedelta(minutes=1)
        end_dtz = self._localized_time(end_dt)

        # until the end of the period is reached
        while dtz <= end_dtz:
            self._prepare_expression(dtz)
            # month does not match, move forward to first event in next month
            if dtz.month not in self._month:
                dtz = self._move_to_next_month(dtz)
                continue

            # day or weekday does not match move forward to first event in next day
            if dtz.day not in self._day_of_month or dtz.weekday() not in self._day_of_week:
                dtz = self._move_to_next_day(dtz)
                continue

            # hours do not match move forward to first event in next hour
            if dtz.hour not in self._hours:
                dtz = self._move_to_next_hour(dtz)
                continue

            # minutes do not match, move forward to first event in next minute
            if dtz.minute not in self._minutes:
                dtz = self._move_to_next_minute(dtz)
                continue

            yield dtz
            dtz = self._move_to_next_minute(dtz)

    def _move_to_previous_month(self, dt):
        """
        Move to end of previous month
        :param dt: Tested datetime
        :return: Last day for expression in previous month
        """
        # is the current month is the list
        if dt.month in self._month:
            # get the index -1, note that if the index is -1  the last item in the list is used so no special
            # handling required if it is the first one in the list and we have to move back to previous month
            index = self._month.index(dt.month) - 1
        else:
            # find a matching index in the list that is <= current month
            index = len(self._month) - 1
            while index >= 0:
                if self._month[index] <= dt.month:
                    break
                index -= 1

        # get the month from the list
        previous_month = self._month[index]

        # year - 1 if the new month is later than the current month
        year = dt.year if previous_month < dt.month else dt.year - 1

        previous_month_builder = MonthdaySetBuilder(year=year, month=previous_month)
        month_days_previous = sorted(previous_month_builder.build(self._day_of_month_str))
        day = max(month_days_previous) if month_days_previous != [] else previous_month_builder.last

        # return the last event for the last day of the previous month
        return datetime(year=year, month=previous_month, day=day, hour=max(self._hours), minute=max(self._minutes),
                        tzinfo=dt.tzinfo)

    def _move_to_next_month(self, dt):
        """
        Move to first day in next month
        :param dt: Tested datetime
        :return: First day in next month for expression
        """
        # is the current month is the set
        if dt.month in self._month:
            # get the index + 1, wrap to first in list if last entry
            index = (self._month.index(dt.month) + 1) % len(self._month)
        else:
            # find a matching index in the list that is >= current month
            index = 0
            while index < len(self._month) - 1:
                if self._month[index] >= dt.month:
                    break
                index += 1
        # get the month
        next_month = self._month[index]

        # year + 1 if the new month is earlier than the next month
        year = dt.year if next_month > dt.month else dt.year + 1

        next_month_builder = MonthdaySetBuilder(year=year, month=next_month)
        month_days_next = sorted(next_month_builder.build(self._day_of_month_str))
        day = min(month_days_next) if month_days_next != [] else next_month_builder.first

        return datetime(year=year, month=next_month, day=day, hour=min(self._hours), minute=min(self._minutes), tzinfo=dt.tzinfo)

    def _move_to_previous_day(self, dt):
        """
        Move to previous day for expression, move to previous month if necessary
        :param dt: Tested datetime
        :return: Next day for expression
        """

        # test if day is in the current set
        if dt.day in self._day_of_month:
            # if so use the index -1
            index = self._day_of_month.index(dt.day) - 1
        else:
            # otherwise get the index of the first entry equal or less than the current day
            index = len(self._day_of_month) - 1
            while index > 0:
                if self._day_of_month[index] <= dt.day:
                    break
                index -= 1

        # get the day from the set, if it is empty use the current day to move into the next month
        d = self._day_of_month[index] if index > -1 else dt.day

        # if the day > the current day then move back to the last event in the previous month
        if d >= dt.day:
            return self._move_to_previous_month(dt)
        else:
            # last event of previous day in same month
            return dt.replace(day=d, hour=max(self._hours), minute=max(self._minutes))

    def _move_to_next_day(self, dt):
        """
        Move to next day for expression, move to next month if necessary
        :param dt: Tested datetime
        :return: Next day for expression
        """
        # test if day is in the current set
        if dt.day in self._day_of_month:
            # if so use the index -1
            index = (self._day_of_month.index(dt.day) + 1) % len(self._day_of_month)
        else:
            # otherwise get the index of the first entry >= the current day
            index = 0
            while index < len(self._day_of_month) - 1:
                if self._day_of_month[index] >= dt.day:
                    break
                index += 1

        # get the day from the set, if it is empty then use current day to move forward to next month
        d = self._day_of_month[index] if self._day_of_month != [] else dt.day

        # if the day > the current day then move back to the last event in the previous month
        if d <= dt.day:
            return self._move_to_next_month(dt)
        else:
            # last event of previous day in same month
            return dt.replace(day=d, hour=min(self._hours), minute=min(self._minutes))

    def _move_to_previous_hour(self, dt):
        """
        Move to previous hour for expression, move to previous day if necessary
        :param dt: Tested datetime
        :return: Previous hour for expression
        """
        # test if current hour is in the set
        if dt.hour in self._hours:
            # if so use the index -1
            index = self._hours.index(dt.hour) - 1
        else:
            # otherwise find the first entry in the set that is <= current hour
            index = len(self._hours) - 1
            while index > 0:
                if self._hours[index] <= dt.hour:
                    break
                index -= 1
        # get the hour from the list
        h = self._hours[index]

        # if the hour >= the current hour then move back to last event in previous day
        if h >= dt.hour:
            return self._move_to_previous_day(dt)
        else:
            # previous event at same day
            return dt.replace(hour=h, minute=max(self._minutes))

    def _move_to_next_hour(self, dt):
        """
        Move to next hour for expression, move to next day if necessary
        :param dt: Tested datetime
        :return: Next hour for expression
        """
        # test if current hour is in the set
        if dt.hour in self._hours:
            # if so used the index -1
            index = (self._hours.index(dt.hour) + 1) % len(self._hours)
        else:
            # otherwise find the first entry in the set that is >= current hour
            index = 0
            while index < len(self._hours) - 1:
                if self._hours[index] >= dt.hour:
                    break
                index += 1
        # get the hour from the list
        h = self._hours[index]

        # if the hour <= the current hour then move to first event in next day
        if h <= dt.hour:
            return self._move_to_next_day(dt)
        else:
            # next event at same day
            return dt.replace(hour=h, minute=min(self._minutes))

    def _move_to_previous_minute(self, dt):
        """
        Move to previous minute, move to previous hour if necessary
        :param dt: Tested datetime
        :return: Previous minute for expression
        """
        # test if current minute is in the set
        if dt.minute in self._minutes:
            # use the index -1
            index = self._minutes.index(dt.minute) - 1
        else:
            # otherwise find the index of the minute <= current minute
            index = len(self._minutes) - 1
            while index > 0:
                if self._minutes[index] <= dt.minute:
                    break
                index -= 1

        # get minute from the list
        minute = self._minutes[index]

        if minute >= dt.minute:
            # if minute >= current minute then return last event in previous hour
            return self._move_to_previous_hour(dt)
        else:
            # return previous event in same hour
            return dt.replace(minute=minute)

            # gets the last event in the previous minute in the set

    def _move_to_next_minute(self, dt):
        """
        Moves to next minute, move to next hour if necessary
        :param dt: tested datetime
        :return: Next minute for expression
        """
        # test if current minute is in the set
        if dt.minute in self._minutes:
            # use the index +1
            index = (self._minutes.index(dt.minute) + 1) % len(self._minutes)
        else:
            # otherwise find the index of the minute <= current minute
            index = 0
            while index < len(self._minutes) - 1:
                if self._minutes[index] >= dt.minute:
                    break
                index += 1

        # get minute from the list
        minute = self._minutes[index]

        if minute <= dt.minute:
            # if minute <= current minute then return first event in next hour
            return self._move_to_next_hour(dt)
        else:
            # return next event in same hour
            return dt.replace(minute=minute)
