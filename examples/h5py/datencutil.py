#!/usr/bin/env python
"""
This module performs conversions of time coordinate data to/from datetime objects,
it borrows from the netCDF4 netcdftime.py. This module was pasted together so 
users/testers that run the examples/tests are not required to install a fullup 
netcdf library suite just to handle datetime values/arrays. This slimmed down 
version of netcdftime is not intended to be as thorough, as some of the uncommon 
odd calendars are not available in here.
"""
#TODO: test this module 

import re
import datetime
import numpy 

microsec_units = ['microseconds','microsecond', 'microsec', 'microsecs']
millisec_units = ['milliseconds', 'millisecond', 'millisec', 'millisecs']
sec_units =      ['second', 'seconds', 'sec', 'secs', 's']
min_units =      ['minute', 'minutes', 'min', 'mins']
hr_units =       ['hour', 'hours', 'hr', 'hrs', 'h']
day_units =      ['day', 'days', 'd']
_units = microsec_units+millisec_units+sec_units+min_units+hr_units+day_units
_calendars = ['standard', 'gregorian', 'proleptic_gregorian', 'julian'] 

# Adapted from http://delete.me.uk/2005/03/iso8601.html (netCDF4)
ISO8601_REGEX = re.compile(r"(?P<year>[+-]?[0-9]{1,4})(-(?P<month>[0-9]{1,2})(-(?P<day>[0-9]{1,2})"
                           r"(((?P<separator1>.)(?P<hour>[0-9]{1,2}):(?P<minute>[0-9]{1,2})(:(?P<second>[0-9]{1,2})(\.(?P<fraction>[0-9]+))?)?)?"
                           r"((?P<separator2>.?)(?P<timezone>Z|(([-+])([0-9]{1,2}):([0-9]{1,2}))))?)?)?)?"
                           )

def parse_timezone(tzstring):
    """
    Parses ISO 8601 time zone specs into tzinfo offsets
    Adapted from pyiso8601 (http://code.google.com/p/pyiso8601/)
    """
    if tzstring == "Z":
        return 0
    # This isn't strictly correct, but it's common to encounter dates without
    # timezones so I'll assume the default (which defaults to UTC).
    if tzstring is None:
        return 0
    m = TIMEZONE_REGEX.match(tzstring)
    prefix, hours, minutes = m.groups()
    hours, minutes = int(hours), int(minutes)
    if prefix == "-":
        hours = -hours
        minutes = -minutes
    return minutes + hours * 60.

def parse_date(datestring):
    """
    Parses ISO 8601 dates into datetime objects
    The timezone is parsed from the date string, assuming UTC by default.
    Adapted from pyiso8601 (http://code.google.com/p/pyiso8601/)
    """
    if not isinstance(datestring, str) and not isinstance(datestring, unicode):
        raise ValueError("Expecting a string %r" % datestring)
    m = ISO8601_REGEX.match(datestring.strip())
    if not m:
        raise ValueError("Unable to parse date string %r" % datestring)
    groups = m.groupdict()
    tzoffset_mins = parse_timezone(groups["timezone"])
    if groups["hour"] is None:
        groups["hour"] = 0
    if groups["minute"] is None:
        groups["minute"] = 0
    if groups["second"] is None:
        groups["second"] = 0
    iyear = int(groups["year"])
    return iyear, int(groups["month"]), int(groups["day"]),\
        int(groups["hour"]), int(groups["minute"]), int(groups["second"]),\
        tzoffset_mins
#parse_date

def dateparse(timestr):
    """
    Parse a string of the form time-units since yyyy-mm-dd hh:mm:ss
    return a tuple (units,utc_offset, datetimeinstance)"""
    global _units
    timestr_split = timestr.split()
    units = timestr_split[0].lower()
    if units not in _units:
        raise ValueError(
            "units must be one of 'seconds', 'minutes', 'hours' or 'days' (or singular version of these), got '%s'" % units)
    if timestr_split[1].lower() != 'since':
        raise ValueError("no 'since' in unit_string")
    # parse the date string.
    n = timestr.find('since') + 6
    year, month, day, hour, minute, second, utc_offset = parse_date(timestr[n:].strip())
    return units, utc_offset, datetime.datetime(year, month, day, hour, minute, second)


def JulianDayFromDate(date, calendar='standard'):
    """
    creates a Julian Day from a 'datetime-like' object.  Returns the fractional
    Julian Day (resolution approx 0.1 second).

    if calendar='standard' or 'gregorian' (default), Julian day follows Julian
    Calendar on and before 1582-10-5, Gregorian calendar after 1582-10-15.

    if calendar='proleptic_gregorian', Julian Day follows gregorian calendar.

    if calendar='julian', Julian Day follows julian calendar.

    Algorithm:

    Meeus, Jean (1998) Astronomical Algorithms (2nd Edition). Willmann-Bell,
    Virginia. p. 63

    """
    # based on redate.py by David Finlayson. Check if input was scalar and change return accordingly
    try:
        date[0]
    except:
        isscalar = True

    date = numpy.atleast_1d(numpy.array(date))
    year = numpy.empty(len(date), dtype=numpy.int32)
    month = year.copy()
    day = year.copy()
    hour = year.copy()
    minute = year.copy()
    second = year.copy()
    microsecond = year.copy()
    for i, d in enumerate(date):
        year[i] = d.year
        month[i] = d.month
        day[i] = d.day
        hour[i] = d.hour
        minute[i] = d.minute
        second[i] = d.second
        microsecond[i] = d.microsecond
    # Convert time to fractions of a day
    day = day + hour / 24.0 + minute / 1440.0 + (second + microsecond/1.e6) / 86400.0

    # Start Meeus algorithm (variables are in his notation)
    month_lt_3 = month < 3
    month[month_lt_3] = month[month_lt_3] + 12
    year[month_lt_3] = year[month_lt_3] - 1

    A = (year / 100).astype(numpy.int64)

    jd = 365. * year + numpy.int32(0.25 * year + 2000.) + numpy.int32(30.6001 * (month + 1)) + day + 1718994.5

    # optionally adjust the jd for the switch from the Julian to Gregorian Calendar
    # here assumed to have occurred the day after 1582 October 4
    if calendar in ['standard', 'gregorian']:
        if numpy.any((jd >= 2299160.5) & (jd < 2299170.5)): # missing days in Gregorian calendar
            raise ValueError( 'impossible date (falls in gap between end of Julian calendar and beginning of Gregorian calendar')
        B = numpy.zeros(len(jd))             # 1582 October 5 (Julian Calendar)
        ii = numpy.where(jd >= 2299170.5)[0] # 1582 October 15 (Gregorian Calendar)
        if ii.size>0:
            B[ii] = 2 - A[ii] + numpy.int32(A[ii] / 4)
    elif calendar == 'proleptic_gregorian':
        B = 2 - A + numpy.int32(A / 4)
    elif calendar == 'julian':
        B = numpy.zeros(len(jd))
    else:
        raise ValueError('unknown calendar, must be one of julian, standard, gregorian, proleptic_gregorian, got %s' % calendar)

    # adjust for Julian calendar if necessary
    jd = jd + B

    # Add a small offset (proportional to Julian date) for correct re-conversion.
    # This is about 45 microseconds in 2000 for Julian date starting -4712.
    # (pull request #433).
    eps = numpy.finfo(float).eps
    eps = numpy.maximum(eps*jd, eps)
    jd += eps
    if isscalar:
        return jd[0]
    else:
        return jd
#JulianDayFromDate


def DateFromJulianDay(JD, calendar='standard'):
    """
    returns a 'datetime-like' object given Julian Day. Julian Day is a
    fractional day with a resolution of approximately 0.1 seconds.

    if calendar='standard' or 'gregorian' (default), Julian day follows Julian
    Calendar on and before 1582-10-5, Gregorian calendar after  1582-10-15.

    if calendar='proleptic_gregorian', Julian Day follows gregorian calendar.

    if calendar='julian', Julian Day follows julian calendar.

    The datetime object is a 'real' datetime object if the date falls in
    the Gregorian calendar (i.e. calendar='proleptic_gregorian', or
    calendar = 'standard'/'gregorian' and the date is after 1582-10-15).
    Otherwise, it's a 'phony' datetime object which is actually an instance
    of netcdftime.datetime.

    Algorithm:

    Meeus, Jean (1998) Astronomical Algorithms (2nd Edition). Willmann-Bell,
    Virginia. p. 63
    """
    # based on redate.py by David Finlayson.

    julian = numpy.array(JD, dtype=float)
    if numpy.min(julian) < 0:
        raise ValueError('Julian Day must be positive')

    dayofwk = numpy.atleast_1d(numpy.int32(numpy.fmod(numpy.int32(julian + 1.5), 7)))
    # get the day (Z) and the fraction of the day (F)
    # add 0.000005 which is 452 ms in case of jd being after
    # second 23:59:59 of a day we want to round to the next day see issue #75
    Z = numpy.atleast_1d(numpy.int32(numpy.round(julian)))
    F = numpy.atleast_1d(julian + 0.5 - Z).astype(numpy.float64)
    if calendar in ['standard', 'gregorian']:
        alpha = numpy.int32(((Z - 1867216.) - 0.25) / 36524.25)
        A = Z + 1 + alpha - numpy.int32(0.25 * alpha)
        # check if dates before oct 5th 1582 are in the array
        ind_before = numpy.where(julian < 2299160.5)[0]
        if len(ind_before) > 0:
            A[ind_before] = Z[ind_before]

    elif calendar == 'proleptic_gregorian':
        alpha = numpy.int32(((Z - 1867216.) - 0.25) / 36524.25)
        A = Z + 1 + alpha - numpy.int32(0.25 * alpha)
    elif calendar == 'julian':
        A = Z
    else:
        raise ValueError(
            'unknown calendar, must be one of julian,standard,gregorian,proleptic_gregorian, got %s' % calendar)

    B = A + 1524
    C = numpy.atleast_1d(numpy.int32(6680. + ((B - 2439870.) - 122.1) / 365.25))
    D = numpy.atleast_1d(numpy.int32(365 * C + numpy.int32(0.25 * C)))
    E = numpy.atleast_1d(numpy.int32((B - D) / 30.6001))

    # Convert to date
    day = numpy.clip(B - D - numpy.int64(30.6001 * E) + F, 1, None)
    nday = B - D - 123
    dayofyr = nday - 305
    ind_nday_before = numpy.where(nday <= 305)[0]
    if len(ind_nday_before) > 0:
        dayofyr[ind_nday_before] = nday[ind_nday_before] + 60
    month = E - 1
    month[month > 12] = month[month > 12] - 12
    year = C - 4715
    year[month > 2] = year[month > 2] - 1
    year[year <= 0] = year[year <= 0] - 1

    # a leap year?
    leap = numpy.zeros(len(year),dtype=dayofyr.dtype)
    leap[year % 4 == 0] = 1
    if calendar == 'proleptic_gregorian':
        leap[(year % 100 == 0) & (year % 400 != 0)] = 0
    elif calendar in ['standard', 'gregorian']:
        leap[(year % 100 == 0) & (year % 400 != 0) & (julian < 2299160.5)] = 0

    inc_idx = numpy.where((leap == 1) & (month > 2))[0]
    dayofyr[inc_idx] = dayofyr[inc_idx] + leap[inc_idx]

    # Subtract the offset from JulianDayFromDate from the microseconds (pull
    # request #433).
    eps = numpy.finfo(float).eps
    eps = numpy.maximum(eps*julian, eps)
    hour = numpy.clip((F * 24.).astype(numpy.int64), 0, 23)
    F   -= hour / 24.
    minute = numpy.clip((F * 1440.).astype(numpy.int64), 0, 59)
    # this is an overestimation due to added offset in JulianDayFromDate
    second = numpy.clip((F - minute / 1440.) * 86400., 0, None)
    microsecond = (second % 1)*1.e6
    # remove the offset from the microsecond calculation.
    microsecond = numpy.clip(microsecond - eps*86400.*1e6, 0, 999999)

    # convert year, month, day, hour, minute, second to int32
    year = year.astype(numpy.int32)
    month = month.astype(numpy.int32)
    day = day.astype(numpy.int32)
    hour = hour.astype(numpy.int32)
    minute = minute.astype(numpy.int32)
    second = second.astype(numpy.int32)
    microsecond = microsecond.astype(numpy.int32)

    # check if input was scalar and change return accordingly
    isscalar = False
    try:
        JD[0]
    except:
        isscalar = True
    # return a 'real' datetime instance if calendar is gregorian.
    if calendar in 'proleptic_gregorian' or \
            (calendar in ['standard', 'gregorian'] and len(ind_before) == 0):
        if not isscalar:
            return numpy.array([ datetime.datetime(*args) for args in zip(year, month, day, hour, minute, second, microsecond)])

        else:
            return datetime.datetime(year[0], month[0], day[0], hour[0], minute[0], second[0], microsecond[0])
    else:
        # or else, return a 'datetime-like' instance.
        if not isscalar:
            return numpy.array([datetime.datetime(*args)
                             for args in
                             zip(year, month, day, hour, minute,
                                 second, microsecond, dayofwk, dayofyr)])
        else:
            return datetime.datetime(year[0], month[0], day[0], hour[0],
                            minute[0], second[0], microsecond[0], dayofwk[0],
                            dayofyr[0])
#DateFromJulianDay

def num2date(time_value, origin, units, calendar, tzoffset):
    """
    Return a 'datetime' object given a in units described by unit_string using calendar

    dates are in UTC with no offset, even if L{unit_string} contains
    a time zone offset from UTC.

    Resolution is approximately 0.1 seconds.

    Works for scalars, sequences and numpy arrays.
    Returns a scalar if input is a scalar, else returns a numpy array.

    The datetime instances returned by C{num2date} are 'real' python datetime
    objects if the date falls in the Gregorian calendar (i.e.
    C{calendar='proleptic_gregorian'}, or C{calendar = 'standard'/'gregorian'} and
    the date is after 1582-10-15). Otherwise, they are 'phony' datetime
    objects which are actually instances of netcdftime.datetime.  This is
    because the python datetime module cannot handle the weird dates in some
    calendars (such as C{'360_day'} and C{'all_leap'}) which
    do not exist in any real world calendar.
    """
    isscalar = False
    try:
        time_value[0]
    except:
        isscalar = True
    ismasked = False
    if hasattr(time_value, 'mask'):
        mask = time_value.mask
        ismasked = True
    if not isscalar:
        time_value = numpy.array(time_value, dtype='d')
        shape = time_value.shape
    # convert to desired units, add time zone offset.
    if units in microsec_units:
            jdelta = time_value / 86400000000. + tzoffset / 1440.
    elif units in millisec_units:
            jdelta = time_value / 86400000. + tzoffset / 1440.
    elif units in sec_units:
            jdelta = time_value / 86400. + tzoffset / 1440.
    elif units in min_units:
            jdelta = time_value / 1440. + tzoffset / 1440.
    elif units in hr_units:
            jdelta = time_value / 24. + tzoffset / 1440.
    elif units in day_units:
            jdelta = time_value + tzoffset / 1440.
    else:
        raise ValueError('unsupported time units')
    jd = origin + jdelta
    if calendar in ['julian', 'standard', 'gregorian', 'proleptic_gregorian']:
            if not isscalar:
                if ismasked:
                    date = []
                    for j, m in zip(jd.flat, mask.flat):
                        if not m:
                            date.append(DateFromJulianDay(j, calendar))
                        else:
                            date.append(None)
                else:
                    date = DateFromJulianDay(jd.flat, calendar)
            else:
                if ismasked and mask.item():
                    date = None
                else:
                    date = DateFromJulianDay(jd, calendar)
    if isscalar:
        return date
    else:
        return numpy.reshape(numpy.array(date), shape)
#num2date

def get_origin_num(unit_string, calendar): 
    global _calendars
    jday0 = None
    calendar = calendar.lower()
    if calendar not in _calendars:
        raise ValueError("calendar must be one of %s, got '%s'" % (str(_calendars), calendar))

    units, tzoffset, origin = dateparse(unit_string)
    if origin.year == 0:
        raise ValueError('zero not allowed as a reference year, does not exist in Julian or Gregorian calendars')
    elif origin.year < 0:
        raise ValueError('negative reference year in time units, must be >= 1')

    jd0 = JulianDayFromDate(origin, calendar=calendar)
    return jd0, units, tzoffset, origin 
#get_origin_num

