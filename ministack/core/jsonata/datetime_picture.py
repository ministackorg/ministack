"""XPath 3.1 picture strings for timestamps: `format-dateTime` and `parse-dateTime`.

Ports jsonata-js `datetime.js:478-1307`. Dates are handled with proleptic-Gregorian day arithmetic rather than
`datetime`, because JSONata accepts years outside `datetime`'s 1..9999 range and because `Date.UTC` normalises
out-of-range months and days instead of rejecting them. The `timezone` argument is a display offset, never an
IANA zone lookup, so a plain UTC calendar plus a millisecond shift reproduces it exactly.
"""

import math
import re
import time

from .number_picture import (
    LOWER,
    TITLE,
    UPPER,
    analyse_integer_picture,
    format_integer,
    integer_matcher,
    js_parse_int,
    render_integer,
)
from .values import JsonataError

DAY_NAMES = ["", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
MONTH_NAMES = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
               "November", "December"]

MILLIS_IN_A_DAY = 86400000

ISO_8601_PICTURE = "[Y0001]-[M01]-[D01]T[H01]:[m01]:[s01].[f001][Z01:01t]"

DEFAULT_PRESENTATION = {"Y": "1", "M": "1", "D": "1", "d": "1", "F": "n", "W": "1", "w": "1", "X": "1", "x": "1",
                        "H": "1", "h": "1", "P": "n", "m": "01", "s": "01", "f": "1", "Z": "01:01", "z": "01:01",
                        "C": "n", "E": "n"}

NUMERIC_COMPONENTS = "YMDdFWwXxHhmsf"
PRESENTATION2_CHARS = "atco"

# The ISO 8601 shape `$toMillis` accepts when no picture is given (datetime.js:1310).
ISO_8601_REGEX = re.compile(r"^\d{4}(-[01]\d)?(-[0-3]\d)?(T[0-2]\d:[0-5]\d:[0-5]\d)?(\.\d+)?"
                            r"([+-][0-2]\d:?[0-5]\d|Z)?$")


# --- Proleptic Gregorian calendar -----------------------------------------------------------------------------


def _days_from_civil(year, month, day):
    """Days since 1970-01-01 for a 1-based month. Howard Hinnant's algorithm; valid for any year."""
    year -= 1 if month <= 2 else 0
    era = year // 400
    year_of_era = year - era * 400
    day_of_year = (153 * (month + (-3 if month > 2 else 9)) + 2) // 5 + day - 1
    day_of_era = year_of_era * 365 + year_of_era // 4 - year_of_era // 100 + day_of_year
    return era * 146097 + day_of_era - 719468


def _civil_from_days(days):
    days += 719468
    era = days // 146097
    day_of_era = days - era * 146097
    year_of_era = (day_of_era - day_of_era // 1460 + day_of_era // 36524 - day_of_era // 146096) // 365
    year = year_of_era + era * 400
    day_of_year = day_of_era - (365 * year_of_era + year_of_era // 4 - year_of_era // 100)
    month_index = (5 * day_of_year + 2) // 153
    day = day_of_year - (153 * month_index + 2) // 5 + 1
    month = month_index + (3 if month_index < 10 else -9)
    return year + (1 if month <= 2 else 0), month, day


def date_utc(year, month, day=1, hour=0, minute=0, second=0, millis=0, shift_two_digit_year=True):
    """`Date.UTC`, including its 1900 offset for two-digit years and its normalisation of out-of-range fields.

    `shift_two_digit_year=False` reproduces `Date.parse` instead, which the no-picture `$toMillis` path
    uses: a literal four-digit year in an ISO 8601 string is never shifted, unlike a year built field by
    field through `Date.UTC`.
    """
    # A picture component parsed through a digit family, a roman numeral or a word list that doesn't
    # recognise the text (D3132-adjacent parse paths) yields NaN rather than a usable field; propagate
    # it as a NaN result instead of letting `int()` crash.
    if not all(math.isfinite(component) for component in (year, month, day, hour, minute, second, millis)):
        return float("nan")
    year, month, day = int(year), int(month), int(day)
    hour, minute, second, millis = int(hour), int(minute), int(second), int(millis)
    if shift_two_digit_year and 0 <= year <= 99:
        year += 1900
    year += month // 12
    month = month % 12
    days = _days_from_civil(year, month + 1, 1) + (day - 1)
    return ((days * 24 + hour) * 60 + minute) * 60000 + second * 1000 + millis


class UtcMoment:
    """The UTC calendar fields of an epoch-millisecond instant, mirroring the `getUTC*` accessors."""

    def __init__(self, millis):
        # A NaN/Infinity instant reaches here from a picture-parse component that didn't resolve to a
        # real value (see date_utc); every field below propagates it through ordinary float arithmetic
        # instead of the `int()` cast crashing outright, matching the reference's own NaN-laced moment.
        self.millis = int(millis) if math.isfinite(millis) else millis
        days, time_of_day = divmod(self.millis, MILLIS_IN_A_DAY)
        self.year, self.month_number, self.day = _civil_from_days(days)
        self.weekday = (days + 4) % 7
        self.hour, remainder = divmod(time_of_day, 3600000)
        self.minute, remainder = divmod(remainder, 60000)
        self.second, self.millisecond = divmod(remainder, 1000)

    @property
    def month_index(self):
        """Zero-based month, the form `Date.UTC` and `getUTCMonth` speak."""
        return self.month_number - 1


def _start_of_first_week(year, month_index):
    """ISO 8601: week 1 is the week containing the first Thursday. F&O extends the same rule to a month."""
    first = date_utc(year, month_index)
    weekday = UtcMoment(first).weekday or 7
    if weekday > 4:
        return first + (8 - weekday) * MILLIS_IN_A_DAY
    return first - (weekday - 1) * MILLIS_IN_A_DAY


def _delta_weeks(start, end):
    return (end - start) / (MILLIS_IN_A_DAY * 7) + 1


def _week_in_year(moment):
    start_of_week1 = _start_of_first_week(moment.year, 0)
    today = date_utc(moment.year, moment.month_index, moment.day)
    week = _delta_weeks(start_of_week1, today)
    if week > 52:
        if today >= _start_of_first_week(moment.year + 1, 0):
            week = 1
    elif week < 1:
        week = _delta_weeks(_start_of_first_week(moment.year - 1, 0), today)
    return math.floor(week)


def _week_in_month(moment):
    start_of_week1 = _start_of_first_week(moment.year, moment.month_index)
    today = date_utc(moment.year, moment.month_index, moment.day)
    week = _delta_weeks(start_of_week1, today)
    if week > 4:
        if today >= _start_of_first_week(moment.year, moment.month_index + 1):
            week = 1
    elif week < 1:
        week = _delta_weeks(_start_of_first_week(moment.year, moment.month_index - 1), today)
    return math.floor(week)


def _iso_week_year(moment):
    """1 Jan 2005 belongs to week 53 of 2004; `Y` says 2005, so JSONata adds `X` to say 2004."""
    start = _start_of_first_week(moment.year, 0)
    end = _start_of_first_week(moment.year + 1, 0)
    if moment.millis < start:
        return moment.year - 1
    if moment.millis >= end:
        return moment.year + 1
    return moment.year


def _iso_week_month(moment):
    start = _start_of_first_week(moment.year, moment.month_index)
    end = _start_of_first_week(moment.year, moment.month_index + 1)
    if moment.millis < start:
        return (moment.month_index - 1) % 12 + 1
    if moment.millis >= end:
        return (moment.month_index + 1) % 12 + 1
    return moment.month_number


def _day_in_year(moment):
    today = date_utc(moment.year, moment.month_index, moment.day)
    return (today - date_utc(moment.year, 0)) // MILLIS_IN_A_DAY + 1


COMPONENT_READERS = {
    "Y": lambda moment: moment.year,
    "M": lambda moment: moment.month_number,
    "D": lambda moment: moment.day,
    "d": _day_in_year,
    "F": lambda moment: moment.weekday or 7,
    "W": _week_in_year,
    "w": _week_in_month,
    "X": _iso_week_year,
    "x": _iso_week_month,
    "H": lambda moment: moment.hour,
    "h": lambda moment: moment.hour % 12 or 12,
    "P": lambda moment: "pm" if moment.hour >= 12 else "am",
    "m": lambda moment: moment.minute,
    "s": lambda moment: moment.second,
    "f": lambda moment: moment.millisecond,
    "C": lambda moment: "ISO",
    "E": lambda moment: "ISO",
}


def component_value(moment, component):
    """`getDateTimeFragment` (datetime.js:687-823). `Z`/`z` have no value here: the instant is always UTC."""
    reader = COMPONENT_READERS.get(component)
    return reader(moment) if reader else None


# --- Picture analysis -----------------------------------------------------------------------------------------


class LiteralPart:
    """A run of picture text outside any `[...]` marker."""

    def __init__(self, text):
        self.text = text


class MarkerPart:
    """One `[component[presentation][,width]]` marker of a date/time picture."""

    def __init__(self, component):
        self.component = component
        self.presentation1 = None
        self.presentation2 = None
        self.names = None
        self.ordinal = False
        self.width_min = None
        self.width_max = None
        self.has_width = False
        self.integer_format = None
        self.year_digits = -1


class DateTimePicture:
    """An analysed `format-dateTime` picture: literal runs and markers in the order they render."""

    def __init__(self, parts):
        self.parts = parts


def _parse_width_bound(text):
    if text is None or text == "*":
        return None
    return js_parse_int(text)


def _read_width(marker, comma_at, definition):
    width = marker[comma_at + 1:]
    dash = width.find("-")
    if dash == -1:
        low, high = width, None
    else:
        low, high = width[:dash], width[dash + 1:]
    definition.has_width = True
    definition.width_min = _parse_width_bound(low)
    definition.width_max = _parse_width_bound(high)
    return marker[1:comma_at]


def _read_presentation(presentation, definition):
    """A component outside `DEFAULT_PRESENTATION` is unknown regardless of whether it carries its own
    presentation modifier: without this check it slips through analysis, `component_value` reads it as
    `None`, and `format_datetime` fails later with a bare `TypeError` from joining that `None` into text."""
    if definition.component not in DEFAULT_PRESENTATION:
        raise JsonataError("D3132", value=definition.component)

    if len(presentation) == 1:
        definition.presentation1 = presentation
    elif len(presentation) > 1 and presentation[-1] in PRESENTATION2_CHARS:
        definition.presentation2 = presentation[-1]
        definition.ordinal = presentation[-1] == "o"
        definition.presentation1 = presentation[:-1]
    elif len(presentation) > 1:
        definition.presentation1 = presentation
    else:
        definition.presentation1 = DEFAULT_PRESENTATION.get(definition.component)


def _read_names_case(definition):
    lead = definition.presentation1[0]
    if lead == "n":
        definition.names = LOWER
    elif lead == "N":
        definition.names = TITLE if definition.presentation1[1:2] == "n" else UPPER


def _attach_integer_format(definition, previous_part):
    pattern = definition.presentation1
    if definition.presentation2:
        pattern += ";" + definition.presentation2
    definition.integer_format = analyse_integer_picture(pattern)
    if definition.width_min is not None and (definition.integer_format.mandatory_digits or 0) < definition.width_min:
        definition.integer_format.mandatory_digits = definition.width_min
    if definition.component == "Y":
        _apply_year_width(definition)
    # Two adjacent numeric markers need a fixed width, otherwise parsing cannot tell where the first one ends.
    if isinstance(previous_part, MarkerPart) and previous_part.integer_format is not None:
        previous_part.integer_format.parse_width = previous_part.integer_format.mandatory_digits


def _apply_year_width(definition):
    """F&O 9.8.4.4: the picture's digit count is also the modulus applied to the year."""
    definition.year_digits = -1
    if definition.width_max is not None:
        definition.year_digits = definition.width_max
        definition.integer_format.mandatory_digits = definition.width_max
        return
    total = (definition.integer_format.mandatory_digits or 0) + definition.integer_format.optional_digits
    if total >= 2:
        definition.year_digits = total


def _build_marker(marker, previous_part):
    definition = MarkerPart(marker[0:1])
    comma_at = marker.rfind(",")
    presentation = _read_width(marker, comma_at, definition) if comma_at != -1 else marker[1:]
    _read_presentation(presentation, definition)
    _read_names_case(definition)

    if definition.names is None and definition.component in NUMERIC_COMPONENTS:
        _attach_integer_format(definition, previous_part)
    if definition.component in ("Z", "z"):
        definition.integer_format = analyse_integer_picture(definition.presentation1)
    return definition


def analyse_datetime_picture(picture):
    """`analyseDateTimePicture` (datetime.js:489-644). `[[` and `]]` are the escapes; there is no backslash form."""
    parts = []

    def add_literal(start, end):
        if end > start:
            parts.append(LiteralPart(picture[start:end].replace("]]", "]")))

    start = 0
    pos = 0
    while pos < len(picture):
        if picture[pos] != "[":
            pos += 1
            continue
        if picture[pos + 1:pos + 2] == "[":
            add_literal(start, pos)
            parts.append(LiteralPart("["))
            pos += 2
            start = pos
            continue
        add_literal(start, pos)
        start = pos
        pos = picture.find("]", start)
        if pos == -1:
            raise JsonataError("D3135")
        marker = re.sub(r"\s+", "", picture[start + 1:pos])
        parts.append(_build_marker(marker, parts[-1] if parts else None))
        start = pos + 1
        pos += 1
    add_literal(start, pos)
    return DateTimePicture(parts)


# --- Formatting -----------------------------------------------------------------------------------------------


def _split_timezone(timezone):
    """The offset argument is read as `±hhmm`; `fmod` keeps the minutes signed the way JS `%` does."""
    if timezone is None:
        return 0, 0
    offset = js_parse_int(timezone)
    if offset != offset:
        return 0, 0
    return math.floor(offset / 100), int(math.fmod(offset, 100))


def _format_name(value, marker):
    if marker.component in ("M", "x"):
        name = MONTH_NAMES[value - 1]
    elif marker.component == "F":
        name = DAY_NAMES[value]
    else:
        raise JsonataError("D3133", value=marker.component)
    if marker.names == UPPER:
        name = name.upper()
    elif marker.names == LOWER:
        name = name.lower()
    if marker.width_max is not None and len(name) > marker.width_max:
        name = name[:marker.width_max]
    return name


def _format_timezone(marker, offset_hours, offset_minutes):
    offset = offset_hours * 100 + offset_minutes
    if marker.integer_format.regular:
        rendered = render_integer(offset, marker.integer_format)
    else:
        digits = marker.integer_format.mandatory_digits
        if digits in (1, 2):
            rendered = render_integer(offset_hours, marker.integer_format)
            if offset_minutes != 0:
                rendered += ":" + format_integer(offset_minutes, "00")
        elif digits in (3, 4):
            rendered = render_integer(offset, marker.integer_format)
        else:
            raise JsonataError("D3134", value=digits)
    if offset >= 0:
        rendered = "+" + rendered
    if marker.component == "z":
        rendered = "GMT" + rendered
    if offset == 0 and marker.presentation2 == "t":
        rendered = "Z"
    return rendered


def _format_marker(moment, marker, offset_hours, offset_minutes):
    value = component_value(moment, marker.component)
    if marker.component in NUMERIC_COMPONENTS:
        if marker.component == "Y" and marker.year_digits != -1:
            value = value % 10 ** marker.year_digits
        if marker.names:
            return _format_name(value, marker)
        return render_integer(value, marker.integer_format)
    if marker.component in ("Z", "z"):
        return _format_timezone(marker, offset_hours, offset_minutes)
    if marker.component == "P" and marker.names == UPPER:
        return value.upper()
    return value


def format_datetime(millis, picture=None, timezone=None):
    """`$fromMillis`. `timezone` shifts the instant before rendering; no picture means the ISO 8601 default."""
    if not math.isfinite(millis):
        raise JsonataError("D1001", value=millis)
    offset_hours, offset_minutes = _split_timezone(timezone)
    analysed = analyse_datetime_picture(ISO_8601_PICTURE if picture is None else picture)
    moment = UtcMoment(millis + (60 * offset_hours + offset_minutes) * 60000)

    rendered = []
    for part in analysed.parts:
        if isinstance(part, LiteralPart):
            rendered.append(part.text)
        else:
            rendered.append(_format_marker(moment, part, offset_hours, offset_minutes))
    return "".join(rendered)


# --- Parsing --------------------------------------------------------------------------------------------------


class MatchPart:
    """The regex one picture part contributes, and how a captured group becomes a component value."""

    def __init__(self, component, regex, parse=None):
        self.component = component
        self.regex = regex
        self.parse = parse


def _timezone_match_part(marker):
    separator = marker.integer_format.grouping_separators if marker.integer_format.regular else None
    regex = "GMT" if marker.component == "z" else ""
    regex += "[-+][0-9]+"
    if separator:
        regex += re.escape(separator[1]) + "[0-9]+"

    def parse(text):
        if marker.component == "z":
            text = text[3:]
        if separator:
            at = text.find(separator[1])
            return js_parse_int(text[:at]) * 60 + js_parse_int(text[at + 1:])
        if len(text) - 1 <= 2:
            return js_parse_int(text) * 60
        return js_parse_int(text[:3]) * 60 + js_parse_int(text[3:])

    return MatchPart(marker.component, regex, parse)


def _truncated(name, width_max):
    # A malformed width specifier (e.g. "[FNn,abc]") parses to NaN; JS's `if (width && width.max)`
    # treats NaN as falsy and leaves the name untruncated, but Python treats NaN as truthy, so the
    # naive port would try `name[:nan]` and crash. Mirror the reference: ignore the malformed width.
    if not width_max or math.isnan(width_max):
        return name
    return name[:width_max]


def _month_name_lookup(marker):
    return {_truncated(name, marker.width_max): index + 1 for index, name in enumerate(MONTH_NAMES)}


def _weekday_name_lookup(marker):
    return {_truncated(name, marker.width_max): index for index, name in enumerate(DAY_NAMES) if index > 0}


def _meridiem_name_lookup():
    return {"am": 0, "AM": 0, "pm": 1, "PM": 1}


def _name_lookup(marker):
    if marker.component in ("M", "x"):
        return _month_name_lookup(marker)
    if marker.component == "F":
        return _weekday_name_lookup(marker)
    if marker.component == "P":
        return _meridiem_name_lookup()
    raise JsonataError("D3133", value=marker.component)


def _marker_match_part(marker):
    if marker.component in ("Z", "z"):
        return _timezone_match_part(marker)
    if marker.component == "f":
        return MatchPart("f", "[0-9]+", lambda text: float("0." + text[:3]) * 1000)
    if marker.integer_format is not None:
        matcher = integer_matcher(marker.integer_format)
        return MatchPart(marker.component, matcher.regex, matcher.parse)
    lookup = _name_lookup(marker)
    return MatchPart(marker.component, "[a-zA-Z]+", lookup.get)


def datetime_matcher(analysed):
    """`generateRegex`'s datetime branch (datetime.js:956-1043)."""
    parts = []
    for part in analysed.parts:
        if isinstance(part, LiteralPart):
            parts.append(MatchPart(None, re.escape(part.text)))
        else:
            parts.append(_marker_match_part(part))
    return parts


# Which components each recognised date/time shape may draw on, as a bitmask over the letters below.
DATE_LETTERS = "YXMxWwdD"
TIME_LETTERS = "PHhmsf"
DATE_YMD = 0b10100001
DATE_YEAR_DAY = 0b10000010
DATE_ISO_WEEK_MONTH = 0b01010100
DATE_ISO_WEEK_YEAR = 0b01001000
TIME_24_HOUR = 0b010111
TIME_12_HOUR = 0b101111


def _mask_of(components, letters):
    mask = 0
    for letter in letters:
        mask = (mask << 1) + (1 if components.get(letter) else 0)
    return mask


def _matches_shape(shape, mask):
    """Every captured component must belong to the shape, and at least one must be present."""
    return (mask & ~shape) == 0 and (mask & shape) != 0


def _select_components(components):
    date_mask = _mask_of(components, DATE_LETTERS)
    year_month_day = _matches_shape(DATE_YMD, date_mask)
    year_day = not year_month_day and _matches_shape(DATE_YEAR_DAY, date_mask)
    iso_week_month = _matches_shape(DATE_ISO_WEEK_MONTH, date_mask)
    iso_week_year = not iso_week_month and _matches_shape(DATE_ISO_WEEK_YEAR, date_mask)

    time_mask = _mask_of(components, TIME_LETTERS)
    twenty_four_hour = _matches_shape(TIME_24_HOUR, time_mask)
    twelve_hour = not twenty_four_hour and _matches_shape(TIME_12_HOUR, time_mask)

    if year_day:
        date_letters = "YD"
    elif iso_week_month:
        date_letters = "XxwF"
    elif iso_week_year:
        date_letters = "XWF"
    else:
        date_letters = "YMD"
    time_letters = "Phmsf" if twelve_hour else "Hmsf"
    return date_letters + time_letters, year_day, iso_week_month or iso_week_year, twelve_hour


def _fill_defaults(components, letters, now):
    """Above the most significant captured field, default from now; below it, default to zero. A gap is D3136."""
    started = False
    ended = False
    for letter in letters:
        if components.get(letter) is None:
            if started:
                components[letter] = 1 if letter in "MDd" else 0
                ended = True
            else:
                components[letter] = component_value(now, letter)
        else:
            started = True
            if ended:
                raise JsonataError("D3136")


def _resolve_year_day(components):
    """A day-of-year date is converted by walking forward from 1 January of the captured year."""
    derived = UtcMoment(date_utc(components["Y"], 0) + (components["d"] - 1) * MILLIS_IN_A_DAY)
    components["M"] = derived.month_index
    components["D"] = derived.day


def _resolve_twelve_hour(components):
    components["H"] = 0 if components["h"] == 12 else components["h"]
    if components.get("P") == 1:
        components["H"] += 12


def parse_datetime(text, picture=None, now_millis=None):
    """`$toMillis`. Unspecified high-order components come from `now_millis` (defaults to today)."""
    if picture is None:
        return parse_iso_8601(text)
    analysed = analyse_datetime_picture(picture)
    parts = datetime_matcher(analysed)
    full_regex = "^" + "".join("(" + part.regex + ")" for part in parts) + "$"
    match = re.match(full_regex, text, re.IGNORECASE)
    if match is None:
        return None

    components = {}
    for index, part in enumerate(parts):
        if part.parse is not None:
            components[part.component] = part.parse(match.group(index + 1))
    if not components:
        return None

    letters, year_day, iso_week, twelve_hour = _select_components(components)
    now = UtcMoment(now_millis if now_millis is not None else _now_millis())
    _fill_defaults(components, letters, now)

    month = components.get("M")
    components["M"] = month - 1 if month is not None and month > 0 else 0
    if year_day:
        _resolve_year_day(components)
    if iso_week:
        # The ISO week-date shapes are formatting-only in JSONata; there is no inverse.
        raise JsonataError("D3136")
    if twelve_hour:
        _resolve_twelve_hour(components)

    millis = date_utc(components["Y"], components["M"], components["D"], components["H"], components["m"],
                      components["s"], components["f"])
    offset = components.get("Z") or components.get("z")
    if offset:
        millis -= offset * 60000
    return millis


def _now_millis():
    return int(time.time() * 1000)


def parse_iso_8601(text):
    """`$toMillis` without a picture: a fixed ISO 8601 shape, not the picture engine."""
    if not ISO_8601_REGEX.match(text):
        raise JsonataError("D3110", value=text)
    return _date_parse_iso(text)


def _date_parse_iso(text):
    match = re.match(r"^(\d{4})(?:-(\d{2}))?(?:-(\d{2}))?"
                     r"(?:T(\d{2}):(\d{2}):(\d{2}))?(\.\d+)?"
                     r"([+-]\d{2}:?\d{2}|Z)?$", text)
    year, month, day = int(match.group(1)), int(match.group(2) or 1), int(match.group(3) or 1)
    hour, minute, second = int(match.group(4) or 0), int(match.group(5) or 0), int(match.group(6) or 0)
    millis = int(round(float(match.group(7) or 0) * 1000))
    # A date-only ISO string is UTC; a date-time without a zone designator is local time, which here is UTC too.
    total = date_utc(year, month - 1, day, hour, minute, second, millis, shift_two_digit_year=False)
    zone = match.group(8)
    if zone and zone != "Z":
        sign = -1 if zone[0] == "-" else 1
        digits = zone[1:].replace(":", "")
        total -= sign * (int(digits[:2]) * 60 + int(digits[2:])) * 60000
    return total
