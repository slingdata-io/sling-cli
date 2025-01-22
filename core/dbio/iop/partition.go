package iop

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/spf13/cast"
)

type PartitionLevel string

const (
	PartitionLevelSecond    PartitionLevel = "second"
	PartitionLevelMinute    PartitionLevel = "minute"
	PartitionLevelHour      PartitionLevel = "hour"
	PartitionLevelDay       PartitionLevel = "day"
	PartitionLevelWeek      PartitionLevel = "week"
	PartitionLevelYearMonth PartitionLevel = "year_month"
	PartitionLevelMonth     PartitionLevel = "month"
	PartitionLevelYear      PartitionLevel = "year"
)

var PartitionLevelsAscending = []PartitionLevel{
	PartitionLevelSecond,
	PartitionLevelMinute,
	PartitionLevelHour,
	PartitionLevelDay,
	PartitionLevelWeek,
	PartitionLevelMonth,
	PartitionLevelYearMonth,
	PartitionLevelYear,
}
var PartitionLevelsDescending = []PartitionLevel{
	PartitionLevelYear,
	PartitionLevelYearMonth,
	PartitionLevelMonth,
	PartitionLevelWeek,
	PartitionLevelDay,
	PartitionLevelHour,
	PartitionLevelMinute,
	PartitionLevelSecond,
}

func (level PartitionLevel) Str() string {
	return string(level)
}

func (level PartitionLevel) IsValid() bool {
	for _, l := range PartitionLevelsDescending {
		if level == l {
			return true
		}
	}
	return false
}

func (level PartitionLevel) TruncateTime(t time.Time) (time.Time, error) {
	switch level {
	case PartitionLevelYear:
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()), nil
	case PartitionLevelMonth, PartitionLevelYearMonth:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()), nil
	case PartitionLevelWeek:
		// Calculate the number of days to subtract to get to Monday
		daysToMonday := int(t.Weekday() - time.Monday)
		if daysToMonday < 0 {
			daysToMonday += 7 // Adjust for Sunday
		}
		// Subtract the days to get to Monday
		monday := t.AddDate(0, 0, -daysToMonday)
		return time.Date(monday.Year(), monday.Month(), monday.Day(), 0, 0, 0, 0, t.Location()), nil
	case PartitionLevelDay:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()), nil
	case PartitionLevelHour:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()), nil
	case PartitionLevelMinute:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location()), nil
	case PartitionLevelSecond:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, t.Location()), nil
	default:
		return t, g.Error("invalid partition level: %s", level)
	}
}

// ExtractPartitionFields extract the partition fields from the given path
func ExtractPartitionFields(path string) (levels []PartitionLevel) {
	// Regex pattern to match {part_*} fields
	pattern := regexp.MustCompile(`{(part_[^}]+)}`)

	// Find all matches in the path
	matches := pattern.FindAllStringSubmatch(path, -1)

	// Extract the captured groups (without braces)
	levels = make([]PartitionLevel, 0, len(matches))
	for _, match := range matches {
		if len(match) > 1 {
			expression := strings.ReplaceAll(strings.ReplaceAll(match[1], "{", ""), "}", "")
			expression = strings.TrimPrefix(strings.TrimSpace(expression), "part_")
			partLevel := PartitionLevel(expression)
			// ensure if matches
			matched := false
			for _, l := range PartitionLevelsDescending {
				if partLevel == l {
					levels = append(levels, partLevel)
					matched = true
					break
				}
			}
			if !matched {
				g.Warn("invalid partition field: %s", match[1])
			}
		}
	}

	return levels
}

// GetLowestPartTimeUnit loops though possible TimeLevel or PartitionLevel values
// and returns the lowest time.Duration unit
func GetLowestPartTimeUnit(mask string) (time.Duration, error) {
	var timeUnit, timeLevelUnit, partLevelUnit time.Duration

	// Check TimeLevel fields
	for _, level := range TimeLevelAscending {
		if strings.Contains(mask, g.F("{%s}", level)) {
			switch level {
			case TimeLevelSecond:
				timeLevelUnit = time.Second
			case TimeLevelMinute:
				timeLevelUnit = time.Minute
			case TimeLevelHour12, TimeLevelHour24:
				timeLevelUnit = time.Hour
			case TimeLevelDay, TimeLevelDayOfYear:
				timeLevelUnit = time.Hour * 24
			case TimeLevelMonth, TimeLevelMonthName:
				timeLevelUnit = time.Hour * 24 * 31
			case TimeLevelYear, TimeLevelYearShort:
				timeLevelUnit = time.Hour * 24 * 365
			}
		}
		if timeLevelUnit > 0 {
			break
		}
	}

	// Check PartitionLevel fields
	for _, level := range PartitionLevelsAscending {
		if strings.Contains(mask, g.F("{part_%s}", level)) {
			switch level {
			case PartitionLevelSecond:
				partLevelUnit = time.Second
			case PartitionLevelMinute:
				partLevelUnit = time.Minute
			case PartitionLevelHour:
				partLevelUnit = time.Hour
			case PartitionLevelDay:
				partLevelUnit = time.Hour * 24
			case PartitionLevelWeek:
				partLevelUnit = time.Hour * 24 * 7
			case PartitionLevelMonth, PartitionLevelYearMonth:
				partLevelUnit = time.Hour * 24 * 31
			case PartitionLevelYear:
				partLevelUnit = time.Hour * 24 * 365
			}
		}
		if partLevelUnit > 0 {
			break
		}
	}

	// determine lower unit
	if timeLevelUnit > 0 {
		timeUnit = timeLevelUnit
	}
	if partLevelUnit > 0 {
		if timeUnit == 0 || partLevelUnit < timeUnit {
			timeUnit = partLevelUnit
		}
	}

	if timeUnit == 0 {
		return timeUnit, g.Error("no valid time unit found in mask")
	}

	return timeUnit, nil
}

// MatchedPartitionMask determines if the mask and the path have the same
// partition structure
func MatchedPartitionMask(mask, path string) (matches bool) {
	// Split both strings into parts
	maskParts := strings.Split(strings.Trim(mask, "/"), "/")
	pathParts := strings.Split(strings.Trim(path, "/"), "/")

	if len(maskParts) != len(pathParts) {
		return false
	}

	// Add regex patterns to match template variables
	timeTemplatePattern := regexp.MustCompile(`{(YYYY|YY|MMM|MM|DD|DDD|HH|hh|mm|ss)}`)
	partTemplatePattern := regexp.MustCompile(`{part_(minute|hour|day|week|year_month|month|year)}`)

	// Compare each part
	for i, maskPart := range maskParts {
		pathPart := pathParts[i]

		// Handle static parts (no templates)
		if !strings.Contains(maskPart, "{") {
			if maskPart != pathPart {
				return false
			}
			continue
		}

		// Extract all template matches
		timeMatches := timeTemplatePattern.FindAllString(maskPart, -1)
		partMatches := partTemplatePattern.FindAllString(maskPart, -1)

		// Create pattern to match the actual value
		extractPattern := maskPart
		extractPattern = regexp.QuoteMeta(extractPattern)

		// Replace template placeholders with capture groups
		for _, match := range timeMatches {
			placeholder := regexp.QuoteMeta(match)
			extractPattern = strings.Replace(extractPattern, placeholder, `(\d+|[A-Za-z]+)`, 1)
		}
		for _, match := range partMatches {
			placeholder := regexp.QuoteMeta(match)
			extractPattern = strings.Replace(extractPattern, placeholder, `([^/]+)`, 1)
		}

		// Check if the path part matches the pattern
		valuePattern := regexp.MustCompile("^" + extractPattern + "$")
		if !valuePattern.MatchString(pathPart) {
			return false
		}
	}

	return true
}

// ExtractPartitionTimeValue extracts from partition time value from the path
// with mask `data/{YYYY}/{MM}/{DD}` and path `data/2024/12/21`, the returned timestamp should be 2024-12-21.
func ExtractPartitionTimeValue(mask, path string) (timestamp time.Time, err error) {
	// Split both strings into parts
	maskParts := strings.Split(strings.Trim(mask, "/"), "/")
	pathParts := strings.Split(strings.Trim(path, "/"), "/")

	if len(maskParts) != len(pathParts) {
		return timestamp, g.Error("mask and path have different number of segments")
	}

	// Initialize time components with defaults
	timeComponents := map[TimeLevel]int{
		TimeLevelYear:      0, // Year
		TimeLevelYearShort: 0, // Year
		TimeLevelMonth:     1, // Month
		TimeLevelMonthName: 1, // Month
		TimeLevelDayOfYear: 1, // Day
		TimeLevelDay:       1, // Day
		TimeLevelHour12:    0, // Hour
		TimeLevelHour24:    0, // Hour
		TimeLevelMinute:    0, // Minute
	}

	// Add regex pattern to match template variables
	timeTemplatePattern := regexp.MustCompile(`{(YYYY|YY|MMM|MM|DD|DDD|HH|hh|mm|ss)}`)
	partTemplatePattern := regexp.MustCompile(`{part_(minute|hour|day|week|year_month|month|year)}`)

	// Compare each part and extract time components
	var weekNum int
	for i, maskPart := range maskParts {
		pathPart := pathParts[i]

		// Handle static parts (no templates)
		if !strings.Contains(maskPart, "{") {
			if maskPart != pathPart {
				return timestamp, g.Error("static path segment mismatch: expected %s, got %s", maskPart, pathPart)
			}
			continue
		}

		// Extract all time template matches
		timeMatches := timeTemplatePattern.FindAllStringSubmatch(maskPart, -1)
		partMatches := partTemplatePattern.FindAllStringSubmatch(maskPart, -1)

		if len(timeMatches) == 0 && len(partMatches) == 0 {
			continue // No recognized templates in this part
		}

		// Create pattern to extract values
		extractPattern := maskPart
		extractPattern = regexp.QuoteMeta(extractPattern)

		// Replace template placeholders with capture groups
		for _, match := range timeMatches {
			placeholder := regexp.QuoteMeta(match[0])
			extractPattern = strings.Replace(extractPattern, placeholder, `(\d+|[A-Za-z]+)`, 1)
		}
		for _, match := range partMatches {
			placeholder := regexp.QuoteMeta(match[0])
			extractPattern = strings.Replace(extractPattern, placeholder, `([^/]+)`, 1)
		}

		// Extract values using the constructed pattern
		valuePattern := regexp.MustCompile("^" + extractPattern + "$")
		valueMatches := valuePattern.FindStringSubmatch(pathPart)

		if valueMatches == nil {
			return timestamp, g.Error("failed to match path part pattern: %s with %s", extractPattern, pathPart)
		}

		// Process time template matches
		valueIndex := 1
		for _, match := range timeMatches {
			tTemplate := TimeLevel(match[1])
			if _, exists := timeComponents[tTemplate]; !exists {
				continue
			}

			value := valueMatches[valueIndex]
			valueIndex++

			if tTemplate == TimeLevelMonthName {
				// Handle month name parsing
				monthNum := 0
				for m := 1; m <= 12; m++ {
					if strings.EqualFold(value, time.Month(m).String()[:3]) {
						monthNum = m
						timeComponents[TimeLevelMonth] = monthNum
						break
					}
				}
				if monthNum == 0 {
					return timestamp, g.Error("invalid month name: %s", value)
				}
			} else {
				// Parse numeric value
				numValue, err := strconv.Atoi(value)
				if err != nil {
					return timestamp, g.Error("failed to parse time component %s: %v", tTemplate, err)
				}
				timeComponents[tTemplate] = numValue
			}
		}

		// Process partition template matches
		for _, match := range partMatches {
			pcTemplate := PartitionLevel(match[1])
			value := valueMatches[valueIndex]
			valueIndex++

			// Clean value if it has a prefix
			if valueParts := strings.Split(value, "="); len(valueParts) == 2 {
				value = valueParts[len(valueParts)-1]
			}

			switch pcTemplate {
			case PartitionLevelSecond:
				timeComponents[TimeLevelSecond], err = cast.ToIntE(value)
			case PartitionLevelMinute:
				timeComponents[TimeLevelMinute], err = cast.ToIntE(value)
			case PartitionLevelHour:
				timeComponents[TimeLevelHour24], err = cast.ToIntE(value)
			case PartitionLevelDay:
				timeComponents[TimeLevelDay], err = cast.ToIntE(value)
			case PartitionLevelWeek:
				weekNum, err = cast.ToIntE(value)
				if err != nil {
					err = g.Error("invalid week format: %s", value)
				}
			case PartitionLevelYearMonth:
				parts := strings.Split(value, "-")
				if len(parts) != 2 {
					err = g.Error("invalid year_month format: %s", value)
					break
				}
				timeComponents[TimeLevelYear], err = cast.ToIntE(parts[0])
				if err != nil {
					break
				}
				timeComponents[TimeLevelMonth], err = cast.ToIntE(parts[1])
			case PartitionLevelMonth:
				timeComponents[TimeLevelMonth], err = cast.ToIntE(value)
			case PartitionLevelYear:
				timeComponents[TimeLevelYear], err = cast.ToIntE(value)
			}

			if err != nil {
				return timestamp, g.Error(err, "failed to parse partition component %s", pcTemplate)
			}
		}
	}

	// set week if present
	if weekNum > 0 {
		if timeComponents[TimeLevelYear] == 0 {
			return timestamp, g.Error("missing year to parse week number component %d", weekNum)
		}

		// Find the first day of the year
		firstDay := time.Date(timeComponents[TimeLevelYear], 1, 1, 0, 0, 0, 0, time.UTC)
		// Find the first Monday of the year
		firstMonday := firstDay
		for firstMonday.Weekday() != time.Monday {
			firstMonday = firstMonday.AddDate(0, 0, 1)
		}
		// Add the weeks
		timestamp := firstMonday.AddDate(0, 0, (weekNum-1)*7)
		timeComponents[TimeLevelMonth] = int(timestamp.Month())
		timeComponents[TimeLevelDay] = timestamp.Day()
	}

	// Create time.Time from components
	timestamp = time.Date(
		timeComponents[TimeLevelYear],
		time.Month(timeComponents[TimeLevelMonth]),
		timeComponents[TimeLevelDay],
		timeComponents[TimeLevelHour24],
		timeComponents[TimeLevelMinute],
		0, 0, time.UTC,
	)

	return timestamp, nil
}

func GetPartitionDateMap(partKeyPrefix string, timestamp time.Time) map[string]any {
	pdm := map[string]any{}
	partKeyPrefix = strings.Trim(strings.ToLower(partKeyPrefix), "\"'`[] ")

	for _, pl := range PartitionLevelsDescending {
		value := ""
		switch pl {
		case PartitionLevelYear:
			value = timestamp.Format("2006")
		case PartitionLevelMonth:
			value = timestamp.Format("01")
		case PartitionLevelYearMonth:
			value = timestamp.Format("2006-01")
		case PartitionLevelWeek:
			_, week := timestamp.ISOWeek()
			value = g.F("%02d", week)
		case PartitionLevelDay:
			value = timestamp.Format("02")
		case PartitionLevelHour:
			value = timestamp.Format("15")
		case PartitionLevelMinute:
			value = timestamp.Format("04")
		case PartitionLevelSecond:
			value = timestamp.Format("05")
		default:
			g.Warn("invalid partition level for GetPartitionDateMap: %s", pl)
			continue
		}

		pdmKey := g.F("part_%s", pl)
		pdm[pdmKey] = g.F("%s_%s=%s", partKeyPrefix, pl, value)
	}
	return pdm
}

type TimeLevel string

const (
	TimeLevelYear      TimeLevel = "YYYY"
	TimeLevelYearShort TimeLevel = "YY"
	TimeLevelMonthName TimeLevel = "MMM"
	TimeLevelMonth     TimeLevel = "MM"
	TimeLevelDay       TimeLevel = "DD"
	TimeLevelDayOfYear TimeLevel = "DDD"
	TimeLevelHour24    TimeLevel = "HH"
	TimeLevelHour12    TimeLevel = "hh"
	TimeLevelMinute    TimeLevel = "mm"
	TimeLevelSecond    TimeLevel = "ss"
)

var TimeLevelDescending = []TimeLevel{
	TimeLevelYear,
	TimeLevelYearShort,
	TimeLevelMonthName,
	TimeLevelMonth,
	TimeLevelDay,
	TimeLevelDayOfYear,
	TimeLevelHour24,
	TimeLevelHour12,
	TimeLevelMinute,
	TimeLevelSecond,
}

var TimeLevelAscending = []TimeLevel{
	TimeLevelSecond,
	TimeLevelMinute,
	TimeLevelHour12,
	TimeLevelHour24,
	TimeLevelDayOfYear,
	TimeLevelDay,
	TimeLevelMonth,
	TimeLevelMonthName,
	TimeLevelYearShort,
	TimeLevelYear,
}

func (tl TimeLevel) AsPartitionLevel() PartitionLevel {
	switch tl {
	case TimeLevelYear, TimeLevelYearShort:
		return PartitionLevelYear
	case TimeLevelMonth, TimeLevelMonthName:
		return PartitionLevelMonth
	case TimeLevelDay, TimeLevelDayOfYear:
		return PartitionLevelDay
	case TimeLevelHour24, TimeLevelHour12:
		return PartitionLevelHour
	case TimeLevelMinute:
		return PartitionLevelMinute
	case TimeLevelSecond:
		return PartitionLevelSecond
	}

	return PartitionLevel("")
}

// ExtractISO8601DateFields return a list of found date fields
func ExtractISO8601DateFields(path string) (list []TimeLevel) {
	for _, v := range TimeLevelDescending {
		if strings.Contains(path, g.F("{%s}", v)) {
			list = append(list, v)
		}
	}
	return
}

// GetISO8601DateMap return a map of date parts for string formatting
func GetISO8601DateMap(t time.Time) map[string]any {
	m := map[string]any{}
	for _, v := range TimeLevelDescending {
		m[string(v)] = t.Format(Iso8601ToGoLayout(string(v)))
	}
	return m
}

// https://www.w3.org/QA/Tips/iso-date
// https://www.w3.org/TR/NOTE-datetime
// https://www.iso.org/iso-8601-date-and-time-format.html
func Iso8601ToGoLayout(dateFormat string) (goDateFormat string) {
	goDateFormat = strings.TrimSpace(dateFormat)
	goDateFormat = strings.ReplaceAll(goDateFormat, "TZD", "-07:00")
	goDateFormat = strings.ReplaceAll(goDateFormat, "YYYY", "2006")
	goDateFormat = strings.ReplaceAll(goDateFormat, "YY", "06")
	goDateFormat = strings.ReplaceAll(goDateFormat, "MMM", "Jan")
	goDateFormat = strings.ReplaceAll(goDateFormat, "MM", "01")
	goDateFormat = strings.ReplaceAll(goDateFormat, "DD", "02")
	goDateFormat = strings.ReplaceAll(goDateFormat, "DDD", "Mon")
	goDateFormat = strings.ReplaceAll(goDateFormat, "HH", "15")
	goDateFormat = strings.ReplaceAll(goDateFormat, "hh", "03")
	goDateFormat = strings.ReplaceAll(goDateFormat, "mm", "04")
	goDateFormat = strings.ReplaceAll(goDateFormat, ".ss", ".000")
	goDateFormat = strings.ReplaceAll(goDateFormat, "ss", "05")
	goDateFormat = strings.ReplaceAll(goDateFormat, ".s", ".000")
	goDateFormat = strings.ReplaceAll(goDateFormat, "ISO8601", "2006-01-02T15:04:05Z")

	goDateFormat = regexp.MustCompile(`Z\d\d:?\d\d$`).ReplaceAllString(goDateFormat, "Z0700")
	goDateFormat = regexp.MustCompile(`-\d\d:?\d\d$`).ReplaceAllString(goDateFormat, "-0700")
	goDateFormat = regexp.MustCompile(`\+\d\d:?\d\d$`).ReplaceAllString(goDateFormat, "+0700")

	return
}

func GetLowestPartTimeLevel(url string) (PartitionLevel, error) {
	var pLevelL, tLevelL PartitionLevel
	for _, level := range TimeLevelAscending {
		if strings.Contains(url, g.F("{%s}", level)) {
			tLevelL = level.AsPartitionLevel()
			break
		}
	}

	for _, level := range PartitionLevelsAscending {
		if strings.Contains(url, g.F("{part_%s}", level)) {
			pLevelL = level
			break
		}
	}

	for _, level := range PartitionLevelsAscending {
		// return the first matching ascending
		if level == pLevelL || level == tLevelL {
			return level, nil
		}
	}

	return "", g.Error("did not find a partition level in path")
}

// GeneratePartURIsFromRange generates all the possible URIs
// given a start/end range. Must first determine the lowest time resolution
func GeneratePartURIsFromRange(mask, updateKey string, start, end time.Time) (uris []string, err error) {
	if end.Before(start) {
		return uris, g.Error("range end (%s) is before start (%s)", end, start)
	}

	level, err := GetLowestPartTimeLevel(mask)
	if err != nil {
		return nil, g.Error(err, "could not get lowest partition level")
	}

	// Initialize empty slice for URIs
	uris = make([]string, 0)

	// Get the time unit for incrementing
	timeUnit, err := GetLowestPartTimeUnit(mask)
	if err != nil {
		return nil, g.Error(err, "could not get time unit")
	}

	// Special handling for month level since it's not a fixed duration
	isMonthLevel := level == PartitionLevelMonth || level == PartitionLevelYearMonth

	// Start from truncated time
	current, err := level.TruncateTime(start)
	if err != nil {
		return nil, g.Error(err, "could not truncate start time")
	}

	// Generate URIs for each time increment
	for current.Before(end) || current.Equal(end) {
		// For each timestamp, generate the URI by replacing all time format patterns
		uri := mask

		// Replace time format patterns
		uri = g.Rm(uri, GetISO8601DateMap(current))
		uri = g.Rm(uri, GetPartitionDateMap(updateKey, current))

		uris = append(uris, uri)

		// Increment time based on level
		if isMonthLevel {
			current = current.AddDate(0, 1, 0)
		} else {
			current = current.Add(timeUnit)
		}
	}

	return uris, nil
}
