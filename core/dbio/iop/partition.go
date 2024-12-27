package iop

import (
	"regexp"
	"strings"
	"time"

	"github.com/flarco/g"
)

type PartitionLevel string

const (
	PartitionLevelMinute    PartitionLevel = "minute"
	PartitionLevelHour      PartitionLevel = "hour"
	PartitionLevelDay       PartitionLevel = "day"
	PartitionLevelWeek      PartitionLevel = "week"
	PartitionLevelYearMonth PartitionLevel = "year_month"
	PartitionLevelMonth     PartitionLevel = "month"
	PartitionLevelYear      PartitionLevel = "year"
)

var PartitionLevelsAscending = []PartitionLevel{
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
