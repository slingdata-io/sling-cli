package iop

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPartitionLevel_IsValid(t *testing.T) {
	tests := []struct {
		name  string
		level PartitionLevel
		want  bool
	}{
		{"valid minute", PartitionLevelMinute, true},
		{"valid hour", PartitionLevelHour, true},
		{"valid day", PartitionLevelDay, true},
		{"valid week", PartitionLevelWeek, true},
		{"valid month", PartitionLevelMonth, true},
		{"valid year_month", PartitionLevelYearMonth, true},
		{"valid year", PartitionLevelYear, true},
		{"invalid level", "invalid", false},
		{"empty level", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.level.IsValid()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPartitionLevel_TruncateTime(t *testing.T) {
	loc := time.UTC
	baseTime := time.Date(2024, 3, 15, 14, 30, 45, 123, loc)

	tests := []struct {
		name    string
		level   PartitionLevel
		input   time.Time
		want    time.Time
		wantErr bool
	}{
		{
			name:  "minute truncation",
			level: PartitionLevelMinute,
			input: baseTime,
			want:  time.Date(2024, 3, 15, 14, 30, 0, 0, loc),
		},
		{
			name:  "hour truncation",
			level: PartitionLevelHour,
			input: baseTime,
			want:  time.Date(2024, 3, 15, 14, 0, 0, 0, loc),
		},
		{
			name:  "day truncation",
			level: PartitionLevelDay,
			input: baseTime,
			want:  time.Date(2024, 3, 15, 0, 0, 0, 0, loc),
		},
		{
			name:  "week truncation",
			level: PartitionLevelWeek,
			input: baseTime,                                // March 15, 2024 is a Friday
			want:  time.Date(2024, 3, 11, 0, 0, 0, 0, loc), // Should truncate to Monday
		},
		{
			name:  "month truncation",
			level: PartitionLevelMonth,
			input: baseTime,
			want:  time.Date(2024, 3, 1, 0, 0, 0, 0, loc),
		},
		{
			name:  "year_month truncation",
			level: PartitionLevelYearMonth,
			input: baseTime,
			want:  time.Date(2024, 3, 1, 0, 0, 0, 0, loc),
		},
		{
			name:  "year truncation",
			level: PartitionLevelYear,
			input: baseTime,
			want:  time.Date(2024, 1, 1, 0, 0, 0, 0, loc),
		},
		{
			name:    "invalid level",
			level:   "invalid",
			input:   baseTime,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.level.TruncateTime(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPartitionExtractFields(t *testing.T) {
	tests := []struct {
		name string
		path string
		want []PartitionLevel
	}{
		{
			name: "single partition",
			path: "/data/{part_year}/file.csv",
			want: []PartitionLevel{PartitionLevelYear},
		},
		{
			name: "multiple partitions",
			path: "/data/{part_year}/{part_month}/{part_day}/file.csv",
			want: []PartitionLevel{PartitionLevelYear, PartitionLevelMonth, PartitionLevelDay},
		},
		{
			name: "mixed content",
			path: "/data/{part_year}/static/{part_month}/file.csv",
			want: []PartitionLevel{PartitionLevelYear, PartitionLevelMonth},
		},
		{
			name: "invalid partition",
			path: "/data/{part_invalid}/file.csv",
			want: []PartitionLevel{},
		},
		{
			name: "no partitions",
			path: "/data/static/file.csv",
			want: []PartitionLevel{},
		},
		{
			name: "all partition levels",
			path: "/data/{part_year}/{part_year_month}/{part_month}/{part_week}/{part_day}/{part_hour}/{part_minute}/file.csv",
			want: []PartitionLevel{
				PartitionLevelYear,
				PartitionLevelYearMonth,
				PartitionLevelMonth,
				PartitionLevelWeek,
				PartitionLevelDay,
				PartitionLevelHour,
				PartitionLevelMinute,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractPartitionFields(tt.path)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExtractPartitionTimeValue(t *testing.T) {
	tests := []struct {
		name     string
		mask     string
		path     string
		want     time.Time
		wantErr  bool
		errMatch string
	}{
		{
			name: "basic date format",
			mask: "data/{YYYY}/{MM}/{DD}",
			path: "data/2024/03/21",
			want: time.Date(2024, 3, 21, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "partition format year_month",
			mask: "data/{part_year_month}/files",
			path: "data/2024-03/files",
			want: time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "partition format with prefix",
			mask: "data/{part_year}/files",
			path: "data/created_dt_year=2024/files",
			want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "mixed format",
			mask: "data/{YYYY}/{part_month}/{DD}",
			path: "data/2024/03/21",
			want: time.Date(2024, 3, 21, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "with time components",
			mask: "data/{YYYY}/{MM}/{DD}/{HH}/{mm}",
			path: "data/2024/03/21/15/30",
			want: time.Date(2024, 3, 21, 15, 30, 0, 0, time.UTC),
		},
		{
			name: "with time components 2",
			mask: "data/y{YYYY}-m{MM}/{DD}/{HH}/{mm}",
			path: "data/y2024-m03/21/15/30",
			want: time.Date(2024, 3, 21, 15, 30, 0, 0, time.UTC),
		},
		{
			name: "week partition",
			mask: "data/{YYYY}/{part_week}",
			path: "data/2024/12",
			want: time.Date(2024, 3, 18, 0, 0, 0, 0, time.UTC), // Week 12 of 2024
		},
		{
			name: "week partition 2",
			mask: "data/y{YYYY}/{part_week}",
			path: "data/y2024/12",
			want: time.Date(2024, 3, 18, 0, 0, 0, 0, time.UTC), // Week 12 of 2024
		},
		{
			name:     "mismatched segments",
			mask:     "data/{YYYY}/{MM}",
			path:     "data/2024",
			wantErr:  true,
			errMatch: "different number of segments",
		},
		{
			name:     "invalid year_month format",
			mask:     "data/{part_year_month}",
			path:     "data/2024",
			wantErr:  true,
			errMatch: "invalid year_month format",
		},
		{
			name:     "invalid week without year",
			mask:     "data/{part_week}",
			path:     "data/12",
			wantErr:  true,
			errMatch: "missing year to parse week",
		},
		{
			name:     "static path mismatch",
			mask:     "data/static/{YYYY}",
			path:     "data/wrong/2024",
			wantErr:  true,
			errMatch: "static path segment mismatch",
		},
		{
			name: "with month name format",
			mask: "data/{YYYY}/{MMM}/{DD}",
			path: "data/2024/Mar/21",
			want: time.Date(2024, 3, 21, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "with lowercase month name",
			mask: "data/{YYYY}/{MMM}/{DD}",
			path: "data/2024/mar/21",
			want: time.Date(2024, 3, 21, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "with invalid month name",
			mask:     "data/{YYYY}/{MMM}/{DD}",
			path:     "data/2024/Invalid/21",
			wantErr:  true,
			errMatch: "invalid month name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractPartitionTimeValue(tt.mask, tt.path)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetLowestPartTimeUnit(t *testing.T) {
	tests := []struct {
		name     string
		mask     string
		want     time.Duration
		wantErr  bool
		errMatch string
	}{
		{
			name: "minute partition",
			mask: "data/{part_minute}/file.csv",
			want: time.Minute,
		},
		{
			name: "hour partition",
			mask: "data/{part_hour}/file.csv",
			want: time.Hour,
		},
		{
			name: "day partition",
			mask: "data/{part_day}/file.csv",
			want: 24 * time.Hour,
		},
		{
			name: "week partition",
			mask: "data/{part_week}/file.csv",
			want: 7 * 24 * time.Hour,
		},
		{
			name: "month partition",
			mask: "data/{part_month}/file.csv",
			want: 31 * 24 * time.Hour,
		},
		{
			name: "year_month partition",
			mask: "data/{part_year_month}/file.csv",
			want: 31 * 24 * time.Hour,
		},
		{
			name: "year partition",
			mask: "data/{part_year}/file.csv",
			want: 365 * 24 * time.Hour,
		},
		{
			name: "multiple partitions - lowest wins",
			mask: "data/{part_year}/{part_month}/{part_day}/file.csv",
			want: 24 * time.Hour,
		},
		{
			name: "time format second",
			mask: "data/{YYYY}/{MM}/{DD}/{ss}",
			want: time.Second,
		},
		{
			name: "time format minute",
			mask: "data/{YYYY}/{MM}/{DD}/{mm}",
			want: time.Minute,
		},
		{
			name: "time format hour",
			mask: "data/{YYYY}/{MM}/{DD}/{HH}",
			want: time.Hour,
		},
		{
			name: "mixed formats - lowest wins",
			mask: "data/{part_year}/{MM}/{part_minute}",
			want: time.Minute,
		},
		{
			name:     "no time units",
			mask:     "data/static/file.csv",
			wantErr:  true,
			errMatch: "no valid time unit found in mask",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetLowestPartTimeUnit(tt.mask)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMatchedPartitionMask(t *testing.T) {
	tests := []struct {
		name    string
		mask    string
		path    string
		want    bool
		wantErr bool
	}{
		{
			name: "basic date format match",
			mask: "data/{YYYY}/{MM}/{DD}",
			path: "data/2024/03/21",
			want: true,
		},
		{
			name: "partition format match",
			mask: "data/{part_year}/{part_month}",
			path: "data/2024/03",
			want: true,
		},
		{
			name: "partition format with prefix match",
			mask: "data/{part_year}/files",
			path: "data/created_dt_year=2024/files",
			want: true,
		},
		{
			name: "mixed format match",
			mask: "data/{YYYY}/{part_month}/{DD}",
			path: "data/2024/03/21",
			want: true,
		},
		{
			name: "with time components match",
			mask: "data/{YYYY}/{MM}/{DD}/{HH}/{mm}",
			path: "data/2024/03/21/15/30",
			want: true,
		},
		{
			name: "with static segments match",
			mask: "data/static/{YYYY}/{MM}",
			path: "data/static/2024/03",
			want: true,
		},
		{
			name: "mismatched segments",
			mask: "data/{YYYY}/{MM}",
			path: "data/2024",
			want: false,
		},
		{
			name: "static path mismatch",
			mask: "data/static/{YYYY}",
			path: "data/wrong/2024",
			want: false,
		},
		{
			name: "complex pattern match",
			mask: "data/{part_year}/{part_month}/logs_{YYYY}-{MM}-{DD}_{HH}",
			path: "data/2024/03/logs_2024-03-21_15",
			want: true,
		},
		{
			name: "all partition levels match",
			mask: "data/{part_year}/{part_month}/{part_day}/{part_hour}/{part_minute}",
			path: "data/2024/03/21/15/30",
			want: true,
		},
		{
			name: "invalid format in path",
			mask: "data/{YYYY}/{MM}",
			path: "data/invalid/13",
			want: true, // Note: The function only checks structure, not value validity
		},
		{
			name: "empty segments",
			mask: "data///{YYYY}",
			path: "data///2024",
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchedPartitionMask(tt.mask, tt.path)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGeneratePartURIsFromRange(t *testing.T) {
	tests := []struct {
		name      string
		mask      string
		updateKey string
		start     time.Time
		end       time.Time
		want      []string
		wantErr   bool
	}{
		{
			name:      "year partitions",
			mask:      "data/{part_year}/files",
			updateKey: "created_dt",
			start:     time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
			end:       time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			want: []string{
				"data/created_dt_year=2022/files",
				"data/created_dt_year=2023/files",
				"data/created_dt_year=2024/files",
			},
		},
		{
			name:      "month partitions",
			mask:      "data/{part_month}/files",
			updateKey: "created_dt",
			start:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:       time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
			want: []string{
				"data/created_dt_month=01/files",
				"data/created_dt_month=02/files",
				"data/created_dt_month=03/files",
			},
		},
		{
			name:      "day partitions",
			mask:      "data/{part_day}/files",
			updateKey: "created_dt",
			start:     time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
			end:       time.Date(2024, 3, 3, 0, 0, 0, 0, time.UTC),
			want: []string{
				"data/created_dt_day=01/files",
				"data/created_dt_day=02/files",
				"data/created_dt_day=03/files",
			},
		},
		{
			name:      "hour partitions",
			mask:      "data/{part_hour}/files",
			updateKey: "created_dt",
			start:     time.Date(2024, 3, 1, 22, 0, 0, 0, time.UTC),
			end:       time.Date(2024, 3, 2, 0, 0, 0, 0, time.UTC),
			want: []string{
				"data/created_dt_hour=22/files",
				"data/created_dt_hour=23/files",
				"data/created_dt_hour=00/files",
			},
		},
		{
			name:      "minute partitions",
			mask:      "data/{part_minute}/files",
			updateKey: "created_dt",
			start:     time.Date(2024, 3, 1, 0, 58, 0, 0, time.UTC),
			end:       time.Date(2024, 3, 1, 1, 0, 0, 0, time.UTC),
			want: []string{
				"data/created_dt_minute=58/files",
				"data/created_dt_minute=59/files",
				"data/created_dt_minute=00/files",
			},
		},
		{
			name:      "complex nested partitions",
			mask:      "data/{part_year}/{part_month}/{part_day}/files",
			updateKey: "created_dt",
			start:     time.Date(2024, 3, 31, 0, 0, 0, 0, time.UTC),
			end:       time.Date(2024, 4, 2, 0, 0, 0, 0, time.UTC),
			want: []string{
				"data/created_dt_year=2024/created_dt_month=03/created_dt_day=31/files",
				"data/created_dt_year=2024/created_dt_month=04/created_dt_day=01/files",
				"data/created_dt_year=2024/created_dt_month=04/created_dt_day=02/files",
			},
		},
		{
			name:      "year_month partitions",
			mask:      "data/{part_year_month}/files",
			updateKey: "created_dt",
			start:     time.Date(2023, 11, 1, 0, 0, 0, 0, time.UTC),
			end:       time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			want: []string{
				"data/created_dt_year_month=2023-11/files",
				"data/created_dt_year_month=2023-12/files",
				"data/created_dt_year_month=2024-01/files",
			},
		},
		{
			name:      "mixed ISO8601 and partition format",
			mask:      "data/{YYYY}/{part_month}/dt={DD}/files",
			updateKey: "created_dt",
			start:     time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
			end:       time.Date(2024, 3, 3, 0, 0, 0, 0, time.UTC),
			want: []string{
				"data/2024/created_dt_month=03/dt=01/files",
				"data/2024/created_dt_month=03/dt=02/files",
				"data/2024/created_dt_month=03/dt=03/files",
			},
		},
		{
			name:      "invalid partition level",
			mask:      "data/{part_invalid}/files",
			updateKey: "created_dt",
			start:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:       time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			wantErr:   true,
		},
		{
			name:      "end before start",
			mask:      "data/{part_day}/files",
			updateKey: "created_dt",
			start:     time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			end:       time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GeneratePartURIsFromRange(tt.mask, tt.updateKey, tt.start, tt.end)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
