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
