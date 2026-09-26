package store

import (
	"os"
	"strings"
	"time"

	"github.com/denisbrodbeck/machineid"
	"github.com/flarco/g"
	"github.com/jmoiron/sqlx"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/env"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	// Db is the main databse connection
	Db   *gorm.DB
	Dbx  *sqlx.DB
	Conn database.Connection

	// DropAll signifies to drop all tables and recreate them
	DropAll = false
)

// InitDB initializes the database
func InitDB() {
	var err error

	if Db != nil {
		// already initiated
		return
	}

	dbURL := g.F("sqlite://%s/.sling.db?cache=shared&mode=rwc&_journal_mode=WAL", env.HomeDir)
	Conn, err = database.NewConn(dbURL, "silent=true")
	if err != nil {
		g.Debug("could not initialize local .sling.db. %s", err.Error())
		return
	}

	Db, err = Conn.GetGormConn(&gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		g.Debug("could not connect to local .sling.db. %s", err.Error())
		return
	}

	allTables := []interface{}{
		&Setting{},
		&QueryHistory{},
	}

	for _, table := range allTables {
		dryDB := Db.Session(&gorm.Session{DryRun: true})
		tableName := dryDB.Find(table).Statement.Table
		if DropAll {
			Db.Exec(g.F(`drop table if exists "%s"`, tableName))
		}
		err = Db.AutoMigrate(table)
		if err != nil {
			g.Debug("error AutoMigrating table for local .sling.db. => %s\n%s", tableName, err.Error())
			return
		}
	}

	// settings
	settings()
}

type Setting struct {
	Key   string `json:"key" gorm:"primaryKey"`
	Value string `json:"value"`
}

func settings() {
	// ProtectedID returns a hashed version of the machine ID in a cryptographically secure way,
	// using a fixed, application-specific key.
	// Internally, this function calculates HMAC-SHA256 of the application ID, keyed by the machine ID.
	machineID, _ := machineid.ProtectedID("sling")
	if machineID == "" {
		// generate random id then
		machineID = "m." + g.RandString(g.AlphaRunesLower+g.NumericRunes, 62)
	}

	Db.Create(&Setting{"machine-id", machineID})
	os.Setenv("MACHINE_ID", machineID)
}

func GetMachineID() string {
	if Db == nil {
		machineID, _ := machineid.ProtectedID("sling")
		return machineID
	}
	s := Setting{Key: "machine-id"}
	Db.First(&s)
	return s.Value
}

// QueryHistory stores executed query history entries
type QueryHistory struct {
	ID           uint      `json:"id" gorm:"primaryKey;autoIncrement"`
	WorkspaceKey *string   `json:"workspace_key" gorm:"index"`
	Connection   string    `json:"connection"`
	Query        string    `json:"query"`
	Status       string    `json:"status"`
	DurationMs   int64     `json:"duration_ms"`
	RowCount     int       `json:"row_count"`
	ErrorMessage string    `json:"error_message,omitempty"`
	CreatedAt    time.Time `json:"created_at" gorm:"autoCreateTime;index"`
}

// SaveQueryHistory persists a query history entry
func SaveQueryHistory(entry *QueryHistory) error {
	if Db == nil {
		return nil
	}
	return Db.Create(entry).Error
}

// QueryHistoryFilter narrows a query history page. The empty fields match
// everything.
type QueryHistoryFilter struct {
	WorkspaceKey *string
	Connection   string // case-insensitive equal
	Search       string // case-insensitive substring of query
	Status       string // optional
	Limit        int
	Offset       int
}

// GetQueryHistoryFiltered returns a page of query history and the total count
// after the filters. The filters apply in SQL, before Count, LIMIT and OFFSET,
// so a search sees every stored row, not only the current page.
func GetQueryHistoryFiltered(f QueryHistoryFilter) (entries []QueryHistory, total int64, err error) {
	if Db == nil {
		return nil, 0, nil
	}

	q := Db.Model(&QueryHistory{})
	if f.WorkspaceKey == nil {
		q = q.Where("workspace_key IS NULL")
	} else {
		q = q.Where("workspace_key = ?", *f.WorkspaceKey)
	}
	if f.Connection != "" {
		q = q.Where("lower(connection) = lower(?)", f.Connection)
	}
	if f.Search != "" {
		// Each term is its own filter: "users select" and "select users" both
		// need every term somewhere in the query.
		for _, term := range strings.Fields(strings.ToLower(f.Search)) {
			q = q.Where("lower(query) LIKE ?", "%"+term+"%")
		}
	}
	if f.Status != "" {
		q = q.Where("status = ?", f.Status)
	}

	if err = q.Count(&total).Error; err != nil {
		return nil, 0, g.Error(err, "could not count query history")
	}

	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}

	if err = q.Order("created_at DESC").Limit(limit).Offset(f.Offset).Find(&entries).Error; err != nil {
		return nil, 0, g.Error(err, "could not get query history")
	}

	return entries, total, nil
}

// GetQueryHistory retrieves query history entries filtered by workspace key
func GetQueryHistory(workspaceKey *string, limit, offset int) (entries []QueryHistory, total int64, err error) {
	return GetQueryHistoryFiltered(QueryHistoryFilter{WorkspaceKey: workspaceKey, Limit: limit, Offset: offset})
}
