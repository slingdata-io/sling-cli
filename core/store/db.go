package store

import (
	"os"

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
