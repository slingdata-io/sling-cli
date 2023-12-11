package store

import (
	"github.com/flarco/dbio/database"
	"github.com/flarco/g"
	"github.com/jmoiron/sqlx"
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

	dbURL := g.F("sqlite://%s/.sling.db?cache=shared&mode=rwc&_journal_mode=WAL", env.HomeDir)
	Conn, err = database.NewConn(dbURL)
	if err != nil {
		g.Debug("could not initialize local .sling.db. %s", err.Error())
		return
	}

	g.LogError(err, "Could not initialize sqlite connection: %s", dbURL)

	Db, err = Conn.GetGormConn(&gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	g.LogError(err, "Could not connect to sqlite database: %s", dbURL)

	allTables := []interface{}{
		&Execution{},
		&Task{},
		&Replication{},
	}

	for _, table := range allTables {
		dryDB := Db.Session(&gorm.Session{DryRun: true})
		tableName := dryDB.Find(table).Statement.Table
		if DropAll {
			Db.Exec(g.F(`drop table if exists "%s"`, tableName))
		}
		g.Debug("Creating table: " + tableName)
		err = Db.AutoMigrate(table)
		g.LogError(err, "error AutoMigrating table: "+tableName)
	}
}
