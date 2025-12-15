package database

import (
	"github.com/slingdata-io/sling-cli/core/dbio"
)

// ArrowFlightConn is an Arrow FlightSQL connection
type ArrowFlightConn struct {
	BaseConn
	URL string
}

// Init initiates the connection
func (conn *ArrowFlightConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbArrowDBC
	conn.BaseConn.defaultPort = 12345

	// exasol driver is auto-registered on import

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}
