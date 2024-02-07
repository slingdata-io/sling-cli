// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package database

import (
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

func TestBatchInsertClickhouse(t *testing.T) {
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("100.110.2.70:9000")},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "admin",
			Password: "dElta123!",
		},
		Settings: g.M(
			"allow_experimental_map_type", "1",
			// "allow_experimental_lightweight_delete", "true",
		),
		DialTimeout: 5 * time.Second,
		// Compression: compression,
		// TLS:         tlsConfig,
		// Protocol:    protocol,
	})

	conn.SetMaxIdleConns(5)

	if _, err := conn.Exec(`DROP TABLE IF EXISTS example`); err != nil {
		assert.NoError(t, err)
		return
	}
	_, err := conn.Exec(`
		CREATE TABLE IF NOT EXISTS example (
			  Col1 UInt8
			, Col2 String
			, Col3 FixedString(3)
			, Col4 UUID
			, Col5 Map(String, UInt8)
			, Col6 Array(String)
			, Col7 Tuple(String, UInt8, Array(Map(String, String)))
			, Col8 DateTime
		) Engine = Memory
	`)
	if err != nil {
		assert.NoError(t, err)
		return
	}
	scope, err := conn.Begin()
	if err != nil {
		assert.NoError(t, err)
		return
	}
	batch, err := scope.Prepare("INSERT INTO example")
	if err != nil {
		assert.NoError(t, err)
		return
	}
	for i := 0; i < 1000; i++ {
		_, err := batch.Exec(
			uint8(42),
			"ClickHouse", "Inc",
			uuid.New(),
			map[string]uint8{"key": 1},             // Map(String, UInt8)
			[]string{"Q", "W", "E", "R", "T", "Y"}, // Array(String)
			[]interface{}{ // Tuple(String, UInt8, Array(Map(String, String)))
				"String Value", uint8(5), []map[string]string{
					{"key": "value"},
					{"key": "value"},
					{"key": "value"},
				},
			},
			time.Now(),
		)
		if err != nil {
			assert.NoError(t, err)
			return
		}
	}
	err = scope.Commit()
	assert.NoError(t, err)

	rows, err := conn.Query(`
		select count(*) cnt, sum(Col1) total from example
	`)
	assert.NoError(t, err)

	rows.Next()
	var cnt, total int
	rows.Scan(&cnt, &total)
	g.Info("count: %d, total %d", cnt, total)
}
