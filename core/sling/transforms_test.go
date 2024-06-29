package sling

import (
	"testing"

	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestTransformMsUUID(t *testing.T) {
	uuidBytes := []byte{0x78, 0x56, 0x34, 0x12, 0x34, 0x12, 0x34, 0x12, 0x12, 0x34, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc}
	sp := iop.NewStreamProcessor()
	val, _ := ParseMsUUID(sp, cast.ToString(uuidBytes))
	assert.Equal(t, "12345678-1234-1234-1234-123456789abc", val)
}
