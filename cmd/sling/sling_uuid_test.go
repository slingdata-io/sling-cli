package main

import (
	"testing"
	"unsafe"

	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/stretchr/testify/assert"
)

func TestMsUUID(t *testing.T) {
	uuidBytes := []uint8{0x78, 0x56, 0x34, 0x12, 0x34, 0x12, 0x34, 0x12, 0x12, 0x34, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc}
	sp := iop.NewStreamProcessor()
	val, _ := sling.ParseMsUUID(sp, *(*string)(unsafe.Pointer(&uuidBytes)))
	assert.Equal(t, "12345678-1234-1234-1234-123456789abc", val)
}
