package local

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCloneRepo(t *testing.T) {
	home, err := GetHome()
	assert.NoError(t, err)

	// clone
	path, err := home.CloneRepo("https://github.com/flarco/gonkey")
	assert.NoError(t, err)

	files, err := ioutil.ReadDir(path)
	assert.NoError(t, err)

	assert.Len(t, files, 10)

	// pull
	path, err = home.CloneRepo("https://github.com/flarco/gonkey")
	assert.NoError(t, err)

	os.RemoveAll(path)
}
