package connection

import (
	"os"
	"testing"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

func TestConnection(t *testing.T) {
	m := g.M(
		"url", os.Getenv("POSTGRES_URL"),
	)
	c1, err := NewConnection("POSTGRES", "postgres", m)
	assert.NoError(t, err)
	_ = c1

	m = g.M(
		"username", "postgres",
		"password", "postgres",
		"host", "bionic.larco.us",
		"port", 55432,
		"database", "postgres",
		"sslmode", "disable",
	)
	c2, err := NewConnection("POSTGRES", "postgres", m)
	assert.NoError(t, err)
	_ = c2

	_, err = NewConnection("Db", "someother", m)
	assert.NoError(t, err)

	_, err = NewConnectionFromURL("Db", os.Getenv("POSTGRES_URL"))
	assert.NoError(t, err)
}

func TestQueryURL(t *testing.T) {
	password := "<JuIQ){cXpV{<)nB+4DrNX;LC+0dx;+Vl4hk^!{M(+R.66Y<}"
	// wrong := "%3CJuIQ%29%7BcXpV%7B%3C%29nB+4DrNX;LC+0dx;+Vl4hk%5E%21%7BM%28+R.66Y%3C%7D"
	// correct := "%3CJuIQ%29%7BcXpV%7B%3C%29nB%2B4DrNX%3BLC%2B0dx%3B%2BVl4hk%5E%21%7BM%28%2BR.66Y%3C%7D"
	// correct := "%3CJuIQ%29%7BcXpV%7B%3C%29nB%2B4DrNX%3BLC%2B0dx%3B%2BVl4hk%5E%21%7BM%28%2BR.66Y%3C%7D"
	// println(url.QueryEscape(password))
	_ = password
}
