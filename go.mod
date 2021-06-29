module github.com/slingdata-io/sling

go 1.16

require (
	github.com/dustin/go-humanize v1.0.0
	github.com/flarco/dbio v0.0.3
	github.com/flarco/g v0.0.2
	github.com/getsentry/sentry-go v0.11.0
	github.com/go-openapi/strfmt v0.19.6 // indirect
	github.com/integrii/flaggy v1.4.4
	github.com/jedib0t/go-pretty v4.3.0+incompatible
	github.com/jmespath/go-jmespath v0.4.0
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/rs/zerolog v1.20.0
	github.com/rudderlabs/analytics-go v3.3.1+incompatible // indirect
	github.com/segmentio/backo-go v0.0.0-20200129164019-23eae7c10bd3 // indirect
	github.com/spf13/cast v1.3.1
	github.com/stretchr/testify v1.7.0
	github.com/tidwall/gjson v1.8.0 // indirect
	github.com/xtgo/uuid v0.0.0-20140804021211-a0b114877d4c // indirect
	gopkg.in/cheggaaa/pb.v2 v2.0.7
	gopkg.in/yaml.v2 v2.3.0
)

replace github.com/flarco/dbio => ../dbio

replace github.com/flarco/g => ../g
