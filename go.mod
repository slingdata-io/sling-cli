module github.com/flarco/sling

go 1.16

require (
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/c-bata/go-prompt v0.2.6
	github.com/dustin/go-humanize v1.0.0
	github.com/flarco/dbio v0.2.3
	github.com/flarco/g v0.0.7
	github.com/getsentry/sentry-go v0.11.0
	github.com/go-openapi/strfmt v0.19.6 // indirect
	github.com/integrii/flaggy v1.4.4
	github.com/jedib0t/go-pretty v4.3.0+incompatible
	github.com/jmespath/go-jmespath v0.4.0
	github.com/json-iterator/go v1.1.11 // indirect
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0
	github.com/mitchellh/mapstructure v1.4.1 // indirect
	github.com/rs/zerolog v1.20.0
	github.com/rudderlabs/analytics-go v3.3.1+incompatible
	github.com/segmentio/backo-go v0.0.0-20200129164019-23eae7c10bd3 // indirect
	github.com/spf13/cast v1.3.1
	github.com/stretchr/testify v1.7.0
	github.com/tidwall/gjson v1.8.0 // indirect
	github.com/xtgo/uuid v0.0.0-20140804021211-a0b114877d4c // indirect
	google.golang.org/api v0.44.0 // indirect
	google.golang.org/genproto v0.0.0-20210602131652-f16073e35f0c // indirect
	gopkg.in/cheggaaa/pb.v2 v2.0.7
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace github.com/flarco/dbio => ../dbio

replace github.com/flarco/g => ../g
