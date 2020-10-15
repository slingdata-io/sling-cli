module github.com/slingdata-io/sling

go 1.15

require (
	cloud.google.com/go v0.66.0
	cloud.google.com/go/bigquery v1.8.0
	cloud.google.com/go/storage v1.12.0
	github.com/360EntSecGroup-Skylar/excelize v1.4.1
	github.com/Azure/azure-sdk-for-go v47.0.0+incompatible
	github.com/Azure/azure-storage-blob-go v0.10.0
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/PuerkitoBio/goquery v1.6.0
	github.com/aws/aws-sdk-go v1.35.7
	github.com/cheggaaa/pb v2.0.7+incompatible
	github.com/denisenkom/go-mssqldb v0.0.0-20200910202707-1e08a3fab204
	github.com/digitalocean/godo v1.47.0
	github.com/dnaeon/go-vcr v1.1.0 // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/flarco/gutil v0.0.0
	github.com/go-ini/ini v1.62.0 // indirect
	github.com/go-openapi/strfmt v0.19.6 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/godror/godror v0.20.4
	github.com/integrii/flaggy v1.4.4
	github.com/jedib0t/go-pretty v4.3.0+incompatible
	github.com/jmespath/go-jmespath v0.4.0
	github.com/jmoiron/sqlx v1.2.0
	github.com/json-iterator/go v1.1.10
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0
	github.com/lib/pq v1.3.0
	github.com/markbates/pkger v0.17.1
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/mattn/go-sqlite3 v1.11.0
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/pkg/sftp v1.12.0
	github.com/rs/zerolog v1.20.0
	github.com/shirou/gopsutil v2.20.8+incompatible
	github.com/snowflakedb/gosnowflake v1.3.9
	github.com/solcates/go-sql-bigquery v0.2.4
	github.com/spf13/cast v1.3.1
	github.com/stretchr/testify v1.6.1
	github.com/xitongsys/parquet-go v1.5.3
	github.com/xitongsys/parquet-go-source v0.0.0-20201014235637-c24a23d9ef1e
	github.com/xo/dburl v0.0.0-20200910011426-652e0d5720a3
	golang.org/x/crypto v0.0.0-20200820211705-5c72a883971a
	golang.org/x/oauth2 v0.0.0-20200902213428-5d25da1a8d43
	google.golang.org/api v0.33.0
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/VividCortex/ewma.v1 v1.1.1 // indirect
	gopkg.in/cheggaaa/pb.v2 v2.0.7 // indirect
	gopkg.in/fatih/color.v1 v1.7.0 // indirect
	gopkg.in/mattn/go-colorable.v0 v0.1.0 // indirect
	gopkg.in/mattn/go-isatty.v0 v0.0.4 // indirect
	gopkg.in/mattn/go-runewidth.v0 v0.0.4 // indirect
	gopkg.in/yaml.v2 v2.2.8
	gorm.io/driver/postgres v1.0.2
	gorm.io/gorm v1.20.2
	syreclabs.com/go/faker v1.2.2
)

replace github.com/flarco/gutil v0.0.0 => ../gutil
