package main

import (
	"log"

	"github.com/slingdata-io/sling-cli/core/sling"
)

func main() {
	// cfgStr can be JSON or YAML
	cfgStr := `
    source:
        conn: $POSTGRES_URL
        stream: myschema.mytable
    
    target:
        conn: $SNOWFLAKE_URL
        object: yourschema.yourtable
		
		mode: drop
  `
	cfg, err := sling.NewConfig(cfgStr)
	if err != nil {
		log.Fatal(err)
	}

	err = sling.Sling(cfg)
	if err != nil {
		log.Fatal(err)
	}
}
