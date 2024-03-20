package main

import (
	"github.com/fatih/color"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/env"
)

type media struct{}

var SlingMedia = media{}

func (m media) PrintFollowUs() {
	choices := []string{
		g.F("%s 👉 %s", color.HiGreenString("Follow Sling's Evolution"), color.HiBlueString("https://twitter.com/SlingDataIO")),
		// g.F("%s%s", color.HiGreenString("Follow Sling's Evolution: "), color.HiBlueString("https://linkedin.com/company/slingdata-io")),
	}
	i := g.RandInt(len(choices))
	env.Println("\n" + choices[i])
}
