package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/flarco/g"
	"github.com/integrii/flaggy"
	"github.com/slingdata-io/sling-cli/core"
	"github.com/slingdata-io/sling-cli/core/sling/assist"
	"github.com/spf13/cast"
)

var cliSkills = &g.CliSC{
	Name:                "skills",
	Description:         "Print the Sling skills for the current agent session",
	ExecuteWithoutFlags: true,
	PosFlags: []g.Flag{
		{Name: "refs...", ShortName: "", Type: "string", Description: "skill or skill/FILE.md"},
	},
	ExecProcess: processSkills,
}

func init() {
	cliSkills.Make().Add()
}

// processSkills writes only to stdout. It changes no files.
func processSkills(c *g.CliSC) (ok bool, err error) {
	ok = true
	catalog := assist.NewSkillCatalog()

	refs := []string{}
	if v := strings.TrimSpace(cast.ToString(c.Vals["refs..."])); v != "" {
		refs = append(refs, v)
	}
	refs = append(refs, flaggy.TrailingArguments...)

	if len(refs) == 0 {
		text, err := catalog.Index(core.Version)
		if err != nil {
			return ok, err
		}
		fmt.Fprint(os.Stdout, text)
		return ok, nil
	}

	for i, ref := range refs {
		text, err := catalog.Render(ref)
		if err != nil {
			return ok, err
		}
		if i > 0 {
			fmt.Fprintln(os.Stdout)
		}
		fmt.Fprint(os.Stdout, text)
	}
	return ok, nil
}
