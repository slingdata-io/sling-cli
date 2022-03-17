package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/c-bata/go-prompt"
	"github.com/flarco/g"
	"github.com/flarco/sling/core"
	"github.com/flarco/sling/core/env"
	"github.com/spf13/cast"
)

var suggestions = []prompt.Suggest{}

func init() {
	suggList := [][]string{
		{"exit", "Exit interactive mode"},
		{cliRun.Name, cliRun.Description},
		{cliConns.Name, cliConns.Description},
	}
	for _, sl := range suggList {
		suggestions = append(suggestions, prompt.Suggest{Text: sl[0], Description: sl[1]})
	}
}

func completer(in prompt.Document) []prompt.Suggest {
	localSuggestions := []prompt.Suggest{}

	w := in.GetWordBeforeCursor()
	blocks := strings.Split(in.Text, " ")
	// trimmedBlocks := strings.Split(strings.TrimSpace(in.Text), " ")

	lastWord := blocks[len(blocks)-1]
	prevWord := lastWord

	if len(blocks) > 1 {
		prevWord = blocks[len(blocks)-2]
	}

	switch blocks[0] {
	case cliRun.Name:
		// collect strings flags
		stringFlags := []string{}
		for _, f := range cliRun.Flags {
			if f.Type == "string" {
				stringFlags = append(stringFlags, f.Name)
			}
		}

		// suggestions based on previous word
		switch {
		case g.In(prevWord, "src-conn", "tgt-conn"):
			for _, conn := range env.GetLocalConns() {
				localSuggestions = append(localSuggestions, prompt.Suggest{Text: conn.Name, Description: conn.Description})
			}
			return prompt.FilterHasPrefix(localSuggestions, w, true)
		case g.In(prevWord, cast.ToSlice(stringFlags)...):
			return []prompt.Suggest{}
		}

		// suggest normal flags
		for _, f := range cliRun.Flags {
			localSuggestions = append(localSuggestions, prompt.Suggest{Text: f.Name, Description: f.Description})
		}
		return prompt.FilterHasPrefix(localSuggestions, w, true)

	case cliConns.Name:
		for _, f := range cliConns.Flags {
			localSuggestions = append(localSuggestions, prompt.Suggest{Text: f.Name, Description: f.Description})
		}
		return prompt.FilterHasPrefix(localSuggestions, w, true)
	case "":
		return []prompt.Suggest{}
	}
	return prompt.FilterHasPrefix(suggestions, w, true)
}

func executor(in string) {
	in = strings.TrimSpace(in)

	blocks := strings.Split(in, " ")
	switch blocks[0] {
	case "exit":
		fmt.Println("exiting")
		os.Exit(0)
	case cliRun.Name:
		cliRun.Vals = g.M(cast.ToSlice(blocks[1:])...)
		g.LogError(cliRun.ExecProcess(cliRun))
	case cliConns.Name:
		if len(blocks) == 1 {
			return
		}
		for _, subCom := range cliConns.SubComs {
			if subCom.Name == blocks[1] {
				subCom.Vals = g.M(cast.ToSlice(blocks[2:])...)
				g.LogError(subCom.ExecProcess(subCom))
			}
		}
	}
	println(in)
}

func slingPrompt(c *g.CliSC) (err error) {
	fmt.Println("sling - An Extract-Load tool")
	fmt.Println("Slings data from a data source to a data target.\nVersion " + core.Version)

	p := prompt.New(
		executor,
		completer,
		prompt.OptionPrefix("sling > "),
		// prompt.OptionLivePrefix(livePrefix),
		prompt.OptionTitle("sling"),
	)

	p.Run()

	return
}
