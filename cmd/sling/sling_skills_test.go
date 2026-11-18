package main

import "testing"

func TestSkillsCommandRegistered(t *testing.T) {
	if cliSkills.Name != "skills" {
		t.Fatalf("name is %q", cliSkills.Name)
	}
	if len(cliSkills.PosFlags) != 1 || cliSkills.PosFlags[0].Name != "refs..." {
		t.Fatalf("want one PosFlag refs..., got %+v", cliSkills.PosFlags)
	}
	if !cliSkills.ExecuteWithoutFlags {
		t.Fatal("ExecuteWithoutFlags must be true")
	}
}
