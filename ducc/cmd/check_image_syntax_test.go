package cmd

import "testing"

func TestCheckImageSyntaxCmd(t *testing.T) {
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"check-image-syntax", "registry.hub.docker.com/almalinux:9"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	cmd.SetArgs([]string{"check-image-syntax", "thatshouldntwork"})
	err = cmd.Execute()
	if err == nil {
		t.Fatal("That should have returned an error")
	}
}
