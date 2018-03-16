// +build ignore

package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"

	"github.com/coreos/go-semver/semver"
)

func main() {
	var version string
	tag := getTag()
	if tag != "" && strings.HasPrefix(tag, "v") {
		version = strings.Split(tag, "v")[1]
	} else {
		c := getLatestTag()
		if c == "" {
			version = "0.0.0"
		} else {
			version = strings.Split(getTagForCommit(c), "v")[1]
			v := semver.New(version)
			v.BumpPatch()
			v.PreRelease = "dev"
			version = v.String()
		}
	}

	fmt.Printf("%s\n", version)
}

// getTag - Tet the current commit's tag, if any. Otherwise an empty string.
func getTag() string {
	t, err := runError("git", "describe", "--abbrev=0", "--exact-match")
	if err != nil {
		return ""
	}
	return string(t)
}

func getLatestTag() string {
	t, err := runError("git", "rev-list", "--tags", "--max-count=1")
	if err != nil {
		return ""
	}
	return string(t)
}

func getTagForCommit(commit string) string {
	t, err := runError("git", "describe", "--abbrev=0", "--tags", commit)
	if err != nil {
		return ""
	}
	return string(t)
}

func runError(cmd string, args ...string) ([]byte, error) {
	ecmd := exec.Command(cmd, args...)
	bs, err := ecmd.CombinedOutput()
	if err != nil {
		return nil, err
	}

	return bytes.TrimSpace(bs), nil
}
