// Package version holds build metadata injected through ldflags.
package version

import "fmt"

var (
	// ProjectName is the binary/project name.
	ProjectName = "unknown"
	// Version is the semantic version or CI tag.
	Version = "dev"
	// GitCommit is the short hash of the commit used for the build.
	GitCommit = "unknown"
	// GitMsg is the last commit subject, sanitized by the Makefile.
	GitMsg = "unknown"
	// BuildDate is the UTC build timestamp in RFC3339 format.
	BuildDate = "unknown"
)

func String() string {
	return StringWithAppEnv("")
}

func StringWithAppEnv(appEnv string) string {
	out := fmt.Sprintf("project=%s\nversion=%s\ngit_commit=%s\ngit_msg=%s\nbuild_date=%s",
		ProjectName,
		Version,
		GitCommit,
		GitMsg,
		BuildDate,
	)
	if appEnv != "" {
		out += fmt.Sprintf("\napp_env=%s", appEnv)
	}
	return out
}
