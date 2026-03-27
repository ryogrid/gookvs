package e2elib

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// envVarNames maps binary names to their corresponding environment variable names.
var envVarNames = map[string]string{
	"gookv-server": "GOOKV_SERVER_BIN",
	"gookv-pd":     "GOOKV_PD_BIN",
	"gookv-ctl":    "GOOKV_CTL_BIN",
}

// FindBinary returns the path to a gookv binary (gookv-server, gookv-pd, gookv-ctl).
// Resolution order: env var (GOOKV_SERVER_BIN etc.) > repo root > $PATH
func FindBinary(name string) string {
	// 1. Check environment variable.
	if envVar, ok := envVarNames[name]; ok {
		if p := os.Getenv(envVar); p != "" {
			if _, err := os.Stat(p); err == nil {
				return p
			}
		}
	}

	// 2. Check repo root bin/ and root directory.
	if root := findRepoRoot(); root != "" {
		candidates := []string{
			filepath.Join(root, "bin", name),
			filepath.Join(root, name),
		}
		for _, c := range candidates {
			if _, err := os.Stat(c); err == nil {
				return c
			}
		}
	}

	// 3. Check $PATH.
	if p, err := exec.LookPath(name); err == nil {
		return p
	}

	return ""
}

// SkipIfNoBinary skips the test if the specified binary is not found.
func SkipIfNoBinary(t *testing.T, names ...string) {
	t.Helper()
	for _, name := range names {
		if FindBinary(name) == "" {
			t.Skipf("binary %q not found; set %s or place it in repo root/bin", name, envVarNames[name])
		}
	}
}

// findRepoRoot walks up from cwd to find go.mod containing "github.com/ryogrid/gookv".
func findRepoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}

	for {
		gomod := filepath.Join(dir, "go.mod")
		if data, err := os.ReadFile(gomod); err == nil {
			if strings.Contains(string(data), "github.com/ryogrid/gookv") {
				return dir
			}
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return ""
}
