package httpapi

// SVC-ORCH-G2 ①：本仓是 `run.*` 的唯一 writer。守卫测试静态扫全仓写语句：
// 只允许 `run.` 前缀，出现 quant./market./analysis./risk. 等外部 schema 即红；
// 也不允许把本仓四张表写成不带 schema 的裸名（那会随 search_path 漂移）。

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

var writeStatement = regexp.MustCompile(`(?i)\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+([a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?)`)

// runTables 是本仓拥有的表；裸名出现即视为「写错 schema 的前兆」。
var runTables = map[string]bool{
	"backtest_jobs":        true,
	"run_artifacts":        true,
	"batch_runs":           true,
	"walk_forward_reports": true,
	"worker_registry":      true,
	"parent_jobs":          true,
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(filepath.Dir(filepath.Dir(file)))
}

func TestNoCrossSchemaWrites(t *testing.T) {
	root := repoRoot(t)
	var offenders []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "dist", "deploy":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		for _, m := range writeStatement.FindAllStringSubmatch(string(src), -1) {
			target := strings.ToLower(m[1])
			if !strings.Contains(target, ".") {
				if runTables[target] {
					offenders = append(offenders, rel+": unqualified write to "+target)
				}
				continue
			}
			schema := strings.SplitN(target, ".", 2)[0]
			if schema != "run" {
				offenders = append(offenders, rel+": "+strings.TrimSpace(m[0]))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk repo: %v", err)
	}
	if len(offenders) > 0 {
		t.Fatalf("writes outside run.* (this repo is the only run.* writer):\n%s", strings.Join(offenders, "\n"))
	}
}
