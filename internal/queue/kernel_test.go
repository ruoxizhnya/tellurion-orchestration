package queue

import (
	"encoding/json"
	"testing"
)

// TestKernelOfRuleIsStringOnly 钉住生效内核的抽取规则本体（缺键 / null / 非字符串 /
// 空串 / 纯空白 = 默认，且字符串两端空白不算内核名的一部分）。
// PG 侧那份 SQL 形在 internal/pgstore.claimSQL；两侧同口径的行为钉子是
// qtest.PairCases 里的 ClaimByKernelReadsOnlyStringKernels。
func TestKernelOfRuleIsStringOnly(t *testing.T) {
	cases := []struct{ what, raw, want string }{
		{"empty bytes", "", "go"},
		{"no kernel key", `{"strategy":"ma_cross"}`, "go"},
		{"explicit string", `{"kernel":"nautilus"}`, "nautilus"},
		{"empty string", `{"kernel":""}`, "go"},
		{"whitespace only", `{"kernel":"   "}`, "go"},
		{"padded value", `{"kernel":" go "}`, "go"},
		{"null", `{"kernel":null}`, "go"},
		{"number", `{"kernel":123}`, "go"},
		{"nested object", `{"kernel":{"name":"go"}}`, "go"},
		{"top-level array", `["not-an-object"]`, "go"},
		{"top-level scalar", `"go"`, "go"},
		// 非法 JSON 进不了队列（Enqueue 里 Canonicalize 当场拒），这里只求不 panic。
		{"malformed", `{"kernel":`, "go"},
	}
	for _, c := range cases {
		if got := KernelOf(json.RawMessage(c.raw)); got != c.want {
			t.Errorf("%s: KernelOf(%s) = %q, want %q", c.what, c.raw, got, c.want)
		}
	}
}

func TestMatchesKernels(t *testing.T) {
	const row = `{"kernel":"nautilus"}`
	cases := []struct {
		what     string
		declared []string
		raw      string
		want     bool
	}{
		{"nil declaration is unfiltered", nil, row, true},
		{"empty declaration is unfiltered", []string{}, row, true},
		{"declared matches", []string{"nautilus"}, row, true},
		{"among several", []string{"go", "nautilus"}, row, true},
		{"declared misses", []string{"go"}, row, false},
		{"declaration padded", []string{" nautilus "}, row, true},
		{"default row taken by go", []string{"go"}, `{"a":1}`, true},
		{"default row rejected by nautilus", []string{"nautilus"}, `{"a":1}`, false},
	}
	for _, c := range cases {
		if got := MatchesKernels(c.declared, json.RawMessage(c.raw)); got != c.want {
			t.Errorf("%s: MatchesKernels(%q, %s) = %v, want %v", c.what, c.declared, c.raw, got, c.want)
		}
	}
}
