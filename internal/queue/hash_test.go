package queue

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestCanonicalizeIsOrderIndependent(t *testing.T) {
	a, err := Canonicalize(json.RawMessage(`{"b": 1, "a": {"z": true, "y": [1, 2]}}`))
	if err != nil {
		t.Fatalf("Canonicalize: %v", err)
	}
	b, err := Canonicalize(json.RawMessage(`{"a":{"y":[1,2],"z":true},"b":1}`))
	if err != nil {
		t.Fatalf("Canonicalize: %v", err)
	}
	if string(a) != string(b) {
		t.Fatalf("canonical forms differ:\n%s\n%s", a, b)
	}
	if string(a) != `{"a":{"y":[1,2],"z":true},"b":1}` {
		t.Fatalf("unexpected canonical form: %s", a)
	}
}

func TestCanonicalizeKeepsNumberPrecision(t *testing.T) {
	raw := json.RawMessage(`{"seed":12345678901234567890}`)
	out, err := Canonicalize(raw)
	if err != nil {
		t.Fatalf("Canonicalize: %v", err)
	}
	if string(out) != `{"seed":12345678901234567890}` {
		t.Fatalf("number lost precision: %s", out)
	}
}

func TestCanonicalizeRejectsBadJSON(t *testing.T) {
	cases := []string{
		``,
		`   `,
		`{"a":}`,
		`{"a":1} {"b":2}`,
	}
	for _, raw := range cases {
		if _, err := Canonicalize(json.RawMessage(raw)); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("Canonicalize(%q): err = %v, want ErrInvalidInput", raw, err)
		}
	}
}

func TestRequestHashIsStable(t *testing.T) {
	h1, err := RequestHash(json.RawMessage(`{"a":1,"b":2}`))
	if err != nil {
		t.Fatalf("RequestHash: %v", err)
	}
	h2, err := RequestHash(json.RawMessage(`{ "b": 2, "a": 1 }`))
	if err != nil {
		t.Fatalf("RequestHash: %v", err)
	}
	if h1 != h2 {
		t.Fatalf("hash depends on key order: %s vs %s", h1, h2)
	}
	if len(h1) != 64 {
		t.Fatalf("hash length = %d, want 64", len(h1))
	}

	// 已知向量：sha256(`{"a":1}`)。
	h3, err := RequestHash(json.RawMessage(`{"a":1}`))
	if err != nil {
		t.Fatalf("RequestHash: %v", err)
	}
	const want = "015abd7f5cc57a2dd94b7590f04ad8084273905ee33ec5cebeae62276a97f862"
	if h3 != want {
		t.Fatalf("hash = %s, want %s", h3, want)
	}
}
