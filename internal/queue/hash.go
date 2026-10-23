package queue

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
)

// Canonicalize 把任意 JSON 归一化成稳定字节序列：递归排序对象键、去空白、
// 数字保留原文（`json.Number`，不经过 float64 丢精度）。
//
// 入队时 `request_json` 的 canonical 形式是幂等判据的一部分（docs/db-schema.md
// §5：`request_hash = sha256(canonical request_json)`），因此归一化必须
// 只依赖内容、不依赖入参书写顺序。
func Canonicalize(raw json.RawMessage) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, fmt.Errorf("%w: empty request_json", ErrInvalidInput)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("%w: request_json is not valid JSON: %v", ErrInvalidInput, err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("%w: request_json has trailing data", ErrInvalidInput)
	}
	// json.Marshal 对 map 的键按字典序输出，所以这里的输出与入参键序无关。
	out, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%w: request_json cannot be canonicalized: %v", ErrInvalidInput, err)
	}
	return out, nil
}

// RequestHash 返回 `sha256(canonical request_json)` 的十六进制串（64 字符，
// 与 `run.backtest_jobs.request_hash CHAR(64)` 对齐）。
func RequestHash(raw json.RawMessage) (string, error) {
	canonical, err := Canonicalize(raw)
	if err != nil {
		return "", err
	}
	return hashBytes(canonical), nil
}

func hashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
