package pgstore_test

// request_canonical 这条列存在的全部理由，用一个用例钉住。
//
// 幂等判据是 `request_hash = sha256(canonical request_json)`（docs/db-schema.md §5）。
// 如果身份字节只存在 JSONB 里，读回来的就不是当初参与 hash 的那份字节：
// PostgreSQL 的 jsonb 会规范化数字文本并重排空白。真库实测（本用例同款输入）：
//
//	输入  {"a": 1e-05, "b": 0e0}
//	输出  {"a": 0.00001, "b": 0}
//
// 也就是说「拿 request_json 复算 hash」一定会和 request_hash 不一致，
// 于是同一个幂等键在第二次提交时被判成**不同请求** → 409。
// 这不是理论风险：commission/slippage 这类小数值 Go 就会写成 1e-05 形状。
//
// 处置：行里同时存 request_json（JSONB，可查询）与 request_canonical
// （TEXT，identity 的字节锚点），读路径回后者。见 internal/queue/ddl.go 的列注释。

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"tellurion-orchestration/internal/pgstore"
	"tellurion-orchestration/internal/queue"
)

func TestCanonicalBytesSurviveTheJSONBRoundTrip(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()
	q := store.Queue(nil, pgstore.Options{})

	// 一份**只有**在字节级保真才可复现的请求：指数形状的小数 + 尾随零 + 键序。
	body := `{"slippage_rate": 1e-05, "commission_rate": 0.00025, "zero": 0e0, "capital": 1000000.10, "universe": ["600000.SH", "000001.SZ"]}`
	canonical, err := queue.Canonicalize([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	sum, err := queue.RequestHash(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(canonical), "1e-05") {
		t.Fatalf("canonical form must preserve the caller's number text, got %s", canonical)
	}

	job, created, err := q.Enqueue(ctx, &queue.RunRecord{IdempotencyKey: "anchor", RequestJSON: []byte(body)})
	if err != nil || !created {
		t.Fatalf("Enqueue = %v / %v", created, err)
	}

	// 1) port 读回来的是**当初参与 hash 的那份字节**。
	back := mustRow(t, q, job.ID)
	if string(back.RequestJSON) != string(canonical) {
		t.Fatalf("request bytes changed on round trip:\n got  %s\n want %s", back.RequestJSON, canonical)
	}
	if sum != job.RequestHash {
		t.Fatalf("Enqueue stored hash %q, want the canonical-content hash %q", job.RequestHash, sum)
	}
	if reSum := hashOf(back.RequestJSON); reSum != job.RequestHash {
		t.Fatalf("recomputed hash %q != stored request_hash %q", reSum, job.RequestHash)
	}

	// 2) 同一个键再提交一次必须是**重放**，不是 409——哪怕第二次把空白和键序写得不一样。
	again, created, err := q.Enqueue(ctx, &queue.RunRecord{
		IdempotencyKey: "anchor",
		RequestJSON:    []byte(`{"universe":["600000.SH","000001.SZ"], "capital":1000000.10, "zero":0e0, "commission_rate":0.00025, "slippage_rate":1e-05}`),
	})
	if err != nil || created || again.ID != job.ID {
		t.Fatalf("replay: created=%v err=%v id=%s", created, err, again.ID)
	}

	// 3) 对照腿：JSONB 那一列**确实**不是锚点。这条断言是防止将来有人
	//    「简化」成只留 request_json 然后拿它复算 hash。
	var fromJSONB string
	if err := store.Pool().QueryRow(ctx,
		`SELECT request_json::text FROM run.backtest_jobs WHERE id=$1`, job.ID).Scan(&fromJSONB); err != nil {
		t.Fatal(err)
	}
	if hashOf([]byte(fromJSONB)) == job.RequestHash {
		t.Fatal("JSONB text reproduced the identity hash — 这条对照腿的前提（jsonb 会改数字文本）已经不成立，" +
			"要重新评估 request_canonical 是否还必要，并把本用例改写成证明新形状")
	}
	if !strings.Contains(fromJSONB, "0.00001") {
		t.Fatalf("expected jsonb to rewrite 1e-05 into 0.00001, got %s", fromJSONB)
	}
	// 锚点列与 JSONB 列必须在同一行里描述同一份内容（结构等价，文本不必等价）。
	var canonicalCol string
	if err := store.Pool().QueryRow(ctx,
		`SELECT request_canonical FROM run.backtest_jobs WHERE id=$1`, job.ID).Scan(&canonicalCol); err != nil {
		t.Fatal(err)
	}
	if canonicalCol != string(canonical) {
		t.Fatalf("request_canonical = %q, want %q", canonicalCol, canonical)
	}
	// 读路径取的是锚点列，不是 JSONB 列——列名换掉时 Scan 会错位，这里显式核对。
	if strings.Contains(fromJSONB, string(canonical)) {
		t.Fatal("harness bug: the two columns hold identical text, the对照腿无效")
	}
}

func hashOf(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func mustRow(t *testing.T, q queue.JobQueue, id string) *queue.RunRecord {
	t.Helper()
	row, err := q.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	return row
}
