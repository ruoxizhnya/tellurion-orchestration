package plan

import (
	"bytes"
	"encoding/json"
	"io"
	"sort"
	"strings"
)

// MaxBatchTasks 是单次批量的任务上限（与 core 的 REV-11 同值：100）。
// 超限在**展开前**拒绝，避免一个请求先分配十万个子 run 再报错。
const MaxBatchTasks = 100

// BatchRequest 是批量扫描的规划输入。
//
//	{
//	  "base_run_spec": {完整 RunSpec},
//	  "grid": {"param_a": [v1, v2], "param_b": [x]}
//	}
//
// Grid 的键写进 `base_run_spec.strategy_params`（浅合并，网格点覆盖同名 base 值），
// 其余字段原样保留。空网格 / 空取值 / 非标量取值 / 展开超限都是 invalid_params。
type BatchRequest struct {
	BaseRunSpec json.RawMessage
	Grid        map[string][]json.RawMessage
}

// ExpandBatch 把网格展开成笛卡尔积，每个点生成一条**完整**的子 run spec。
//
// 确定性两条（父报告的 task 顺序必须可复现）：
//   - 参数名按字典序排列；
//   - 最后一个参数变化最快（里程表进位），与入参 map 的遍历顺序无关。
//
// 展开后的子 Request 会重新 marshal——encoding/json 对 map 键排序、RawMessage
// 原样输出，所以同一请求两次展开的字节完全一致。
func ExpandBatch(req BatchRequest) ([]Child, error) {
	base, err := RunSpecObject(req.BaseRunSpec)
	if err != nil {
		return nil, err
	}
	if len(req.Grid) == 0 {
		return nil, invalidf("grid must not be empty")
	}

	names := make([]string, 0, len(req.Grid))
	for name, values := range req.Grid {
		if strings.TrimSpace(name) == "" {
			return nil, invalidf("grid parameter name must not be empty")
		}
		if len(values) == 0 {
			return nil, invalidf("grid parameter %q has no values", name)
		}
		for i, v := range values {
			if err := validateScalar(v); err != nil {
				return nil, invalidf("grid parameter %q value %d: %v", name, i, err)
			}
		}
		names = append(names, name)
	}
	sort.Strings(names)

	// 先算总任务数，再展开：上限是发布契约（api-schema.md §2 invalid_params），
	// 不能在内存里先生成再丢弃。
	total := 1
	for _, name := range names {
		if total > MaxBatchTasks/len(req.Grid[name]) {
			return nil, invalidf("grid expands beyond %d tasks", MaxBatchTasks)
		}
		total *= len(req.Grid[name])
	}

	existing, err := strategyParamsOf(base)
	if err != nil {
		return nil, err
	}

	out := make([]Child, 0, total)
	idx := make([]int, len(names))
	for {
		point := make(map[string]json.RawMessage, len(names))
		for i, name := range names {
			point[name] = req.Grid[name][idx[i]]
		}

		merged := make(map[string]json.RawMessage, len(existing)+len(point))
		for k, v := range existing {
			merged[k] = v
		}
		for k, v := range point {
			merged[k] = v
		}
		paramsJSON, err := json.Marshal(merged)
		if err != nil {
			return nil, invalidf("strategy_params cannot be encoded: %v", err)
		}

		childObj := cloneObject(base)
		childObj["strategy_params"] = paramsJSON
		child, err := marshalObject(childObj)
		if err != nil {
			return nil, err
		}

		out = append(out, Child{
			Kind:    ChildBatch,
			Index:   len(out) + 1,
			Request: child,
			Params:  point,
		})

		// 里程表进位：最后一个参数先动。
		pos := len(names) - 1
		for pos >= 0 {
			idx[pos]++
			if idx[pos] < len(req.Grid[names[pos]]) {
				break
			}
			idx[pos] = 0
			pos--
		}
		if pos < 0 {
			break
		}
	}
	return out, nil
}

// strategyParamsOf 取 base 里已有的 strategy_params（缺失 / null = 空对象）。
//
// 非对象一律拒绝：契约（`StrategyParams`）只允许浅层对象，数组 / 标量会让
// 「同参」的语义依赖遍历顺序，也会让 params_hash 失去唯一定义。
func strategyParamsOf(base map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	raw, ok := base["strategy_params"]
	if !ok || len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return map[string]json.RawMessage{}, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, invalidf("base_run_spec.strategy_params must be an object: %v", err)
	}
	if obj == nil {
		return map[string]json.RawMessage{}, nil
	}
	return obj, nil
}

// validateScalar 只接受 JSON 标量（string / number / bool / null）。
//
// 与 contracts 的 `StrategyParams` 形状同源：嵌套对象 / 数组会让「同参」的
// 语义依赖遍历顺序，也让 params_hash 失去唯一定义。
func validateScalar(raw json.RawMessage) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return invalidf("not valid JSON: %v", err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return invalidf("has trailing content")
	}
	switch v.(type) {
	case string, json.Number, bool, nil:
		return nil
	default:
		return invalidf("must be a scalar (string / number / bool / null)")
	}
}
