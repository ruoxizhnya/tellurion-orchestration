// Package orchestration 是 tellurion-orchestration 的占位包。
//
// 本仓尚未实现（见 README 的状态一节）。放这个文件只是为了：
//  1. 让 `go build ./...` 从一开始就是绿的（空 module 会报 no Go files）；
//  2. 让本仓可被 `go list ./...` 识别，从而能接入 R1/R2 自检。
//
// 实现落地时按能力拆子包，不要把所有代码堆在根包。
package orchestration
