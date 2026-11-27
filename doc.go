// Package spacenet is a modular Go + MoeLua (Lua 5.5.1) actor runtime
// compatible with cloudwu/skynet's Lua APIs.
//
// Core spacenet contains zero networking, cryptography, TLS, or legacy harbor
// dependencies; callers opt into only the capabilities they need via independent
// contrib modules (github.com/yuchanns/spacenet/contrib/*) and
// spacenet.WithContrib(...) so unused subsystems are never imported or linked.
package spacenet
