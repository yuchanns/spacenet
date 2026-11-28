package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet"
)

// loadConfigFile evaluates a Skynet Lua configuration file (supporting Skynet's
// `include "path"` directive and `$ENV_VAR` expansion) and returns spacenet
// options applying its environment variables, thread count, harbor, logger, and bootstrap.
func loadConfigFile(filename string) ([]spacenet.Option, error) {
	env, err := parseConfigFile(filename)
	if err != nil {
		return nil, err
	}
	opts := []spacenet.Option{spacenet.WithEnvMap(env)}
	if t, ok := env["thread"]; ok {
		if n, err := strconv.Atoi(t); err == nil && n > 0 {
			opts = append(opts, spacenet.WithThreads(n))
		}
	}
	if h, ok := env["harbor"]; ok {
		if n, err := strconv.Atoi(h); err == nil && n >= 0 && n <= 255 {
			opts = append(opts, spacenet.WithHarbor(uint8(n)))
		}
	}
	if b, ok := env["bootstrap"]; ok && b != "" {
		opts = append(opts, spacenet.WithBootstrap(b))
	}
	if lf, ok := env["logger"]; ok && lf != "" {
		opts = append(opts, spacenet.WithLoggerFile(lf))
	}
	return opts, nil
}

// parseConfigFile evaluates a Skynet Lua configuration file into a string map
// matching skynet-src/skynet_main.c's load_config script.
func parseConfigFile(filename string) (map[string]string, error) {
	s := moelua.New()
	defer s.Close()

	// Exact Skynet load_config logic using MoeLua.
	const loadConfigLua = `
local filename = ...
local result = {}
local function getenv(name) return assert(os.getenv(name), [[os.getenv() failed: ]] .. name) end
local sep = package.config:sub(1,1)
local current_path = [[.]]..sep
local function is_abs(p)
	if p:sub(1,1) == [[/]] or p:sub(1,1) == [[\]] then
		return true
	end
	if p:match([=[^[A-Za-z]:[/\]]=]) then
		return true
	end
	return false
end
local function include(cfg)
	local last_path = current_path
	local path, name = cfg:match([[(.*[/\])(.*)$]])
	if path then
		if is_abs(path) then
			current_path = path
		else
			current_path = current_path .. path
		end
	else
		name = cfg
	end
	local f = assert(io.open(current_path .. name))
	local code, rerr = f:read [[*a]]
	f:close()
	assert(code, rerr)
	code = string.gsub(code, [[%$([%w_%d]+)]], getenv)
	assert(load(code, [[@]]..name, [[t]], result))()
	current_path = last_path
end
setmetatable(result, { __index = { include = include } })
include(filename)
setmetatable(result, nil)
return result
`
	ch, err := s.Compile("=load_config", loadConfigLua)
	if err != nil {
		return nil, err
	}
	fn, err := s.Load(ch)
	if err != nil {
		return nil, err
	}
	absOrRel := filename
	if !filepath.IsAbs(absOrRel) {
		if _, err := os.Stat(absOrRel); err != nil {
			return nil, fmt.Errorf("spacenet: cannot open config %q: %w", filename, err)
		}
	}
	res, err := s.Call(fn, s.String(absOrRel))
	if err != nil {
		return nil, fmt.Errorf("spacenet: eval config %q: %w", filename, err)
	}
	if len(res) == 0 || !res[0].IsTable() {
		return nil, fmt.Errorf("spacenet: config %q did not return a table", filename)
	}
	tbl := s.TableOf(res[0])
	out := make(map[string]string)
	err = tbl.ForEach(func(k, v moelua.Value) error {
		ks, ok := s.ToString(k)
		if !ok {
			return nil
		}
		switch {
		case v.IsBool():
			if v.IsTrue() {
				out[ks] = "true"
			} else {
				out[ks] = "false"
			}
		case v.IsString():
			vs, _ := s.ToString(v)
			out[ks] = vs
		case v.IsInt():
			out[ks] = strconv.FormatInt(v.Int(), 10)
		case v.IsFloat():
			out[ks] = strconv.FormatFloat(v.Float(), 'f', -1, 64)
		default:
			out[ks] = strings.TrimSpace(s.FormatValue(v))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
