package core

import (
	"embed"
	"io/fs"
)

//go:embed all:lua
var rawFS embed.FS

var subFS fs.FS

func init() {
	var err error
	subFS, err = fs.Sub(rawFS, "lua")
	if err != nil {
		panic(err)
	}
}

// FS returns the embedded Core Skynet Lua files (lualib/* and service/*).
func FS() fs.FS {
	return subFS
}
