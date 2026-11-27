// Package all aggregates the standard spacenet contrib modules for
// full-featured servers and command-line nodes.
package all

import (
	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/cluster"
	"github.com/yuchanns/spacenet/contrib/crypt"
	"github.com/yuchanns/spacenet/contrib/debug"
	"github.com/yuchanns/spacenet/contrib/http"
	"github.com/yuchanns/spacenet/contrib/share"
	"github.com/yuchanns/spacenet/contrib/socket"
	"github.com/yuchanns/spacenet/contrib/sproto"
	"github.com/yuchanns/spacenet/contrib/tls"
)

// Contribs returns all official spacenet contrib modules.
func Contribs() []*spacenet.Contrib {
	return []*spacenet.Contrib{
		socket.New(),
		crypt.New(),
		tls.New(),
		http.New(),
		cluster.New(),
		share.New(),
		sproto.New(),
		debug.New(),
	}
}
