// Package share provides Skynet's cross-actor data sharing and pub/sub
// subsystems: sharetable, datasheet, sharedata, sharemap, stm, and multicast.
package share

import (
	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet/contrib/sproto"
	"github.com/yuchanns/spacenet/runtime/contrib"
)

// New returns the share contrib (sharetable, datasheet, sharedata, sharemap,
// stm, multicast), declaring a dependency on contrib/sproto for spacenet.sharemap.
func New() *contrib.Contrib {
	return contrib.New("share",
		contrib.WithDeps(sproto.New()),
		contrib.WithInit(initMulticastNode),
		contrib.WithFS(subFS),
		contrib.WithPreloads(map[string]moelua.GoFunc{
			"spacenet.multicast.core":  preloadMulticastCore,
			"spacenet.stm":             preloadSTM,
			"spacenet.sharetable.core": preloadShareTableCore,
			"spacenet.datasheet.core":  preloadDatasheetCore,
			"spacenet.sharedata.core":  preloadShareDataCore,
		}),
	)
}

func toInt(v moelua.Value) int64 {
	if v.IsInt() {
		return v.Int()
	}
	if v.IsFloat() {
		return int64(v.Float())
	}
	return 0
}
