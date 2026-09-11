package gateway

import (
	_ "embed"
	"net/http"
)

// viewerHTML is the live map, served at / by this process, embedded from viewer.html.
//
// It is an embedded asset rather than a raw Go string literal, and the reason is mechanical
// rather than aesthetic: a raw string literal cannot contain a backtick, so a single one
// anywhere in the markup or the script turns the file into a syntax error that fails the build.
// The first version of this file was a raw string and did exactly that. An embedded asset has no
// such hazard, keeps editor support for the HTML it actually is, and is the same pattern the
// migrations use.
//
// # What the viewer deliberately does not do
//
// No map tiles. A tile provider means a third-party request from the page, an API key for
// anything beyond a demo, and a viewer that silently renders an empty grey rectangle when it is
// offline. The canvas plots the fleet in its own coordinate space instead, which is honest about
// what the data is — deterministic vehicles on committed geometry — and works with no network.
//
// The `event:` names it handles are the contract the gateway documents: `position` frames carry
// data, and `ready`, `resumed`, `reset` and `shed` are control frames it reports in its status
// line. A viewer that ignored the control frames would still work; one that ignored `reset`
// would draw a map with a hole in it and no way to know.
//
//go:embed viewer.html
var viewerHTML string

// MountViewer registers the viewer at the exact root path.
//
// The `{$}` is not decoration: in Go's ServeMux a bare "/" is a prefix pattern that matches every
// path no other pattern claims, which would turn every typo into this page instead of a 404.
// `/{$}` matches the root and nothing else.
func (g *Gateway) MountViewer(mux *http.ServeMux) {
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if _, err := w.Write([]byte(viewerHTML)); err != nil {
			g.log.Debug("writing the viewer failed", "err", err)
		}
	})
}
