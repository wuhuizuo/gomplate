package datafs

import (
	"context"
	"fmt"
	"io/fs"
	"net/url"
	"strings"

	"github.com/hairyhenderson/go-fsimpl"
	"github.com/hairyhenderson/gomplate/v4/internal/urlhelpers"
)

type fsProviderCtxKey struct{}

// ContextWithFSProvider returns a context with the given FSProvider. Should
// only be used in tests.
func ContextWithFSProvider(ctx context.Context, fsp fsimpl.FSProvider) context.Context {
	return context.WithValue(ctx, fsProviderCtxKey{}, fsp)
}

// FSProviderFromContext returns the FSProvider from the context, if any
func FSProviderFromContext(ctx context.Context) fsimpl.FSProvider {
	if fsp, ok := ctx.Value(fsProviderCtxKey{}).(fsimpl.FSProvider); ok {
		return fsp
	}

	return nil
}

// FSysForPath returns an [io/fs.FS] for the given path (which may be an URL),
// rooted at /. A [fsimpl.FSProvider] is required to be present in ctx,
// otherwise an error is returned.
func FSysForPath(ctx context.Context, path string) (fs.FS, error) {
	u, err := urlhelpers.ParseSourceURL(path)
	if err != nil {
		return nil, err
	}

	fsp := FSProviderFromContext(ctx)
	if fsp == nil {
		return nil, fmt.Errorf("no filesystem provider in context")
	}

	// git URLs are special - they have double-slashes that separate a repo from
	// a path in the repo. A missing double-slash means the path is the root.
	switch u.Scheme {
	case "git", "git+file", "git+http", "git+https", "git+ssh":
		u.Path, _, _ = strings.Cut(u.Path, "//")
	default:
		u.Path = "/"
	}

	fsys, err := fsp.New(u)
	if err != nil {
		return nil, fmt.Errorf("filesystem provider for %q unavailable: %w", path, err)
	}

	return fsys, nil
}

type fsp struct {
	newFunc func(*url.URL) (fs.FS, error)
	schemes []string
}

func (p fsp) Schemes() []string {
	return p.schemes
}

func (p fsp) New(u *url.URL) (fs.FS, error) {
	return p.newFunc(u)
}

// WrappedFSProvider is an FSProvider that returns the given fs.FS
func WrappedFSProvider(fsys fs.FS, schemes ...string) fsimpl.FSProvider {
	return fsp{
		newFunc: func(u *url.URL) (fs.FS, error) { return fsys, nil },
		schemes: schemes,
	}
}
