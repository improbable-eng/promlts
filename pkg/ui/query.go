// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package ui

import (
	"html/template"
	"net/http"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/thanos-io/thanos/pkg/api"
	"github.com/thanos-io/thanos/pkg/component"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/query"
)

type Query struct {
	*BaseUI
	storeSets []*query.StoreSet

	externalPrefix, prefixHeader string

	cwd     string
	birth   time.Time
	version api.ThanosVersion
	now     func() model.Time
}

func NewQueryUI(logger log.Logger, storeSets []*query.StoreSet, externalPrefix, prefixHeader string) *Query {
	tmplVariables := map[string]string{
		"Component": component.Query.String(),
	}
	runtimeInfo := api.GetRuntimeInfoFunc(logger)

	tmplFuncs := queryTmplFuncs()
	tmplFuncs["uiPrefix"] = func() string { return "/classic" }

	return &Query{
		BaseUI:         NewBaseUI(logger, "query_menu.html", tmplFuncs, tmplVariables, externalPrefix, prefixHeader, component.Query),
		storeSets:      storeSets,
		externalPrefix: externalPrefix,
		prefixHeader:   prefixHeader,
		cwd:            runtimeInfo().CWD,
		birth:          runtimeInfo().StartTime,
		version:        *api.BuildInfo,
		now:            model.Now,
	}
}

func queryTmplFuncs() template.FuncMap {
	return template.FuncMap{
		"since": func(t time.Time) time.Duration {
			return time.Since(t) / time.Millisecond * time.Millisecond
		},
		"formatTimestamp": func(timestamp int64) string {
			return time.Unix(timestamp/1000, 0).Format(time.RFC3339)
		},
		"title": strings.Title,
	}
}

// Register registers new GET routes for subpages and redirects from / to /graph.
func (q *Query) Register(r *route.Router, ins extpromhttp.InstrumentationMiddleware) {
	r.Get("/classic/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, path.Join("/", GetWebPrefix(q.logger, q.externalPrefix, q.prefixHeader, r), "/classic/graph"), http.StatusFound)
	})

	// Redirect the original React UI's path (under "/new") to its new path at the root.
	r.Get("/new/*path", func(w http.ResponseWriter, r *http.Request) {
		p := route.Param(r.Context(), "path")
		http.Redirect(w, r, path.Join("/", GetWebPrefix(q.logger, q.externalPrefix, q.prefixHeader, r), p)+"?"+r.URL.RawQuery, http.StatusFound)
	})

	r.Get("/classic/graph", instrf("graph", ins, q.graph))
	r.Get("/classic/stores", instrf("stores", ins, q.stores))
	r.Get("/classic/status", instrf("status", ins, q.status))
	r.Get("/classic/static/*filepath", instrf("static", ins, q.serveStaticAsset))

	registerReactApp(r, ins, q.BaseUI)

	// TODO(bplotka): Consider adding more Thanos related data e.g:
	// - What store nodes we see currently.
	// - What sidecars we see currently.
}

func (q *Query) graph(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(q.logger, q.externalPrefix, q.prefixHeader, r)

	q.executeTemplate(w, "graph.html", prefix, nil)
}

func (q *Query) status(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(q.logger, q.externalPrefix, q.prefixHeader, r)

	q.executeTemplate(w, "status.html", prefix, struct {
		Birth   time.Time
		CWD     string
		Version api.ThanosVersion
	}{
		Birth:   q.birth,
		CWD:     q.cwd,
		Version: q.version,
	})
}

func (q *Query) stores(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(q.logger, q.externalPrefix, q.prefixHeader, r)
	statuses := make(map[component.StoreAPI][]query.StoreStatus)
	for _, storesSet := range q.storeSets {
		for _, status := range storesSet.GetStoreStatus() {
			statuses[status.StoreType] = append(statuses[status.StoreType], status)
		}
	}

	sources := make([]component.StoreAPI, 0, len(statuses))
	for k := range statuses {
		sources = append(sources, k)
	}
	sort.Slice(sources, func(i int, j int) bool {
		if sources[i] == nil {
			return false
		}
		if sources[j] == nil {
			return true
		}
		return sources[i].String() < sources[j].String()
	})

	q.executeTemplate(w, "stores.html", prefix, struct {
		Stores  map[component.StoreAPI][]query.StoreStatus
		Sources []component.StoreAPI
	}{
		Stores:  statuses,
		Sources: sources,
	})
}
