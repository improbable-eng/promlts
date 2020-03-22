// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package ui

import (
	"html/template"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/common/version"
	"github.com/thanos-io/thanos/pkg/component"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/query"
)

type Query struct {
	*BaseUI
	getStoreStatus func() []query.StoreStatus

	flagsMap map[string]string

	cwd   string
	birth time.Time
	reg   prometheus.Registerer
	now   func() model.Time
}

type thanosVersion struct {
	Version   string `json:"version"`
	Revision  string `json:"revision"`
	Branch    string `json:"branch"`
	BuildUser string `json:"buildUser"`
	BuildDate string `json:"buildDate"`
	GoVersion string `json:"goVersion"`
}

func NewQueryUI(logger log.Logger, reg prometheus.Registerer, getStoreStatus func() []query.StoreStatus, flagsMap map[string]string) *Query {
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "<error retrieving current working directory>"
	}
	return &Query{
		BaseUI:         NewBaseUI(logger, "query_menu.html", queryTmplFuncs()),
		getStoreStatus: getStoreStatus,
		flagsMap:       flagsMap,
		cwd:            cwd,
		birth:          time.Now(),
		reg:            reg,
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

// Register registers new GET routes for subpages and retirects from / to /graph.
func (q *Query) Register(r *route.Router, ins extpromhttp.InstrumentationMiddleware) {
	instrf := func(name string, next func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
		return ins.NewHandler(name, http.HandlerFunc(next))
	}

	r.Get("/", instrf("root", q.root))
	r.Get("/graph", instrf("graph", q.graph))
	r.Get("/stores", instrf("stores", q.stores))
	r.Get("/status", instrf("status", q.status))

	r.Get("/static/*filepath", instrf("static", q.serveStaticAsset))
	// TODO(bplotka): Consider adding more Thanos related data e.g:
	// - What store nodes we see currently.
	// - What sidecars we see currently.
}

// Root redirects "/" requests to "/graph", taking into account the path prefix value.
func (q *Query) root(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(q.logger, q.flagsMap, r)

	http.Redirect(w, r, path.Join(prefix, "/graph"), http.StatusFound)
}

func (q *Query) graph(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(q.logger, q.flagsMap, r)

	q.executeTemplate(w, "graph.html", prefix, nil)
}

func (q *Query) status(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(q.logger, q.flagsMap, r)

	q.executeTemplate(w, "status.html", prefix, struct {
		Birth   time.Time
		CWD     string
		Version thanosVersion
	}{
		Birth: q.birth,
		CWD:   q.cwd,
		Version: thanosVersion{
			Version:   version.Version,
			Revision:  version.Revision,
			Branch:    version.Branch,
			BuildUser: version.BuildUser,
			BuildDate: version.BuildDate,
			GoVersion: version.GoVersion,
		},
	})
}

func (q *Query) stores(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(q.logger, q.flagsMap, r)
	statuses := make(map[component.StoreAPI][]query.StoreStatus)
	for _, status := range q.getStoreStatus() {
		statuses[status.StoreType] = append(statuses[status.StoreType], status)
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
