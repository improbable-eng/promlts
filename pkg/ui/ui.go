// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package ui

import (
	"bytes"
	"html/template"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/route"
	"github.com/prometheus/common/version"
	"github.com/thanos-io/thanos/pkg/component"
)

var (
	reactAppPaths = []string{
		"/",
		"/alerts",
		"/config",
		"/flags",
		"/configfiles",
		"/graph",
		"/rules",
		"/service-discovery",
		"/status",
		"/targets",
		"/tsdb-status",
		"/version",
		"/stores",
		"/blocks",
		"/loaded",
	}
)

type BaseUI struct {
	logger                       log.Logger
	menuTmpl                     string
	tmplFuncs                    template.FuncMap
	tmplVariables                map[string]string
	externalPrefix, prefixHeader string
	component                    component.Component
}

func NewBaseUI(logger log.Logger, menuTmpl string, funcMap template.FuncMap, tmplVariables map[string]string, externalPrefix, prefixHeader string, component component.Component) *BaseUI {
	funcMap["pathPrefix"] = func() string { return "" }
	funcMap["buildVersion"] = func() string { return version.Revision }

	return &BaseUI{logger: logger, menuTmpl: menuTmpl, tmplFuncs: funcMap, tmplVariables: tmplVariables, externalPrefix: externalPrefix, prefixHeader: prefixHeader, component: component}
}
func (bu *BaseUI) serveStaticAsset(w http.ResponseWriter, req *http.Request) {
	fp := route.Param(req.Context(), "filepath")
	fp = filepath.Join("pkg/ui/static", fp)
	bu.serveAsset(fp, w, req)
}

func (bu *BaseUI) serveReactUI(w http.ResponseWriter, req *http.Request) {
	fp := route.Param(req.Context(), "filepath")
	for _, rp := range reactAppPaths {
		if fp != rp {
			continue
		}
		bu.serveReactIndex("pkg/ui/static/react/index.html", w, req)
		return
	}
	fp = filepath.Join("pkg/ui/static/react/", fp)
	bu.serveAsset(fp, w, req)
}

func (bu *BaseUI) serveReactIndex(index string, w http.ResponseWriter, req *http.Request) {
	_, file, err := bu.getAssetFile(index)
	if err != nil {
		level.Warn(bu.logger).Log("msg", "Could not get file", "err", err, "file", index)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	prefix := GetWebPrefix(bu.logger, bu.externalPrefix, bu.prefixHeader, req)

	tmpl, err := template.New("").Funcs(bu.tmplFuncs).
		Funcs(template.FuncMap{"pathPrefix": absolutePrefix(prefix)}).
		Parse(string(file))

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := tmpl.Execute(w, bu.tmplVariables); err != nil {
		level.Warn(bu.logger).Log("msg", "template expansion failed", "err", err)
	}
}

func (bu *BaseUI) getAssetFile(filename string) (os.FileInfo, []byte, error) {
	info, err := AssetInfo(filename)
	if err != nil {
		return nil, nil, err
	}
	file, err := Asset(filename)
	if err != nil {
		return nil, nil, err
	}
	return info, file, nil
}

func (bu *BaseUI) serveAsset(fp string, w http.ResponseWriter, req *http.Request) {
	info, file, err := bu.getAssetFile(fp)
	if err != nil {
		level.Warn(bu.logger).Log("msg", "Could not get file", "err", err, "file", fp)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	http.ServeContent(w, req, info.Name(), info.ModTime(), bytes.NewReader(file))
}

func (bu *BaseUI) getTemplate(name string) (string, error) {
	baseTmpl, err := Asset("pkg/ui/templates/_base.html")
	if err != nil {
		return "", errors.Errorf("error reading base template: %s", err)
	}
	menuTmpl, err := Asset(filepath.Join("pkg/ui/templates", bu.menuTmpl))
	if err != nil {
		return "", errors.Errorf("error reading menu template %s: %s", bu.menuTmpl, err)
	}
	pageTmpl, err := Asset(filepath.Join("pkg/ui/templates", name))
	if err != nil {
		return "", errors.Errorf("error reading page template %s: %s", name, err)
	}
	return string(baseTmpl) + string(menuTmpl) + string(pageTmpl), nil
}

func (bu *BaseUI) executeTemplate(w http.ResponseWriter, name string, prefix string, data interface{}) {
	text, err := bu.getTemplate(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	t, err := template.New("").Funcs(bu.tmplFuncs).
		Funcs(template.FuncMap{"pathPrefix": absolutePrefix(prefix)}).
		Parse(text)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := t.Execute(w, data); err != nil {
		level.Warn(bu.logger).Log("msg", "template expansion failed", "err", err)
	}
}

func absolutePrefix(prefix string) func() string {
	return func() string {
		if prefix == "" {
			return ""
		}
		return path.Join("/", prefix)
	}
}

// GetWebPrefix sanitizes an external URL path prefix value.
// A value provided by web.external-prefix flag is preferred over the one supplied through an HTTP header.
func GetWebPrefix(logger log.Logger, externalPrefix, prefixHeader string, r *http.Request) string {
	prefix := r.Header.Get(prefixHeader)

	// Ignore web.prefix-header value if web.external-prefix is defined.
	if len(externalPrefix) > 0 {
		prefix = externalPrefix
	}

	// Even if rfc2616 suggests that Location header "value consists of a single absolute URI", browsers
	// support relative location too. So for extra security, scheme and host parts are stripped from a dynamic prefix.
	prefix, err := SanitizePrefix(prefix)
	if err != nil {
		level.Warn(logger).Log("msg", "Could not parse value of UI external prefix", "prefix", prefix, "err", err)
	}

	return prefix
}

// SanitizePrefix makes sure that path prefix value is valid.
// A prefix is returned without a trailing slash. Hence empty string is returned for the root path.
func SanitizePrefix(prefix string) (string, error) {
	u, err := url.Parse(prefix)
	if err != nil {
		return "", err
	}

	// Remove double slashes, convert to absolute path.
	sanitizedPrefix := strings.TrimPrefix(path.Clean(u.Path), ".")
	sanitizedPrefix = strings.TrimSuffix(sanitizedPrefix, "/")

	return sanitizedPrefix, nil
}
