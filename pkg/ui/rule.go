// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package ui

import (
	"fmt"
	"html/template"
	"math"
	"net/http"
	"os"
	"path"
	"regexp"
	"time"

	"github.com/prometheus/common/version"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/rules"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	thanosrule "github.com/thanos-io/thanos/pkg/rule"
)

type Rule struct {
	*BaseUI

	flagsMap map[string]string

	ruleManager *thanosrule.Manager
	queryURL    string
	reg         prometheus.Registerer

	cwd   string
	birth time.Time
}

func NewRuleUI(logger log.Logger, reg prometheus.Registerer, ruleManager *thanosrule.Manager, queryURL string, flagsMap map[string]string) *Rule {
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "<error retrieving current working directory>"
	}
	return &Rule{
		BaseUI:      NewBaseUI(logger, "rule_menu.html", ruleTmplFuncs(queryURL)),
		flagsMap:    flagsMap,
		ruleManager: ruleManager,
		queryURL:    queryURL,
		reg:         reg,
		birth:       time.Now(),
		cwd:         cwd,
	}
}

func ruleTmplFuncs(queryURL string) template.FuncMap {
	return template.FuncMap{
		"since": func(t time.Time) time.Duration {
			return time.Since(t) / time.Millisecond * time.Millisecond
		},
		"alertStateToClass": func(as rules.AlertState) string {
			switch as {
			case rules.StateInactive:
				return "success"
			case rules.StatePending:
				return "warning"
			case rules.StateFiring:
				return "danger"
			default:
				panic("unknown alert state")
			}
		},
		"ruleHealthToClass": func(rh rules.RuleHealth) string {
			switch rh {
			case rules.HealthUnknown:
				return "warning"
			case rules.HealthGood:
				return "success"
			default:
				return "danger"
			}
		},
		"queryURL": func() string { return queryURL },
		"reReplaceAll": func(pattern, repl, text string) string {
			re := regexp.MustCompile(pattern)
			return re.ReplaceAllString(text, repl)
		},
		"humanizeDuration": func(v float64) string {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return fmt.Sprintf("%.4g", v)
			}
			if v == 0 {
				return fmt.Sprintf("%.4gs", v)
			}
			if math.Abs(v) >= 1 {
				sign := ""
				if v < 0 {
					sign = "-"
					v = -v
				}
				seconds := int64(v) % 60
				minutes := (int64(v) / 60) % 60
				hours := (int64(v) / 60 / 60) % 24
				days := (int64(v) / 60 / 60 / 24)
				// For days to minutes, we display seconds as an integer.
				if days != 0 {
					return fmt.Sprintf("%s%dd %dh %dm %ds", sign, days, hours, minutes, seconds)
				}
				if hours != 0 {
					return fmt.Sprintf("%s%dh %dm %ds", sign, hours, minutes, seconds)
				}
				if minutes != 0 {
					return fmt.Sprintf("%s%dm %ds", sign, minutes, seconds)
				}
				// For seconds, we display 4 significant digits.
				return fmt.Sprintf("%s%.4gs", sign, v)
			}
			prefix := ""
			for _, p := range []string{"m", "u", "n", "p", "f", "a", "z", "y"} {
				if math.Abs(v) >= 1 {
					break
				}
				prefix = p
				v *= 1000
			}
			return fmt.Sprintf("%.4g%ss", v, prefix)
		},
	}
}

func (ru *Rule) alerts(w http.ResponseWriter, r *http.Request) {
	var groups []thanosrule.Group
	for _, group := range ru.ruleManager.RuleGroups() {
		if group.HasAlertingRules() {
			groups = append(groups, group)
		}
	}

	alertStatus := AlertStatus{
		Groups: groups,
		AlertStateToRowClass: map[rules.AlertState]string{
			rules.StateInactive: "success",
			rules.StatePending:  "warning",
			rules.StateFiring:   "danger",
		},
		Counts: alertCounts(groups),
	}

	prefix := GetWebPrefix(ru.logger, ru.flagsMap, r)

	// TODO(bwplotka): Update HTML to include partial response.
	ru.executeTemplate(w, "alerts.html", prefix, alertStatus)
}

func (ru *Rule) rules(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(ru.logger, ru.flagsMap, r)

	// TODO(bwplotka): Update HTML to include partial response.
	ru.executeTemplate(w, "rules.html", prefix, ru.ruleManager)
}

// Root redirects / requests to /graph, taking into account the path prefix value.
func (ru *Rule) root(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(ru.logger, ru.flagsMap, r)

	http.Redirect(w, r, path.Join(prefix, "/alerts"), http.StatusFound)
}

func (ru *Rule) status(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(ru.logger, ru.flagsMap, r)

	ru.executeTemplate(w, "status.html", prefix, struct {
		Birth   time.Time
		CWD     string
		Version thanosVersion
	}{
		Birth: ru.birth,
		CWD:   ru.cwd,
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

func (ru *Rule) Register(r *route.Router, ins extpromhttp.InstrumentationMiddleware) {
	instrf := func(name string, next func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
		return ins.NewHandler(name, http.HandlerFunc(next))
	}

	r.Get("/", instrf("root", ru.root))
	r.Get("/alerts", instrf("alerts", ru.alerts))
	r.Get("/rules", instrf("rules", ru.rules))
	r.Get("/status", instrf("status", ru.status))

	r.Get("/static/*filepath", instrf("static", ru.serveStaticAsset))
}

// AlertStatus bundles alerting rules and the mapping of alert states to row classes.
type AlertStatus struct {
	Groups               []thanosrule.Group
	AlertStateToRowClass map[rules.AlertState]string
	Counts               AlertByStateCount
}

type AlertByStateCount struct {
	Inactive int32
	Pending  int32
	Firing   int32
}

func alertCounts(groups []thanosrule.Group) AlertByStateCount {
	result := AlertByStateCount{}
	for _, group := range groups {
		for _, alert := range group.AlertingRules() {
			switch alert.State() {
			case rules.StateInactive:
				result.Inactive++
			case rules.StatePending:
				result.Pending++
			case rules.StateFiring:
				result.Firing++
			}
		}
	}
	return result
}
