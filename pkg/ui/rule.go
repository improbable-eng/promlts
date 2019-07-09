package ui

import (
	"fmt"
	"html/template"
	"math"
	"net/http"
	"path"
	"regexp"
	"sort"
	"time"

	"github.com/go-kit/kit/log"
	extpromhttp "github.com/improbable-eng/thanos/pkg/extprom/http"
	thanosrule "github.com/improbable-eng/thanos/pkg/rule"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/rules"
)

type Rule struct {
	*BaseUI

	flagsMap map[string]string

	ruleManagers thanosrule.Managers
	queryURL     string
}

func NewRuleUI(logger log.Logger, ruleManagers map[storepb.PartialResponseStrategy]*rules.Manager, queryURL string, flagsMap map[string]string) *Rule {
	return &Rule{
		BaseUI:       NewBaseUI(logger, "rule_menu.html", ruleTmplFuncs(queryURL)),
		flagsMap:     flagsMap,
		ruleManagers: ruleManagers,
		queryURL:     queryURL,
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
				// For seconds, we display 4 significant digts.
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
	alerts := ru.ruleManagers.AlertingRules()
	alertsSorter := byAlertStateAndNameSorter{alerts: alerts}
	sort.Sort(alertsSorter)

	alertStatus := AlertStatus{
		AlertingRules: alertsSorter.alerts,
		AlertStateToRowClass: map[rules.AlertState]string{
			rules.StateInactive: "success",
			rules.StatePending:  "warning",
			rules.StateFiring:   "danger",
		},
	}

	prefix := GetWebPrefix(ru.logger, ru.flagsMap, r)

	// TODO(bwplotka): Update HTML to include partial response.
	ru.executeTemplate(w, "alerts.html", prefix, alertStatus)
}

func (ru *Rule) rules(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(ru.logger, ru.flagsMap, r)

	// TODO(bwplotka): Update HTML to include partial response.
	ru.executeTemplate(w, "rules.html", prefix, ru.ruleManagers)
}

// root redirects / requests to /graph, taking into account the path prefix value
func (ru *Rule) root(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(ru.logger, ru.flagsMap, r)

	http.Redirect(w, r, path.Join(prefix, "/alerts"), http.StatusFound)
}

func (ru *Rule) Register(r *route.Router, ins extpromhttp.ServerInstrumentor) {
	instrf := func(name string, next func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
		return ins.NewInstrumentedHandler(name, http.HandlerFunc(next))
	}

	r.Get("/", instrf("root", ru.root))
	r.Get("/alerts", instrf("alerts", ru.alerts))
	r.Get("/rules", instrf("rules", ru.rules))

	r.Get("/static/*filepath", instrf("static", ru.serveStaticAsset))
}

// AlertStatus bundles alerting rules and the mapping of alert states to row classes.
type AlertStatus struct {
	AlertingRules        []thanosrule.AlertingRule
	AlertStateToRowClass map[rules.AlertState]string
}

type byAlertStateAndNameSorter struct {
	alerts []thanosrule.AlertingRule
}

func (s byAlertStateAndNameSorter) Len() int {
	return len(s.alerts)
}

func (s byAlertStateAndNameSorter) Less(i, j int) bool {
	return s.alerts[i].State() > s.alerts[j].State() ||
		(s.alerts[i].State() == s.alerts[j].State() &&
			s.alerts[i].Name() < s.alerts[j].Name())
}

func (s byAlertStateAndNameSorter) Swap(i, j int) {
	s.alerts[i], s.alerts[j] = s.alerts[j], s.alerts[i]
}
