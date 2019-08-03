package ui

import (
	"html/template"
	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/server"
)

// Bucket is a web UI representing state of buckets as a timeline.
type Bucket struct {
	*BaseUI
	// Unique Prometheus label that identifies each shard, used as the title. If
	// not present, all labels are displayed externally as a legend.
	Label       string
	Blocks      template.JS
	RefreshedAt time.Time
	flagsMap    map[string]string
	Err         error
}

func NewBucketUI(logger log.Logger, label string, flagsMap map[string]string) *Bucket {
	return &Bucket{
		BaseUI:   NewBaseUI(logger, "bucket_menu.html", queryTmplFuncs()),
		Blocks:   "[]",
		Label:    label,
		flagsMap: flagsMap,
	}
}

// Register registers http routes for bucket UI.
func (b *Bucket) Register(s server.Server, ins extpromhttp.InstrumentationMiddleware) {
	instrf := func(name string, next func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
		return ins.NewHandler(name, http.HandlerFunc(next))
	}

	s.Handle("/", instrf("root", b.root))
	s.Handle("/static/*filepath", instrf("static", b.serveStaticAsset))
}

// Handle / of bucket UIs.
func (b *Bucket) root(w http.ResponseWriter, r *http.Request) {
	prefix := GetWebPrefix(b.logger, b.flagsMap, r)
	b.executeTemplate(w, "bucket.html", prefix, b)
}

func (b *Bucket) Set(data string, err error) {
	b.RefreshedAt = time.Now()
	b.Blocks = template.JS(string(data))
	b.Err = err
}
