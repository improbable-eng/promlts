package ui

import (
	"html/template"
	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
)

// Bucket is a web UI representing state of buckets as a timeline.
type Bucket struct {
	*BaseUI
	Blocks      template.JS
	RefreshedAt time.Time
	Err         error
}

func NewBucketUI(logger log.Logger) *Bucket {
	return &Bucket{
		BaseUI: NewBaseUI(logger, "bucket_menu.html", queryTmplFuncs()),
		Blocks: "[]",
	}
}

// Register registers http routes for bucket UI.
func (b *Bucket) Register(r *route.Router) {
	instrf := prometheus.InstrumentHandlerFunc

	r.Get("/", instrf("root", b.root))
	r.Get("/static/*filepath", instrf("static", b.serveStaticAsset))
}

// Handle / of bucket UIs
func (b *Bucket) root(w http.ResponseWriter, r *http.Request) {
	b.executeTemplate(w, "bucket.html", "", b)
}

func (b *Bucket) Set(data string, err error) {
	b.RefreshedAt = time.Now()
	b.Blocks = template.JS(string(data))
	b.Err = err
}
