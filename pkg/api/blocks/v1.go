// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package v1

import (
	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/route"
	"github.com/thanos-io/thanos/pkg/api"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	extpromhttp "github.com/thanos-io/thanos/pkg/extprom/http"
	"github.com/thanos-io/thanos/pkg/logging"
)

// BlocksAPI is a very simple API used by Thanos Block Viewer.
type BlocksAPI struct {
	baseAPI          *api.BaseAPI
	logger           log.Logger
	globalBlocksInfo *BlocksInfo
	loadedBlocksInfo *BlocksInfo
}

type BlocksInfo struct {
	Label       string          `json:"label"`
	Blocks      []metadata.Meta `json:"blocks"`
	RefreshedAt time.Time       `json:"refreshedAt"`
	Err         error           `json:"err"`
}

// NewBlocksAPI creates a simple API to be used by Thanos Block Viewer.
func NewBlocksAPI(logger log.Logger, label string, flagsMap map[string]string, configFilesMap map[string]string) *BlocksAPI {
	return &BlocksAPI{
		baseAPI: api.NewBaseAPI(logger, flagsMap, configFilesMap),
		logger:  logger,
		globalBlocksInfo: &BlocksInfo{
			Blocks: []metadata.Meta{},
			Label:  label,
		},
		loadedBlocksInfo: &BlocksInfo{
			Blocks: []metadata.Meta{},
			Label:  label,
		},
	}
}

func (bapi *BlocksAPI) Register(r *route.Router, tracer opentracing.Tracer, logger log.Logger, ins extpromhttp.InstrumentationMiddleware, logMiddleware *logging.HTTPServerMiddleware) {
	bapi.baseAPI.Register(r, tracer, logger, ins, logMiddleware)

	instr := api.GetInstr(tracer, logger, ins, logMiddleware)

	r.Get("/blocks", instr("blocks", bapi.blocks))
}

func (bapi *BlocksAPI) blocks(r *http.Request) (interface{}, []error, *api.ApiError) {
	viewParam := r.URL.Query().Get("view")
	if viewParam == "loaded" {
		return bapi.loadedBlocksInfo, nil, nil
	}
	return bapi.globalBlocksInfo, nil, nil
}

func (b *BlocksInfo) set(blocks []metadata.Meta, err error) {
	if err != nil {
		// Last view is maintained.
		b.RefreshedAt = time.Now()
		b.Err = err
		return
	}

	b.RefreshedAt = time.Now()
	b.Blocks = blocks
	b.Err = err
}

// SetGlobal updates the global blocks' metadata in the API.
func (bapi *BlocksAPI) SetGlobal(blocks []metadata.Meta, err error) {
	bapi.globalBlocksInfo.set(blocks, err)
}

// SetLoaded updates the local blocks' metadata in the API.
func (bapi *BlocksAPI) SetLoaded(blocks []metadata.Meta, err error) {
	bapi.loadedBlocksInfo.set(blocks, err)
}
