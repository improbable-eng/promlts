package indexcache

import (
	"io/ioutil"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// BinaryCache is a binary index cache.
type BinaryCache struct {
	IndexCache

	logger log.Logger
}

// WriteIndexCache writes an index cache into the specified filename.
func (c *BinaryCache) WriteIndexCache(indexFn string, fn string) error {
	indexFile, err := fileutil.OpenMmapFile(indexFn)
	if err != nil {
		return errors.Wrapf(err, "open mmap index file %s", indexFn)
	}
	defer runutil.CloseWithLogOnErr(c.logger, indexFile, "close index cache mmap file from %s", indexFn)

	b := realByteSlice(indexFile.Bytes())
	indexr, err := index.NewReader(b)
	if err != nil {
		return errors.Wrap(err, "open index reader")
	}
	defer runutil.CloseWithLogOnErr(c.logger, indexr, "load index cache reader")

	// We assume reader verified index already.
	symbols, err := getSymbolTableBinary(b)
	if err != nil {
		return err
	}

	// Now it is time to write it.
	w, err := index.NewWriter(fn)
	if err != nil {
		return err
	}
	defer runutil.CloseWithLogOnErr(c.logger, w, "index writer")

	err = w.AddSymbols(symbols)
	if err != nil {
		return err
	}

	// Extract label value indices.
	lnames, err := indexr.LabelNames()
	if err != nil {
		return errors.Wrap(err, "read label indices")
	}
	for _, ln := range lnames {
		tpls, err := indexr.LabelValues(ln)
		if err != nil {
			return errors.Wrap(err, "get label values")
		}
		vals := make([]string, 0, tpls.Len())

		for i := 0; i < tpls.Len(); i++ {
			v, err := tpls.At(i)
			if err != nil {
				return errors.Wrap(err, "get label value")
			}
			if len(v) != 1 {
				return errors.Errorf("unexpected tuple length %d", len(v))
			}
			vals = append(vals, v[0])
		}

		err = w.WriteLabelIndex(lnames, vals)
		if err != nil {
			return errors.Wrap(err, "write label indices")
		}
	}

	// Extract postings ranges.
	pranges, err := indexr.PostingsRanges()
	if err != nil {
		return errors.Wrap(err, "read postings ranges")
	}
	for l := range pranges {
		p, err := indexr.Postings(l.Name, l.Value)
		if err != nil {
			return errors.Wrap(err, "postings reader")
		}
		err = w.WritePostings(l.Name, l.Value, p)
		if err != nil {
			return errors.Wrap(err, "postings write")
		}
	}

	return nil
}

// ReadIndexCache reads the index cache from the specified file.
func (c *BinaryCache) ReadIndexCache(fn string) (version int,
	symbols map[uint32]string,
	lvals map[string][]string,
	postings map[labels.Label]index.Range,
	err error) {
	indexFile, err := ioutil.ReadFile(fn)
	if err != nil {
		return 0, nil, nil, nil, errors.Wrapf(err, "open index file %s", fn)
	}
	b := realByteSlice(indexFile)
	indexr, err := index.NewReader(b)
	if err != nil {
		return 0, nil, nil, nil, errors.Wrap(err, "open index reader")
	}
	defer runutil.CloseWithLogOnErr(c.logger, indexr, "load index cache reader")
	version = indexr.Version()

	// We assume reader verified index already.
	symbols, err = getSymbolTableJSON(b)
	if err != nil {
		return 0, nil, nil, nil, errors.Wrap(err, "read symbol table")
	}

	// Extract label value indices.
	lnames, err := indexr.LabelNames()
	if err != nil {
		return 0, nil, nil, nil, errors.Wrap(err, "read label indices")
	}

	for _, ln := range lnames {
		tpls, err := indexr.LabelValues(ln)
		if err != nil {
			return 0, nil, nil, nil, errors.Wrap(err, "get label values")
		}
		vals := make([]string, 0, tpls.Len())

		for i := 0; i < tpls.Len(); i++ {
			v, err := tpls.At(i)
			if err != nil {
				return 0, nil, nil, nil, errors.Wrap(err, "get label value")
			}
			if len(v) != 1 {
				return 0, nil, nil, nil, errors.Errorf("unexpected tuple length %d", len(v))
			}
			vals = append(vals, v[0])
		}

		lvals[ln] = vals
	}

	// Extract postings ranges.
	postings, err = indexr.PostingsRanges()
	if err != nil {
		return 0, nil, nil, nil, errors.Wrap(err, "read postings ranges")
	}

	return version, symbols, lvals, postings, nil
}
