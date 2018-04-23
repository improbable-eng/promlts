package block

import (
	"encoding/json"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// IndexCacheFilename is the canonical name for index cache files.
const IndexCacheFilename = "index.cache.json"

type postingsRange struct {
	Name, Value string
	Start, End  int64
}

type indexCache struct {
	Version     int
	Symbols     map[uint32]string
	LabelValues map[string][]string
	Postings    []postingsRange
}

// WriteIndexCache writes a cache file containing the first lookup stages
// for an index file.
func WriteIndexCache(fn string, r *index.Reader) error {
	f, err := os.Create(fn)
	if err != nil {
		return errors.Wrap(err, "create file")
	}
	defer f.Close()

	v := indexCache{
		Version:     r.Version(),
		Symbols:     r.SymbolTable(),
		LabelValues: map[string][]string{},
	}

	// Extract label value indices.
	lnames, err := r.LabelIndices()
	if err != nil {
		return errors.Wrap(err, "read label indices")
	}
	for _, lns := range lnames {
		if len(lns) != 1 {
			continue
		}
		ln := lns[0]

		tpls, err := r.LabelValues(ln)
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
		v.LabelValues[ln] = vals
	}

	// Extract postings ranges.
	pranges, err := r.PostingsRanges()
	if err != nil {
		return errors.Wrap(err, "read postings ranges")
	}
	for l, rng := range pranges {
		v.Postings = append(v.Postings, postingsRange{
			Name:  l.Name,
			Value: l.Value,
			Start: rng.Start,
			End:   rng.End,
		})
	}

	if err := json.NewEncoder(f).Encode(&v); err != nil {
		return errors.Wrap(err, "encode file")
	}
	return nil
}

// ReadIndexCache reads an index cache file.
func ReadIndexCache(fn string) (
	version int,
	symbols map[uint32]string,
	lvals map[string][]string,
	postings map[labels.Label]index.Range,
	err error,
) {
	f, err := os.Open(fn)
	if err != nil {
		return 0, nil, nil, nil, errors.Wrap(err, "open file")
	}
	defer f.Close()

	var v indexCache
	if err := json.NewDecoder(f).Decode(&v); err != nil {
		return 0, nil, nil, nil, errors.Wrap(err, "decode file")
	}
	strs := map[string]string{}
	lvals = make(map[string][]string, len(v.LabelValues))
	postings = make(map[labels.Label]index.Range, len(v.Postings))

	// Most strings we encounter are duplicates. Dedup string objects that we keep
	// around after the function returns to reduce total memory usage.
	// NOTE(fabxc): it could even make sense to deduplicate globally.
	getStr := func(s string) string {
		if cs, ok := strs[s]; ok {
			return cs
		}
		strs[s] = s
		return s
	}

	for o, s := range v.Symbols {
		v.Symbols[o] = getStr(s)
	}
	for ln, vals := range v.LabelValues {
		for i := range vals {
			vals[i] = getStr(vals[i])
		}
		lvals[getStr(ln)] = vals
	}
	for _, e := range v.Postings {
		l := labels.Label{
			Name:  getStr(e.Name),
			Value: getStr(e.Value),
		}
		postings[l] = index.Range{Start: e.Start, End: e.End}
	}
	return v.Version, v.Symbols, lvals, postings, nil
}

// VerifyIndex does a full run over a block index and verifies that it fulfills the order invariants.
func VerifyIndex(fn string, minTime int64, maxTime int64) error {
	stats, err := GatherIndexIssueStats(fn, minTime, maxTime)
	if err != nil {
		return err
	}

	err = stats.ErrSummary()
	if err != nil {
		return err
	}

	return nil
}

type IndexIssueStats struct {
	Total int

	OutOfOrderCount int
	OutOfOrderSum   int
	ExactSum        int

	// Chunks that are before or after time range in meta.
	Outsiders int
	// Outsiders that will be never accessed. They are completely out of time range specified in block meta.
	CompleteOutsiders int
}

func (i IndexIssueStats) ErrSummary() error {
	if i.OutOfOrderCount > 0 {
		return errors.Errorf("%d/%d series have an average of %.3f out-of-order chunks. "+
			"%.3f of these are exact duplicates (in terms of data and time range). Outsiders: %d, complete outsiders: %d",
			i.OutOfOrderCount, i.Total, float64(i.OutOfOrderSum)/float64(i.OutOfOrderCount),
			float64(i.ExactSum)/float64(i.OutOfOrderSum), i.Outsiders, i.CompleteOutsiders)
	}

	if i.Outsiders > 0 {
		return errors.Errorf("No chunks are out of order, but found some outsider blocks. (Blocks that outside of block time range): %d. Complete: %d",
			i.Outsiders, i.CompleteOutsiders)
	}

	return nil
}

// GatherIndexIssueStats returns useful counters as well as outsider chunks (chunks outside of block time range) that helps to assess index health.
func GatherIndexIssueStats(fn string, minTime int64, maxTime int64) (stats IndexIssueStats, err error) {
	r, err := index.NewFileReader(fn)
	if err != nil {
		return stats, errors.Wrap(err, "open index file")
	}
	defer r.Close()

	p, err := r.Postings(index.AllPostingsKey())
	if err != nil {
		return stats, errors.Wrap(err, "get all postings")
	}
	var (
		lastLset labels.Labels
		lset     labels.Labels
		chks     []chunks.Meta
	)

	for p.Next() {
		lastLset = append(lastLset[:0], lset...)

		id := p.At()
		stats.Total++

		if err := r.Series(id, &lset, &chks); err != nil {
			return stats, errors.Wrap(err, "read series")
		}
		if len(lset) == 0 {
			return stats, errors.Errorf("empty label set detected for series %d", id)
		}
		if lastLset != nil && labels.Compare(lastLset, lset) >= 0 {
			return stats, errors.Errorf("series %v out of order; previous %v", lset, lastLset)
		}
		l0 := lset[0]
		for _, l := range lset[1:] {
			if l.Name <= l0.Name {
				return stats, errors.Errorf("out-of-order label set %s for series %d", lset, id)
			}
			l0 = l
		}
		if len(chks) == 0 {
			return stats, errors.Errorf("empty chunks for series %d", id)
		}

		ooo := 0
		if chks[0].MinTime < minTime || chks[0].MaxTime > maxTime {
			stats.Outsiders++
			if chks[0].MinTime > maxTime || chks[0].MaxTime < minTime {
				stats.CompleteOutsiders++
			}
		}
		for i, c := range chks[1:] {
			c0 := chks[i]

			if c.MinTime < minTime || c.MaxTime > maxTime {
				stats.Outsiders++
				if c.MinTime > maxTime || c.MaxTime < minTime {
					stats.CompleteOutsiders++
				}
			}

			if c.MinTime > c0.MaxTime {
				continue
			}

			// Chunks overlaps or duplicates.
			if c.MinTime == c0.MinTime && c.MaxTime == c0.MaxTime {
				ca := crc32.Checksum(c0.Chunk.Bytes(), castagnoli)
				cb := crc32.Checksum(c.Chunk.Bytes(), castagnoli)
				if ca == cb {
					// Duplicate.
					stats.ExactSum++
				}
				ooo++
			}
		}
		if ooo > 0 {
			stats.OutOfOrderCount++
			stats.OutOfOrderSum += ooo
		}
	}
	if p.Err() != nil {
		return stats, errors.Wrap(err, "walk postings")
	}

	return stats, nil
}

// Repair open the block with given id in dir and creates a new one with the same data.
// It:
// - removes out of order duplicates
// - all "complete" outsiders (they will not accessed anyway)
// Fixable inconsistencies are resolved in the new block.
func Repair(dir string, id ulid.ULID) (resid ulid.ULID, err error) {
	bdir := filepath.Join(dir, id.String())
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	resid = ulid.MustNew(ulid.Now(), entropy)

	meta, err := ReadMetaFile(bdir)
	if err != nil {
		return resid, errors.Wrap(err, "read meta file")
	}
	if meta.Thanos.Downsample.Resolution > 0 {
		return resid, errors.New("cannot repair downsampled block")
	}

	b, err := tsdb.OpenBlock(bdir, nil)
	if err != nil {
		return resid, errors.Wrap(err, "open block")
	}
	defer b.Close()

	indexr, err := b.Index()
	if err != nil {
		return resid, errors.Wrap(err, "open index")
	}

	chunkr, err := b.Chunks()
	if err != nil {
		return resid, errors.Wrap(err, "open chunks")
	}

	resdir := filepath.Join(dir, resid.String())

	chunkw, err := chunks.NewWriter(filepath.Join(resdir, ChunksDirname))
	if err != nil {
		return resid, errors.Wrap(err, "open chunk writer")
	}
	defer chunkw.Close()

	indexw, err := index.NewWriter(filepath.Join(resdir, IndexFilename))
	if err != nil {
		return resid, errors.Wrap(err, "open index writer")
	}
	defer indexw.Close()

	// TODO(fabxc): adapt so we properly handle the version once we update to an upstream
	// that has multiple.
	resmeta := *meta
	resmeta.ULID = resid
	resmeta.Stats = tsdb.BlockStats{} // reset stats

	if err := rewrite(indexr, chunkr, indexw, chunkw, &resmeta); err != nil {
		return resid, errors.Wrap(err, "rewrite block")
	}
	if err := WriteMetaFile(resdir, &resmeta); err != nil {
		return resid, err
	}
	return resid, nil
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// sanitizeChunkSequence ensures order of the input chunks and drops any duplicates.
// It errors if the sequence contains non-dedupable overlaps.
func sanitizeChunkSequence(chks []chunks.Meta, mint int64, maxt int64) ([]chunks.Meta, error) {
	if len(chks) == 0 {
		return nil, nil
	}
	// First, ensure that chunks are ordered by their start time.
	sort.Slice(chks, func(i, j int) bool {
		return chks[i].MinTime < chks[j].MinTime
	})

	// Remove duplicates and complete outsiders.
	repl := make([]chunks.Meta, 0, len(chks))
	for i, c := range chks {
		if c.MinTime > maxt || c.MaxTime < mint {
			// "Complete" outsider. Ignore.
			continue
		}

		if i == 0 {
			repl = append(repl, c)
			continue
		}

		last := repl[i-1]

		if c.MinTime > last.MaxTime {
			repl = append(repl, c)
			continue
		}

		// Verify that the overlapping chunks are exact copies so we can safely discard
		// the current one.
		if c.MinTime != last.MinTime || c.MaxTime != last.MaxTime {
			return nil, errors.Errorf("non-sequential chunks not equal: [%d, %d] and [%d, %d]",
				last.MaxTime, last.MaxTime, c.MinTime, c.MaxTime)
		}
		ca := crc32.Checksum(last.Chunk.Bytes(), castagnoli)
		cb := crc32.Checksum(c.Chunk.Bytes(), castagnoli)

		if ca != cb {
			return nil, errors.Errorf("non-sequential chunks not equal: %x and %x", ca, cb)
		}
	}

	return repl, nil
}

// rewrite writes all data from the readers back into the writers while cleaning
// up mis-ordered and duplicated chunks.
func rewrite(
	indexr tsdb.IndexReader, chunkr tsdb.ChunkReader,
	indexw tsdb.IndexWriter, chunkw tsdb.ChunkWriter,
	meta *Meta,
) error {
	symbols, err := indexr.Symbols()
	if err != nil {
		return err
	}
	if err := indexw.AddSymbols(symbols); err != nil {
		return err
	}

	all, err := indexr.Postings(index.AllPostingsKey())
	if err != nil {
		return err
	}
	all = indexr.SortedPostings(all)

	// We fully rebuild the postings list index from merged series.
	var (
		postings = index.NewMemPostings()
		values   = map[string]stringset{}
		i        = uint64(0)
	)

	var lset labels.Labels
	var chks []chunks.Meta

	for all.Next() {
		id := all.At()

		if err := indexr.Series(id, &lset, &chks); err != nil {
			return err
		}
		for i, c := range chks {
			chks[i].Chunk, err = chunkr.Chunk(c.Ref)
			if err != nil {
				return err
			}
		}
		chks, err := sanitizeChunkSequence(chks, meta.MinTime, meta.MaxTime)
		if err != nil {
			return err
		}

		if len(chks) == 0 {
			continue
		}

		if err := chunkw.WriteChunks(chks...); err != nil {
			return errors.Wrap(err, "write chunks")
		}
		if err := indexw.AddSeries(i, lset, chks...); err != nil {
			return errors.Wrap(err, "add series")
		}

		meta.Stats.NumChunks += uint64(len(chks))
		meta.Stats.NumSeries++

		for _, chk := range chks {
			meta.Stats.NumSamples += uint64(chk.Chunk.NumSamples())
		}

		for _, l := range lset {
			valset, ok := values[l.Name]
			if !ok {
				valset = stringset{}
				values[l.Name] = valset
			}
			valset.set(l.Value)
		}
		postings.Add(i, lset)
		i++
	}
	if all.Err() != nil {
		return errors.Wrap(all.Err(), "iterate series")
	}

	s := make([]string, 0, 256)
	for n, v := range values {
		s = s[:0]

		for x := range v {
			s = append(s, x)
		}
		if err := indexw.WriteLabelIndex([]string{n}, s); err != nil {
			return errors.Wrap(err, "write label index")
		}
	}

	for _, l := range postings.SortedKeys() {
		if err := indexw.WritePostings(l.Name, l.Value, postings.Get(l.Name, l.Value)); err != nil {
			return errors.Wrap(err, "write postings")
		}
	}
	return nil
}

type stringset map[string]struct{}

func (ss stringset) set(s string) {
	ss[s] = struct{}{}
}

func (ss stringset) has(s string) bool {
	_, ok := ss[s]
	return ok
}

func (ss stringset) String() string {
	return strings.Join(ss.slice(), ",")
}

func (ss stringset) slice() []string {
	slice := make([]string, 0, len(ss))
	for k := range ss {
		slice = append(slice, k)
	}
	sort.Strings(slice)
	return slice
}
