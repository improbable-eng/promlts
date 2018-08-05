package cluster

import (
	stdlog "log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/memberlist"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Peer is a single peer in a gossip cluster.
type Peer struct {
	logger   log.Logger
	mlistMtx sync.RWMutex
	mlist    *memberlist.Memberlist
	stopc    chan struct{}

	cfg             *memberlist.Config
	discovery       PeerDiscovery
	initialPeers    []string
	advertiseAddr   string
	refreshInterval time.Duration

	data                 *data
	gossipMsgsReceived   prometheus.Counter
	gossipClusterMembers prometheus.Gauge

	// Own External gRPC StoreAPI host:port (if any) to propagate to other peers.
	advertiseStoreAPIAddr string
	// Own External HTTP QueryAPI host:port (if any) to propagate to other peers.
	advertiseQueryAPIAddress string
}

const (
	DefaultRefreshInterval = 60 * time.Second

	// Peer's network types. These are used as a predefined peer configurations for a specified network type.
	LocalNetworkPeerType = "local"
	LanNetworkPeerType   = "lan"
	WanNetworkPeerType   = "wan"
)

var (
	// NetworkPeerTypes is a list of available peers' network types.
	NetworkPeerTypes = []string{LocalNetworkPeerType, LanNetworkPeerType, WanNetworkPeerType}
)

// PeerType describes a peer's role in the cluster.
type PeerType string

// Constants holding valid PeerType values.
const (
	// PeerTypeStore is for peers that implements StoreAPI and are used for browsing historical data.
	PeerTypeStore = "store"
	// PeerTypeSource is for peers that implements StoreAPI and are used for scraping data. They tend to
	// have data accessible only for short period.
	PeerTypeSource = "source"

	// PeerTypeQuery is for peers that implements QueryAPI and are used for querying the metrics.
	PeerTypeQuery = "query"
)

// PeerState contains state for the peer.
type PeerState struct {
	// Type represents type of the peer holding the state.
	Type PeerType

	// StoreAPIAddr is a host:port address of gRPC StoreAPI of the peer holding the state. Required for PeerTypeSource and PeerTypeStore.
	StoreAPIAddr string
	// QueryAPIAddr is a host:port address of HTTP QueryAPI of the peer holding the state. Required for PeerTypeQuery type only.
	QueryAPIAddr string

	// Metadata holds metadata of the peer holding the state.
	Metadata PeerMetadata
}

// PeerMetadata are the information that can change in runtime of the peer.
type PeerMetadata struct {
	// Labels represents external labels for the peer. Only relevant for PeerTypeSource. Empty for other types.
	Labels []storepb.Label

	// MinTime indicates the minTime of the oldest block available from this peer.
	MinTime int64
	// MaxTime indicates the maxTime of the youngest block available from this peer.
	MaxTime int64
}

// New returns "alone" peer that is ready to join.
func New(
	l log.Logger,
	reg *prometheus.Registry,
	bindAddr string,
	advertiseAddr string,
	advertiseStoreAPIAddr string,
	advertiseQueryAPIAddress string,
	discovery PeerDiscovery,
	waitIfEmpty bool,
	pushPullInterval time.Duration,
	gossipInterval time.Duration,
	refreshInterval time.Duration,
	secretKey []byte,
	networkType string,
) (*Peer, error) {
	l = log.With(l, "component", "cluster")

	bindHost, bindPortStr, err := net.SplitHostPort(bindAddr)
	if err != nil {
		return nil, err
	}
	bindPort, err := strconv.Atoi(bindPortStr)
	if err != nil {
		return nil, errors.Wrap(err, "invalid listen address")
	}

	// Best-effort deduction of advertise address.
	advertiseHost, advertisePort, err := CalculateAdvertiseAddress(bindAddr, advertiseAddr)
	if err != nil {
		level.Warn(l).Log("err", "couldn't deduce an advertise address: "+err.Error())
	}

	if IsUnroutable(advertiseHost) {
		level.Warn(l).Log("err", "this node advertises itself on an unroutable address", "host", advertiseHost, "port", advertisePort)
		level.Warn(l).Log("err", "this node will be unreachable in the cluster")
		level.Warn(l).Log("err", "provide --cluster.advertise-address as a routable IP address or hostname")
	}

	resolvedPeers, err := discovery.ResolvePeers(advertiseAddr, waitIfEmpty)
	if err != nil {
		return nil, errors.Wrap(err, "resolve peers")
	}
	level.Debug(l).Log("msg", "resolved peers to following addresses", "peers", strings.Join(resolvedPeers, ","))

	// TODO(fabxc): generate human-readable but random names?
	name, err := ulid.New(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano())))
	if err != nil {
		return nil, err
	}

	cfg, err := parseNetworkConfig(networkType)
	if err != nil {
		return nil, err
	}
	cfg.Name = name.String()
	cfg.BindAddr = bindHost
	cfg.BindPort = bindPort
	if gossipInterval != 0 {
		cfg.GossipInterval = gossipInterval
	}
	if pushPullInterval != 0 {
		cfg.PushPullInterval = pushPullInterval
	}
	cfg.Logger = stdlog.New(log.NewStdlibAdapter(level.Debug(l)), "peers", stdlog.LstdFlags)
	cfg.SecretKey = secretKey
	if advertiseAddr != "" {
		cfg.AdvertiseAddr = advertiseHost
		cfg.AdvertisePort = advertisePort
	}

	gossipMsgsReceived := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_gossip_messages_received_total",
		Help: "Total gossip NotifyMsg calls.",
	})
	gossipClusterMembers := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_cluster_members",
		Help: "Number indicating current number of members in cluster.",
	})

	reg.MustRegister(gossipMsgsReceived)
	reg.MustRegister(gossipClusterMembers)

	return &Peer{
		logger:                   l,
		initialPeers:             resolvedPeers,
		cfg:                      cfg,
		refreshInterval:          refreshInterval,
		gossipMsgsReceived:       gossipMsgsReceived,
		gossipClusterMembers:     gossipClusterMembers,
		stopc:                    make(chan struct{}),
		data:                     &data{data: map[string]PeerState{}},
		advertiseAddr:            advertiseAddr,
		advertiseStoreAPIAddr:    advertiseStoreAPIAddr,
		advertiseQueryAPIAddress: advertiseQueryAPIAddress,
		discovery:                discovery,
	}, nil
}

// Join joins to the memberlist gossip cluster using initialPeers and given peerType and initialMetadata.
func (p *Peer) Join(peerType PeerType, initialMetadata PeerMetadata) error {
	if p.hasJoined() {
		return errors.New("peer already joined; close it first to rejoin")
	}

	var ml *memberlist.Memberlist
	d := newDelegate(p.logger, ml.NumMembers, p.data, p.gossipMsgsReceived, p.gossipClusterMembers)
	p.cfg.Delegate = d
	p.cfg.Events = d

	ml, err := memberlist.Create(p.cfg)
	if err != nil {
		return errors.Wrap(err, "create memberlist")
	}

	n, err := ml.Join(p.initialPeers)
	if err != nil {
		level.Error(p.logger).Log("msg", "none of the peers was can be reached", "peerType", peerType, "initialPeers", strings.Join(p.initialPeers, ","), "err", err)
	} else {
		level.Debug(p.logger).Log("msg", "joined cluster", "peerType", peerType, "initialPeers", strings.Join(p.initialPeers, ","))
	}

	if n > 0 {
		go warnIfAlone(p.logger, 10*time.Second, p.stopc, ml.NumMembers)
	}

	p.mlistMtx.Lock()
	p.mlist = ml
	p.mlistMtx.Unlock()

	// Initialize state with ourselves.
	p.data.Set(p.Name(), PeerState{
		Type:         peerType,
		StoreAPIAddr: p.advertiseStoreAPIAddr,
		QueryAPIAddr: p.advertiseQueryAPIAddress,
		Metadata:     initialMetadata,
	})

	if p.refreshInterval != 0 {
		go p.periodicallyRefresh()
	}

	return nil
}

func (p *Peer) periodicallyRefresh() {
	tick := time.NewTicker(p.refreshInterval)
	defer tick.Stop()

	for {
		select {
		case <-p.stopc:
			return
		case <-tick.C:
			if err := p.Refresh(); err != nil {
				level.Error(p.logger).Log("msg", "Refreshing memberlist", "err", err)
			}
		}
	}
}

// Refresh renews membership cluster, this will refresh DNS names and join newly added members
func (p *Peer) Refresh() error {
	p.mlistMtx.Lock()
	defer p.mlistMtx.Unlock()

	if p.mlist == nil {
		return nil
	}

	resolvedPeers, err := p.discovery.ResolvePeers(p.advertiseAddr, false)
	if err != nil {
		return errors.Wrapf(err, "refresh cluster could not resolve peers: %v", resolvedPeers)
	}

	currMembers := p.mlist.Members()
	var notConnected []string
	for _, peer := range resolvedPeers {
		var isPeerFound bool

		for _, mem := range currMembers {
			if mem.Address() == peer {
				isPeerFound = true
				break
			}
		}

		if !isPeerFound {
			notConnected = append(notConnected, peer)
		}
	}

	if len(notConnected) == 0 {
		level.Debug(p.logger).Log("msg", "refresh cluster done", "resolvedPeers", strings.Join(resolvedPeers, ","))
		return nil
	}

	curr, err := p.mlist.Join(notConnected)
	if err != nil {
		return errors.Wrapf(err, "join peers %s ", strings.Join(notConnected, ","))
	}

	level.Debug(p.logger).Log("msg", "refresh cluster done, peers joined", "peers", strings.Join(notConnected, ","), "before", len(currMembers), "after", curr)
	return nil
}

func (p *Peer) hasJoined() bool {
	p.mlistMtx.RLock()
	defer p.mlistMtx.RUnlock()

	return p.mlist != nil
}

func warnIfAlone(logger log.Logger, d time.Duration, stopc chan struct{}, numNodes func() int) {
	tick := time.NewTicker(d)
	defer tick.Stop()

	for {
		select {
		case <-stopc:
			return
		case <-tick.C:
			if n := numNodes(); n <= 1 {
				level.Warn(logger).Log("NumMembers", n, "msg", "I appear to be alone in the cluster")
			}
		}
	}
}

// SetLabels updates internal metadata's labels stored in PeerState for this peer.
// Note that this data will be propagated based on gossipInterval we set.
func (p *Peer) SetLabels(labels []storepb.Label) {
	if !p.hasJoined() {
		return
	}

	s, _ := p.data.Get(p.Name())
	s.Metadata.Labels = labels
	p.data.Set(p.Name(), s)
}

// SetTimestamps updates internal metadata's timestamps stored in PeerState for this peer.
// Note that this data will be propagated based on gossipInterval we set.
func (p *Peer) SetTimestamps(mint int64, maxt int64) {
	if !p.hasJoined() {
		return
	}

	s, _ := p.data.Get(p.Name())
	s.Metadata.MinTime = mint
	s.Metadata.MaxTime = maxt
	p.data.Set(p.Name(), s)
}

// Close leaves the cluster waiting up to timeout and shutdowns peer if cluster left.
// TODO(bplotka): Add this method into run.Group closing logic for each command. This will improve graceful shutdown.
func (p *Peer) Close(timeout time.Duration) {
	if !p.hasJoined() {
		return
	}

	if err := p.mlist.Leave(timeout); err != nil {
		level.Error(p.logger).Log("msg", "memberlist leave failed", "err", err)
	}
	close(p.stopc)
	if err := p.mlist.Shutdown(); err != nil {
		level.Error(p.logger).Log("msg", "memberlist shutdown failed", "err", err)
	}
	p.mlist = nil
}

// Name returns the unique ID of this peer in the cluster.
func (p *Peer) Name() string {
	if !p.hasJoined() {
		return ""
	}

	return p.mlist.LocalNode().Name
}

// PeerTypesStoreAPIs gives a PeerType that allows all types that exposes StoreAPI.
func PeerTypesStoreAPIs() []PeerType {
	return []PeerType{PeerTypeStore, PeerTypeSource}
}

// PeerStates returns the custom state information for each peer by memberlist peer id (name).
func (p *Peer) PeerStates(types ...PeerType) map[string]PeerState {
	if !p.hasJoined() {
		return nil
	}

	ps := map[string]PeerState{}
	for _, o := range p.mlist.Members() {
		os, ok := p.data.Get(o.Name)
		if !ok {
			continue
		}

		if len(types) == 0 {
			ps[o.Name] = os
			continue
		}
		for _, t := range types {
			if os.Type == t {
				ps[o.Name] = os
				break
			}
		}
	}
	return ps
}

// PeerState returns the custom state information by memberlist peer name.
func (p *Peer) PeerState(id string) (PeerState, bool) {
	if !p.hasJoined() {
		return PeerState{}, false
	}

	ps, ok := p.data.Get(id)
	if !ok {
		return PeerState{}, false
	}
	return ps, true
}

// Info returns a JSON-serializable dump of cluster state.
// Useful for debug.
func (p *Peer) Info() map[string]interface{} {
	if !p.hasJoined() {
		return nil
	}

	d := map[string]PeerState{}
	for k, v := range p.data.Data() {
		d[k] = v
	}

	return map[string]interface{}{
		"self":    p.mlist.LocalNode(),
		"members": p.mlist.Members(),
		"n":       p.mlist.NumMembers(),
		"state":   d,
	}
}

func IsUnroutable(host string) bool {
	if ip := net.ParseIP(host); ip != nil && (ip.IsUnspecified() || ip.IsLoopback()) {
		return true // typically 0.0.0.0 or localhost
	} else if ip == nil && strings.ToLower(host) == "localhost" {
		return true
	}
	return false
}

func parseNetworkConfig(networkType string) (*memberlist.Config, error) {
	var mc *memberlist.Config

	switch networkType {
	case LanNetworkPeerType:
		mc = memberlist.DefaultLANConfig()
	case WanNetworkPeerType:
		mc = memberlist.DefaultWANConfig()
	case LocalNetworkPeerType:
		mc = memberlist.DefaultLocalConfig()
	default:
		return nil, errors.Errorf("unexpected network type %s, should be one of: %s",
			networkType,
			strings.Join(NetworkPeerTypes, ", "),
		)
	}

	return mc, nil
}
