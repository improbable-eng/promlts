package dns

import (
	"context"
	"net"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type QType string

const (
	A   = QType("dns")
	SRV = QType("dnssrv")
)

type Resolver interface {

	// Resolve performs a DNS lookup and returns a list of records.
	// name is the domain name to be resolved.
	// qtype is the query type. Accepted values are `dns` for A/AAAA lookup and `dnssrv` for SRV lookup.
	// If qtype is `dns`, the domain name to be resolved requires a port or an error will be returned.
	// If scheme is passed through name, it is preserved on IP results.
	Resolve(ctx context.Context, name string, qtype QType) ([]string, error)
}

type ipLookupResolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
	LookupSRV(ctx context.Context, service, proto, name string) (cname string, addrs []*net.SRV, err error)
}

type dnsSD struct {
	resolver ipLookupResolver
}

// NewResolver provides a resolver with a specific net.Resolver. If resolver is nil, the default resolver will be used.
func NewResolver() Resolver {
	return &dnsSD{resolver: net.DefaultResolver}
}

func (s *dnsSD) Resolve(ctx context.Context, name string, qtype QType) ([]string, error) {
	var (
		res    []string
		scheme string
	)

	schemeSplit := strings.Split(name, "//")
	if len(schemeSplit) > 1 {
		scheme = schemeSplit[0]
		name = schemeSplit[1]
	}

	// Split the host and port if present.
	host, port, err := net.SplitHostPort(name)
	if err != nil {
		// The host could be missing a port.
		host, port = name, ""
	}

	switch qtype {
	case A:
		if port == "" {
			return nil, errors.Errorf("missing port in address given for dns lookup: %v", name)
		}
		ips, err := s.resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, errors.Wrapf(err, "lookup IP addresses %q", host)
		}
		for _, ip := range ips {
			res = append(res, appendScheme(scheme, net.JoinHostPort(ip.String(), port)))
		}
	case SRV:
		_, recs, err := s.resolver.LookupSRV(ctx, "", "", host)
		if err != nil {
			return nil, errors.Wrapf(err, "lookup SRV records %q", host)
		}
		for _, rec := range recs {
			// Only use port from SRV record if no explicit port was specified.
			resPort := port
			if resPort == "" {
				resPort = strconv.Itoa(int(rec.Port))
			}
			// Do A lookup for the domain in SRV answer
			resIPs, err := s.resolver.LookupIPAddr(ctx, rec.Target)
			if err != nil {
				return nil, errors.Wrapf(err, "look IP addresses %q", rec.Target)
			}
			for _, resIP := range resIPs {
				res = append(res, appendScheme(scheme, net.JoinHostPort(resIP.String(), resPort)))
			}
		}
	default:
		return nil, errors.Errorf("invalid lookup scheme %q", qtype)
	}

	return res, nil
}

func appendScheme(scheme, host string) string {
	if scheme == "" {
		return host
	}
	return scheme + "//" + host
}
