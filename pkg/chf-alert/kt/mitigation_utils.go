package kt

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
)

func MitigateIPDimIndex(dims []string) (int, error) {
	ipDimIndex := -1

	for i, dim := range dims {
		switch dim {
		case DIMENSION_IP_DST:
			ipDimIndex = i
		case DIMENSION_IP_SRC:
			if ipDimIndex == -1 { // Prefer IP_DST(_CIDR)
				ipDimIndex = i
			}
		default:
			if strings.HasPrefix(dim, DIMENSION_IP_DST_CIDR) {
				ipDimIndex = i
			} else if strings.HasPrefix(dim, DIMENSION_IP_SRC_CIDR) {
				if ipDimIndex == -1 { // Prefer IP_DST(_CIDR)
					ipDimIndex = i
				}
			}
		}
	}

	if ipDimIndex == -1 {
		return -1, fmt.Errorf("No IP found: %v", dims)
	} else {
		return ipDimIndex, nil
	}
}

func MitigateIPFromDims(dims map[string]string, def string) (ip string) {
	if dims == nil {
		return def
	}

	for k, v := range dims {
		switch k {
		case DIMENSION_IP_DST:
			ip = v
		case DIMENSION_IP_SRC:
			if len(ip) == 0 { // Prefer IP_DST(_CIDR)
				ip = v
			}
		default:
			if strings.HasPrefix(k, DIMENSION_IP_DST_CIDR) {
				ip = v
			} else if strings.HasPrefix(k, DIMENSION_IP_SRC_CIDR) {
				if len(ip) == 0 { // Prefer IP_DST(_CIDR)
					ip = v
				}
			}
		}
	}

	if len(ip) == 0 {
		ip = def
	}

	return
}

func MitigateProtocolDimIndex(dims []string) int {
	protocolDimIndex := -1

	for i, dim := range dims {
		if dim == DIMENSION_PROTO {
			protocolDimIndex = i
		}
	}

	return protocolDimIndex
}

func MitigateProtocolDimIndexFromAlarmEvent(alarmEvent *AlarmEvent) int {
	dimensions := strings.Split(alarmEvent.AlertDimension, KeyJoinToken)
	return MitigateProtocolDimIndex(dimensions)
}

// LookupIPAddrsForURL returns the list of IPAddrs associated with the url.
func LookupIPAddrsForURL(ctx context.Context, urlStr string) ([]net.IPAddr, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	host, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		// SplitHostPort returns an error if there was no port, or other conditions.
		// This is OK. Just pass what we've got to LookupIPAddr and see how it goes.
		host = u.Host
	}
	ipaddrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	return ipaddrs, nil
}

// IPAuthorizer allows or disallows communications to a given IP.
type IPAuthorizer interface {
	IsIPAuthorized(ip net.IP) bool
	AreIPAddrsAuthorized(ipaddrs []net.IPAddr) bool
}

// DefaultIPAuthorizer is an IPAuthorizer that allows comms to IPs
// that are not "internal", and pass a configurable blacklist.
// Use this to prevent customer config from making connections to IPs
// that we should not allow for security or application reasons.
// TODO(jackb): make configurable via properties.
var DefaultIPAuthorizer = &ipAuthorizer{
	blacklist: append(
		[]*net.IPNet{
			&oneOneOneZero24,
			&twoTwoTwoTwo32,
		}, internalIPNets...),
}

// TestIPAuthorizer allows communication to any IP during tests.
// Don't use outside of tests.
var TestIPAuthorizer = &ipAuthorizer{blacklist: nil}

// internalIPNets is a list of IPNets that are considered "internal" addresses,
// as in chf/client/netclass/rule (https://github.com/Kentik/CloudhelixFlow/blob/851387c8f958967a2f13bbcf0785dbc48c31cf71/pkg/client/netclass/rule/rule.go#L26).
// Often we don't want to allow customers to interact with internal addresses in our
// systems.
var internalIPNets = func() []*net.IPNet {
	strs := []string{
		"0.0.0.0/8",
		"127.0.0.0/8",
		"100.64.0.0/10",
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"169.254.0.0/16",
		"192.0.0.0/24",
		"192.0.2.0/24",
		"192.18.0.0/15",
		"198.51.100.0/24",
		"203.0.113.0/24",
		"224.0.0.0/4",
		"192.88.99.0/24",
		"240.0.0.0/4",
		"fc00::/7",  // (ula)
		"fe80::/10", // (link local)
	}
	ipnets := make([]*net.IPNet, 0, len(strs))
	for _, str := range strs {
		_, ipnet, err := net.ParseCIDR(str)
		if err != nil {
			panic(err) // We are running at package init time, so assert.
		}
		ipnets = append(ipnets, ipnet)
	}
	return ipnets
}()

// Some IPs that we assume to be bogus or used for testing.
var (
	oneOneOneZero   = net.IP{1, 1, 1, 0}
	oneOneOneZero24 = net.IPNet{IP: oneOneOneZero, Mask: net.CIDRMask(24, 32)}
	twoTwoTwoTwo    = net.IP{2, 2, 2, 2}
	twoTwoTwoTwo32  = net.IPNet{IP: twoTwoTwoTwo, Mask: net.CIDRMask(32, 32)}
)

type ipAuthorizer struct {
	blacklist []*net.IPNet
}

func (ia *ipAuthorizer) IsIPAuthorized(ip net.IP) bool {
	return !isIPInIPNets(ip, ia.blacklist)
}

func (ia *ipAuthorizer) AreIPAddrsAuthorized(ipaddrs []net.IPAddr) bool {
	for _, ipaddr := range ipaddrs {
		if !ia.IsIPAuthorized(ipaddr.IP) {
			return false
		}
	}
	return true
}

// Note that for this use case we just loop over the ips. This is not
// fast, like the patricia tree based implementation elsewhere in our stack.
func isIPInIPNets(ip net.IP, ipnets []*net.IPNet) bool {
	for _, ipnet := range ipnets {
		if ipnet.Contains(ip) {
			return true
		}
	}
	return false
}

type NotTalkingToBlacklistedURLError struct{ URL string }

func (err NotTalkingToBlacklistedURLError) Error() string {
	return fmt.Sprintf("not talking to URL (blacklisted): %s", err.URL)
}
