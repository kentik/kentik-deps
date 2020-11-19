package kt

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

func CheckForDummyIP(s IPCidr) error {
	if strings.HasPrefix(s, MANUAL_MITIGATION_DUMMY_IP) {
		return fmt.Errorf("IPCidr '%s' looks like a dummy/placeholder", s)
	}
	return nil
}

// LooksLikeIPv4 detects whether the IP/CIDR in s looks like
// it is IPv4. This is the same detection check used in net.ParseIP.
func LooksLikeIPv4(s IPCidr) bool {
	// like in net.ParseIP
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '.':
			return true
		case ':':
			return false
		}
	}
	return true
}

// LooksLikeIPv6 detects whether the IP/CIDR in s looks like
// it is IPv6. This is the same detection check used in net.ParseIP.
func LooksLikeIPv6(s IPCidr) bool {
	// like in net.ParseIP
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '.':
			return false
		case ':':
			return true
		}
	}
	return false
}

// EnsureIPNetPrefix32128 is EnsureIPNetPrefix(ipOrCidr, ipv6 ? 128 : 32).
func EnsureIPNetPrefix32128(ipOrCidr IPCidr) (IPCidr, error) {
	prefix := 32
	if LooksLikeIPv6(ipOrCidr) {
		prefix = 128
	}
	return EnsureIPNetPrefix(ipOrCidr, prefix)
}

// EnsureIPNetPrefix returns a string representing a normalized CIDR
// passed through net.ParseCIDR. If there was no prefix (i.e. input
// was just an ip), use the prefix given. Works for both IPv4 and IPv6.
func EnsureIPNetPrefix(ipOrCidr IPCidr, prefix int) (IPCidr, error) {
	if !strings.Contains(string(ipOrCidr), "/") {
		ipOrCidr = fmt.Sprintf("%s/%d", ipOrCidr, prefix)
	}
	_, ipNet, err := net.ParseCIDR(ipOrCidr)
	if err != nil {
		return "", err
	}
	return IPCidr(ipNet.String()), nil
}

// ForceIPNetPrefix2448 is ForceIPNetPrefix(ipOrCidr, ipv6 ? 48 : 24).
func ForceIPNetPrefix2448(ipOrCidr IPCidr) (IPCidr, error) {
	prefix := 24
	if LooksLikeIPv6(ipOrCidr) {
		prefix = 48
	}
	return ForceIPNetPrefix(ipOrCidr, prefix)
}

// ForceIPNetPrefix returns a string representing a normalized CIDR
// passed through net.ParseCIDR where the prefix is guaranteed to be
// equal to `prefix`. Works for both IPv4 and IPv6.
func ForceIPNetPrefix(ipOrCidr IPCidr, prefix int) (IPCidr, error) {
	slashIndex := strings.Index(ipOrCidr, "/")
	if slashIndex >= 0 {
		ipOrCidr = ipOrCidr[0:slashIndex] // Remove whatever old prefix we had.
	}
	ipOrCidr = fmt.Sprintf("%s/%d", ipOrCidr, prefix)
	_, ipNet, err := net.ParseCIDR(ipOrCidr)
	if err != nil {
		return "", err
	}
	return IPCidr(ipNet.String()), nil
}

func EnsureAtLeastPrefix(ipOrCidr IPCidr, prefix int) (IPCidr, error) {
	slashIndex := strings.Index(ipOrCidr, "/")
	if slashIndex >= 0 { // a prefix is specified in ipOrCidr
		if slashIndex == 0 || (slashIndex+1 >= len(ipOrCidr)) {
			return "", fmt.Errorf("invalid ipcidr %s", ipOrCidr)
		}
		specifiedPrefix, err := strconv.Atoi(ipOrCidr[slashIndex+1:])
		if err != nil {
			return "", err
		}
		if specifiedPrefix <= 0 {
			return "", fmt.Errorf("invalid ipcidr %s: prefix must be greater than zero", ipOrCidr)
		}
		if specifiedPrefix < prefix {
			prefix = specifiedPrefix
		}
		ipOrCidr = ipOrCidr[0:slashIndex]
	}
	_, ipNet, err := net.ParseCIDR(fmt.Sprintf("%s/%d", ipOrCidr, prefix))
	if err != nil {
		return "", err
	}
	return IPCidr(ipNet.String()), nil
}
