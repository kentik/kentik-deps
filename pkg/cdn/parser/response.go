package parser

import (
	"net"
	"strings"
)

func (r *Result) storeIP(text string) {
	ip := net.ParseIP(text)
	if ip == nil {
		panic(text + " is supposed to be an IP but it isn't")
	}
	r.IP = append(r.IP, ip)
}

func (r *Result) storeCname(text string) {
	r.Cname = append(r.Cname, strings.ToLower(text))
}
