package bloom

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var pruneTestCases = []struct {
	column  string
	check   string
	query   []string
	parts   [][]string
	expect  []string
	enabled bool
}{
	{ // search for 1 when filter contains
		column: "inet_src_addr",
		check:  "IN",
		query:  iplist("10.0.0.1", 1),
		parts: [][]string{
			iplist("10.0.0.1", 1),
		},
		expect:  iplist("10.0.0.1", 1),
		enabled: true,
	},
	{ // search for all but 1 when filter contains
		column: "inet_dst_addr",
		check:  "NOT IN",
		query:  iplist("10.0.0.1", 1),
		parts: [][]string{
			iplist("10.0.0.1", 1),
		},
		expect:  iplist("10.0.0.1", 1),
		enabled: true,
	},
	{ // search for any of 3 when filter contains all
		column: "inet_src_addr",
		check:  "IN",
		query:  iplist("10.0.0.1", 3),
		parts: [][]string{
			iplist("10.0.0.1", 3),
		},
		expect:  iplist("10.0.0.1", 3),
		enabled: true,
	},
	{ // search for any of 4 when 2 exist in at least one partition
		column: "inet_dst_addr",
		check:  "IN",
		query:  iplist("10.0.0.1", 4),
		parts: [][]string{
			iplist("10.0.0.1", 1),
			iplist("10.0.0.3", 1),
		},
		expect: []string{
			"10.0.0.1",
			"10.0.0.3",
		},
		enabled: true,
	},
	{ // search for any of 32 when filter contains none
		column: "inet_src_addr",
		check:  "IN",
		query:  iplist("10.0.0.1", 32),
		parts: [][]string{
			iplist("11.0.0.1", 32),
		},
		expect:  []string{},
		enabled: true,
	},
	{ // search for not any of 32 when filter contains none
		column: "inet_src_addr",
		check:  "NOT IN",
		query:  iplist("10.0.0.1", 32),
		parts: [][]string{
			iplist("11.0.0.1", 32),
		},
		expect:  []string{},
		enabled: true,
	},
	{ // search for any of 32 when filter contains none, but disabled
		column:  "inet_dst_addr",
		check:   "IN",
		query:   iplist("10.0.0.1", 32),
		parts:   [][]string{},
		expect:  iplist("10.0.0.1", 32),
		enabled: false,
	},
	{ // search for 1 when only a single partition contains
		column: "inet_dst_addr",
		check:  "IN",
		query:  iplist("10.0.0.1", 1),
		parts: [][]string{
			iplist("12.0.0.1", 32),
			iplist("11.0.0.1", 32),
			iplist("10.0.0.1", 32),
		},
		expect:  []string{"10.0.0.1"},
		enabled: true,
	},
}

var dataDirs = []string{
	"/data/fb/",
	"/mem/fb/",
}

func TestRewriteQuery(t *testing.T) {
	assert := assert.New(t)

	for _, test := range pruneTestCases {
		fs := &mapFS{m: map[string][]byte{}}
		createFile, openFile, existsFile = fs.Create, fs.Open, fs.Exists

		var parts []string
		for i, filter := range test.parts {
			part := fmt.Sprintf("1013/router2/1116/2018/10/08/19/%02d", i)
			file := fmt.Sprintf("/data/fb/%s/%s.bf", part, strings.ToUpper(test.column))
			fs.m[file] = bfWithEntries(filter...)
			parts = append(parts, part)
		}

		list := strings.Join(test.query, ",")
		original := fmt.Sprintf("%s %s ('%s')", test.column, test.check, list)

		var expect = "1 = 1 AND "
		if len(test.expect) == 0 {
			switch test.check {
			case "IN":
				expect += "(1 = 0)"
			case "NOT IN":
				expect += "(1 = 1)"
			}
		} else {
			var join, op string
			switch test.check {
			case "IN":
				join, op = " OR ", "="
			case "NOT IN":
				join, op = " AND ", "!="
			}

			clauses := make([]string, len(test.expect))
			for i, addr := range test.expect {
				clauses[i] = fmt.Sprintf("%s %s '%s'", test.column, op, addr)
			}

			expect += fmt.Sprintf("(%s)", strings.Join(clauses, join))
		}

		pruned := MaybeRewriteWhereClause(original, dataDirs, parts, test.enabled)

		assert.Equal(expect, pruned)
	}
}

func iplist(start string, count uint32) []string {
	first := binary.BigEndian.Uint32(net.ParseIP(start).To4())
	last := first + count
	list := make([]string, 0, count)

	for next := first; next < last; next++ {
		var addr [4]byte
		binary.BigEndian.PutUint32(addr[:], next)
		list = append(list, net.IP(addr[:]).String())
	}

	return list
}
