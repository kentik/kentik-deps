package bloom

import (
	"bytes"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/willf/bloom"
)

func TestGetBloomFilterQuerySet(t *testing.T) {
	for _, tc := range getBloomFilterQuerySetTCs {
		bfqs := getBloomFilterQuerySet(tc.whereClause)
		assert.Equal(t, tc.expected, bfqs != nil, tc.comment)
	}
}

func BenchmarkGetBloomFilterQuerySet(b *testing.B) {
	lastTC := getBloomFilterQuerySetTCs[len(getBloomFilterQuerySetTCs)-1]
	for i := 0; i < b.N; i++ {
		_ = getBloomFilterQuerySet(lastTC.whereClause)
	}
}

var getBloomFilterQuerySetTCs = []struct {
	whereClause string
	expected    bool
	comment     string
}{
	{`inet_src_addr = '127.0.0.1'`, true, "inet_src_addr can be queried (plain)"},
	{`((inet_src_addr = '127.0.0.1'))`, true, "inet_src_addr can be queried (parens)"},
	{`((1 = 1)) AND ((inet_src_addr = '127.0.0.1')) AND ((1 = 1)) AND ((1 = 1))`, true, "inet_src_addr can be queried (as in integration)"},
	{`((1 = 1)) AND ((inet_dst_addr = '127.0.0.1')) AND ((1 = 1)) AND ((1 = 1))`, true, "inet_dst_addr can be queried"},
	{`((1 = 1)) AND ((inet_dst_addr = '127.0.0.1') OR (inet_src_addr = '127.0.0.1')) AND ((1 = 1)) AND ((1 = 1))`, true, "src OR dst"},
	{`((1 = 1)) AND ((inet_dst_addr = '127.0.0.1')) AND ((inet_src_addr = '127.0.0.1')) AND ((1 = 1)) AND ((1 = 1))`, true, "src AND dst"},
	{`inet_src_addr = 'ffff:ffff::'`, true, "v6 addrs can be queried (plain)"},
	{`inet_src_addr = 'ffff:ffff::/128'`, true, "v6 addrs can be queried (/128)"},

	{`((1 = 1)) AND ((inet_dst_addr = '127.0.0.1') AND (inet_src_addr = '127.0.0.1')) AND ((1 = 1)) AND ((1 = 1))`, false, "ANDs are nested inside some parens, we don't evaluate arbitrarily complex queries"},
	{`((1 = 1)) AND ((inet_dst_addr = '127.0.0.1') OR (inet_src_addr = '127.0.0.1') OR (l4_dst_port = 22))`, false, "a column that isn't queryable is involved (l4_dst_port)"},
	{`inet_src_addr = '127.0.0.0/28'`, false, "v4 addrs must be /32 or plain"},
	{`inet_src_addr = 'ffff:ffff::/32'`, false, "v6 addrs must be /128 or plain"},
	{`
((1 = 1)) AND ((1 = 1)) AND (((l4_src_port = 49) OR (l4_dst_port = 49))) AND (((inet_src_addr = '69.28.171.15/32') OR (inet_src_addr = '69.28.172.241/32') OR (inet_src_addr = '68.142.119.222/32') OR (inet_src_addr = '68.142.80.48/32') OR (inet_src_addr = '111.119.16.252/32') OR (inet_src_addr = '68.142.80.118/32') OR (inet_src_addr = '69.28.171.6/32') OR (inet_src_addr = '111.119.12.0/32') OR (inet_src_addr = '45.113.116.0/32') OR (inet_src_addr = '178.79.216.241/32') OR (inet_src_addr = '68.142.80.102/32') OR (inet_src_addr = '68.142.80.52/32') OR (inet_src_addr = '203.77.188.4/32') OR (inet_src_addr = '203.77.188.5/32') OR (inet_src_addr = '69.28.171.11/32') OR (inet_src_addr = '68.142.80.17/32') OR (inet_src_addr = '68.142.80.64/32') OR (inet_src_addr = '68.142.80.68/32') OR (inet_src_addr = '68.142.80.35/32') OR (inet_src_addr = '69.28.171.3/32') OR (inet_src_addr = '69.28.172.230/32') OR (inet_src_addr = '69.28.172.238/32') OR (inet_src_addr = '103.53.12.1/32') OR (inet_src_addr = '68.142.80.57/32') OR (inet_src_addr = '68.142.80.34/32') OR (inet_src_addr = '68.142.100.222/32') OR (inet_src_addr = '68.142.80.98/32') OR (inet_src_addr = '68.142.80.112/32') OR (inet_src_addr = '68.142.80.106/32') OR (inet_src_addr = '69.28.171.2/32') OR (inet_src_addr = '69.28.171.16/32') OR (inet_src_addr = '68.142.80.12/32') OR (inet_src_addr = '68.142.64.32/32') OR (inet_src_addr = '68.142.125.233/32') OR (inet_src_addr = '68.142.80.32/32') OR (inet_src_addr = '69.28.171.12/32') OR (inet_src_addr = '117.121.248.61/32') OR (inet_src_addr = '69.28.171.1/32') OR (inet_src_addr = '69.28.172.245/32') OR (inet_src_addr = '117.121.252.1/32') OR (inet_src_addr = '178.79.198.113/32') OR (inet_src_addr = '68.142.81.10/32') OR (inet_src_addr = '203.77.184.225/32') OR (inet_src_addr = '68.142.80.14/32') OR (inet_src_addr = '68.142.80.26/32') OR (inet_src_addr = '69.28.171.34/32')))
`, true, "example query from prod"},
}

func TestBloomEval(t *testing.T) {
	fs := &mapFS{m: bloomEvalTestFS}
	createFile, openFile, existsFile = fs.Create, fs.Open, fs.Exists
	dataDirs := []string{"/data/fb/", "/mem/fb/"}
	for _, tc := range bloomEvalTCs {
		actual := parseKQLAndEvalBloomFilters(tc.whereClause, dataDirs, tc.partitions)
		assert.Equal(t, tc.expected, actual, tc.comment)
	}
}

func TestGetShard(t *testing.T) {
	files := []string{
		"pesa0309_01_bbn_mgmt_sin2_secureserver_net#2",
		"qfx_5012",
	}
	exps := []uint32{2, 0}

	for i, _ := range files {
		assert.Equal(t, exps[i], getShardNum(files[i]), files[i])
	}
}

func TestFb2fbz(t *testing.T) {

	files := []string{
		"23549/pesa0309_01_bbn_mgmt_sin2_secureserver_net#2/17403/2018/09/27/17/04",
		"23549/pesa0309_01_bbn_mgmt_sin2_secureserver_net/17403/2018/09/27/17/04",
		"2009/qfx_5012/8373/HH/2018/09/27/07",
		"2009/qfx_5012#23/8373/HH/2018/09/27/07",
	}
	exps := []string{
		"23549/17403/MM/2018/09/27/23549-17403-2018-09-27-17:04-2.cdb",
		"23549/17403/MM/2018/09/27/23549-17403-2018-09-27-17:04-0.cdb",
		"2009/8373/HH/2018/09/27/2009-8373-2018-09-27-07:HH-0.cdb",
		"2009/8373/HH/2018/09/27/2009-8373-2018-09-27-07:HH-23.cdb",
	}
	expsOld := []string{
		"23549/17403/MM/2018/09/27/23549-17403-2018-09-27-17:04.cdb",
		"23549/17403/MM/2018/09/27/23549-17403-2018-09-27-17:04.cdb",
		"2009/8373/HH/2018/09/27/2009-8373-2018-09-27-07:HH.cdb",
		"2009/8373/HH/2018/09/27/2009-8373-2018-09-27-07:HH.cdb",
	}
	for i, _ := range files {
		oldF, newF := fb2fbz(files[i])
		assert.Equal(t, expsOld[i], oldF, files[i])
		assert.Equal(t, exps[i], newF, files[i])
	}
}

func BenchmarkBloomEval(b *testing.B) {
	tc := bloomEvalTCs[1]
	fs := &mapFS{m: bloomEvalTestFS}
	createFile, openFile, existsFile = fs.Create, fs.Open, fs.Exists
	dataDirs := []string{"/data/fb/", "/mem/fb/"}
	for i := 0; i < b.N; i++ {
		_ = parseKQLAndEvalBloomFilters(tc.whereClause, dataDirs, tc.partitions)
	}
}

var bloomEvalTCs = []struct {
	whereClause string
	partitions  []string
	expected    bool
	comment     string
}{
	{whereClause, []string{part2}, false, "all partitions have BFs which say ip isn't present"},
	{whereClause, []string{part2, part3}, false, "all partitions have BFs which say ip isn't present (2)"},
	{whereClause, []string{}, false, "no partitions, so no data"},

	{whereClause, []string{part1}, true, "all partitions have BFs which say ip may be present"},
	{whereClause, []string{part1, part2}, true, "1/2 partitions have BFs which say ip may be present"},
	{whereClause, []string{part1, part4}, true, "one partition doesn't have any BFs, so we can't prune"},
	{whereClause, []string{part4}, true, "all partitions have no BFs"},
}

var (
	whereClause = `inet_src_addr = '127.0.0.2'`
	part1       = "1013/router2/1116/2018/10/08/19/44"
	part2       = "1013/router2/1116/2018/10/08/19/45"
	part3       = "1013/router2/1116/2018/10/08/19/46"
	part4       = "1013/router2/1116/2018/10/08/19/47"
)

var bloomEvalTestFS = map[string][]byte{
	"/data/fb/1013/router2/1116/2018/10/08/19/44/INET_SRC_ADDR.bf": bfWithEntries("127.0.0.2"),
	"/data/fb/1013/router2/1116/2018/10/08/19/44/INET_DST_ADDR.bf": bfWithEntries("127.0.0.2"),

	"/data/fb/1013/router2/1116/2018/10/08/19/45/INET_SRC_ADDR.bf": bfWithEntries("127.0.0.1"),
	"/data/fb/1013/router2/1116/2018/10/08/19/45/INET_DST_ADDR.bf": bfWithEntries("127.0.0.1"),

	"/data/fb/1013/router2/1116/2018/10/08/19/46/INET_SRC_ADDR.bf": bfWithEntries("127.0.0.1"),
	"/data/fb/1013/router2/1116/2018/10/08/19/46/INET_DST_ADDR.bf": bfWithEntries("127.0.0.1"),

	"/mem/fb/1013/router2/1116/2018/10/08/19/47/01/INET_SRC_ADDR.bf": bfWithEntries("127.0.0.1"),
	"/mem/fb/1013/router2/1116/2018/10/08/19/47/01/INET_DST_ADDR.bf": bfWithEntries("127.0.0.1"),
}

func bfWithEntries(entries ...string) []byte {
	bf := bloom.New(1000, 10)
	for i := range entries {
		bf.Add(fastbitAddrBytes(net.ParseIP(entries[i])))
	}
	return bfBytes(bf)
}

func bfBytes(bf *bloom.BloomFilter) []byte {
	var bs bytes.Buffer
	bf.WriteTo(&bs)
	return bs.Bytes()
}

/*
// Code to print out some KQL in tree form.

func TestBloomKQL(t *testing.T) {
	assert := assert.New(t)
	whereClauseString := getBloomFilterQuerySetTCs[len(getBloomFilterQuerySetTCs)-1].whereClause
	whereClause, err := kql.NewParser(strings.NewReader(whereClauseString)).ParseExpr()
	assert.NoError(err)
	walkPrint(whereClause, 0)
	printByCNF(whereClause)
}

func printByCNF(whereClause kql.Expr) {
	conj := gatherConj(whereClause)
	for i, e1 := range conj {
		fmt.Printf("conj %d: %s\n", i, e1.String())
		disj := gatherDisj(e1)
		for j, e2 := range disj {
			fmt.Printf("  disj %d: %s\n", j, e2.String())
		}
	}
}

func walkPrint(expr kql.Expr, level int) []string {
	tabs := nTabs(level)

	switch e := expr.(type) {
	case *kql.BinaryExpr:
		fmt.Printf("%sBinaryExpr:\n", tabs)
		walkPrint(e.LHS, level+1)
		fmt.Printf("%sBinaryExpr.Op: %s %s\n", tabs, e.Op, e.OpEx)
		walkPrint(e.RHS, level+1)
	case *kql.BooleanLiteral:
		fmt.Printf("%sBooleanLiteral: %s\n", tabs, e)
	case *kql.Call:
		fmt.Printf("%sCall: %s\n", tabs, e)
	case *kql.DateTimeCall:
		fmt.Printf("%sDateTimeCall: %s\n", tabs, e)
	case *kql.Case:
		fmt.Printf("%sCase: %s\n", tabs, e)
	case *kql.Distinct:
		fmt.Printf("%sDistinct: %s\n", tabs, e)
	case *kql.DurationLiteral:
		fmt.Printf("%sDurationLiteral: %s\n", tabs, e)
	case *kql.NumberLiteral:
		fmt.Printf("%sNumberLiteral: %s\n", tabs, e)
	case *kql.IntegerLiteral:
		fmt.Printf("%sIntegerLiteral: %s\n", tabs, e)
	case *kql.ParenExpr:
		fmt.Printf("%sParenExpr:\n", tabs)
		walkPrint(e.Expr, level+1)
	case *kql.RegexLiteral:
		fmt.Printf("%sRegexLiteral: %s\n", tabs, e)
	case *kql.StringLiteral:
		fmt.Printf("%sStringLiteral: %s\n", tabs, e)
	case *kql.TimeLiteral:
		fmt.Printf("%sTimeLiteral: %s\n", tabs, e)
	case *kql.VarRef:
		fmt.Printf("%sVarRef: %s\n", tabs, e)
	case *kql.Wildcard:
		fmt.Printf("%sWildcard: %s\n", tabs, e)
	default:
		fmt.Printf("%sUnknown: %s\n", tabs, e)
	}

	return nil
}

func nTabs(n int) string {
	tabs := make([]byte, n)
	for i := 0; i < n; i++ {
		tabs[i] = ' '
	}
	return string(tabs)
}

*/
