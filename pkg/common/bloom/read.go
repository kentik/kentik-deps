package bloom

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kentik/kql"

	"github.com/DataDog/zstd"
	"github.com/colinmarc/cdb"
	"github.com/willf/bloom"
)

// SecondaryIndexesIndicateMaybeResults checks bloom filters on disk to see
// if this subquery will have any results. Bloom filters can have false positives,
// but not false negatives. Hence "maybe results".
func SecondaryIndexesIndicateMaybeResults(whereClause string, dataDirs, partitions []string, enabled bool, metricIndexTimerUpdate func(time.Duration), metricPrunedMark func(int64)) bool {
	if !enabled {
		return true
	}

	start := time.Now()
	result := parseKQLAndEvalBloomFilters(whereClause, dataDirs, partitions)
	metricIndexTimerUpdate(time.Since(start))
	if result {
		metricPrunedMark(1)
	}
	return result
}

func parseKQLAndEvalBloomFilters(whereClause string, dataDirs, partitions []string) bool {
	bfqs := getBloomFilterQuerySet(whereClause)
	return evalBloomFilterQuerySet(bfqs, dataDirs, partitions)
}

// ColumnsWithBlooms is hardcoded for now. In the future we may consider
// making this dynamic or assuming any column can have a bloom filter.
var ColumnsWithBlooms = setFromStringSlice([]string{
	InetSrcAddr,
	InetDstAddr,
})

// Relevant column names.
const (
	InetSrcAddr = "inet_src_addr"
	InetDstAddr = "inet_dst_addr"
)

func getBloomFilterQuerySet(whereClause string) *bloomFilterQuerySet {
	expr, err := kql.NewParser(strings.NewReader(whereClause)).ParseExpr()
	if err != nil {
		return nil // Unexpected that we can't parse query, don't know anything.
	}

	return getBloomFilterQuerySetFromWhere(expr)
}

func getBloomFilterQuerySetFromWhere(whereClause kql.Expr) *bloomFilterQuerySet {
	// If the request is amenable to speedups via bloom filter querying,
	// come up with a query plan.
	// Currently we'll only consider a query if it is a conjunction
	// of disjunctions, and one of those disjunctions is entirely
	// columns that we may have bloom filters for.

	if whereClause == nil {
		return nil
	}

	// Assume the root is a conjunction, a list of exprs AND'd.
	// (If it's not then we have a "conjunction" of length 1.)
	// We're trying to find a branch of the conjunction that is false,
	// so we can short circuit.
	rootConj := gatherConj(whereClause)
	c := cnfConjunction{Disjunctions: make([]disjunction, 0, len(rootConj))}
ConjunctionLoop:
	for _, branch := range rootConj {
		// Assume this branch is a disjunction, a list of exprs OR'd.
		// (If it's not then we have a "disjunction" of length 1.)
		// We're trying to show that this branch is false, so each
		// expression in the disjunction must be false.
		disj := gatherDisj(branch)
		d := disjunction{Terms: make([]bloomFilterQuery, len(disj))}
		for j, expr := range disj {
			// Check that this expr could possibly be evaluated as "false".
			// We need something like "foo = 'bar'" where foo is a
			// column we have a bloom filter for.
			bexpr, ok := expr.(*kql.BinaryExpr)
			if !ok { // This branch is not simple. Skip it.
				continue ConjunctionLoop
			}
			if !(bexpr.Op == kql.EQ && bexpr.OpEx == kql.ILLEGAL) {
				continue ConjunctionLoop
			}
			ref, ok := bexpr.LHS.(*kql.VarRef)
			if !ok {
				continue ConjunctionLoop
			}
			columnName := strings.ToLower(ref.Val)
			if _, mayHaveBloom := ColumnsWithBlooms[columnName]; !mayHaveBloom {
				continue ConjunctionLoop
			}
			lit, ok := bexpr.RHS.(*kql.StringLiteral)
			if !ok {
				continue ConjunctionLoop
			}

			valueBytes := []byte(lit.Val)
			switch columnName {
			case InetSrcAddr, InetDstAddr:
				valueBytes = fastbitAddrBytes(parseIPInStringLit(lit.Val))
				if valueBytes == nil {
					continue ConjunctionLoop
				}
			}
			d.Terms[j] = bloomFilterQuery{
				columnName: ref.Val,
				value:      valueBytes,
			}
		}

		c.Disjunctions = append(c.Disjunctions, d)
	}

	if len(c.Disjunctions) == 0 {
		return nil
	}

	return &bloomFilterQuerySet{expr: c}
}

func fastbitAddrBytes(ip net.IP) []byte {
	if ip == nil {
		return nil
	}
	if ip4 := ip.To4(); len(ip4) == net.IPv4len {
		bs := [addrLen]byte{}
		bs[0] = 4
		copy(bs[1:], ip4)
		return bs[0:17]
	}
	if len(ip) == net.IPv6len {
		bs := [addrLen]byte{}
		bs[0] = 6
		copy(bs[1:], ip)
		return bs[0:17]
	}
	return nil
}

func parseIPInStringLit(s string) net.IP {
	// If s was a CIDR and not /32 or /128 for v4 or v6 respectively,
	// then this will fail to parse. For now we're only looking at
	// specific ips.

	switch getIPFamily(s) {
	case 4:
		s = strings.TrimSuffix(s, "/32")
	case 6:
		s = strings.TrimSuffix(s, "/128")
	}

	return net.ParseIP(s)
}

func getIPFamily(s string) int {
	// family detection as in net.ParseIP
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '.':
			return 4
		case ':':
			return 6
		}
	}
	return 0
}

type bloomFilterQuerySet struct {
	expr cnfConjunction
}

// A cnfConjunction is the root of a boolean expression in CNF (conjunctive
// normal form). It's a conjunction (AND) of disjunctions (ORs).
type cnfConjunction struct {
	Disjunctions []disjunction
}

// A disjunction is a disjunction (OR) of bloom filter queries.
type disjunction struct {
	Terms []bloomFilterQuery
}

type bloomFilterQuery struct {
	columnName string
	value      []byte
}

func gatherConj(e kql.Expr) []kql.Expr { return gatherBinaryExprs(e, kql.AND, kql.ILLEGAL) }
func gatherDisj(e kql.Expr) []kql.Expr { return gatherBinaryExprs(e, kql.OR, kql.ILLEGAL) }

func gatherBinaryExprs(e kql.Expr, t, tEx kql.Token) []kql.Expr {
	result := make([]kql.Expr, 0)
	lhs := e
	b, isBinaryExpr := e.(*kql.BinaryExpr)
	for isBinaryExpr && b.Op == t && b.OpEx == tEx {
		// kql parses left-associative, so the RHS in a long list of binary exprs
		// will be a simple expr.
		result = append(result, removeParenExprs(b.RHS))
		lhs = b.LHS
		b, isBinaryExpr = b.LHS.(*kql.BinaryExpr)
	}
	if lhs != nil {
		result = append(result, removeParenExprs(lhs))
	}
	return result
}

func removeParenExprs(expr kql.Expr) kql.Expr {
	for pexpr, ok := expr.(*kql.ParenExpr); ok; pexpr, ok = expr.(*kql.ParenExpr) {
		expr = pexpr.Expr
	}
	return expr
}

func setFromStringSlice(ss []string) map[string]struct{} {
	m := make(map[string]struct{}, len(ss))
	for i := range ss {
		m[ss[i]] = struct{}{}
	}
	return m
}

func evalBloomFilterQuerySet(bfqs *bloomFilterQuerySet, dataDirs, partitions []string) bool {
	if bfqs == nil {
		return true
	}

DisjunctionsLoop:
	for i := range bfqs.expr.Disjunctions {
		for j := range bfqs.expr.Disjunctions[i].Terms {
			if evalBloomFilterQueryAllPartitions(bfqs.expr.Disjunctions[i].Terms[j], dataDirs, partitions) {
				// Bloom filter result was "true", which means this disjunction
				// could be true. Try the next one.
				continue DisjunctionsLoop
			}
		}
		if len(bfqs.expr.Disjunctions[i].Terms) > 0 {
			// All of the bloom filter OR'd exprs evaluated to false.
			// (We pruned the query!)
			return false
		}
	}

	// None of the AND'd exprs evaluated to false.
	// (We have to run the query, there could be results.)
	return true
}

func evalBloomFilterQueryAllPartitions(query bloomFilterQuery, dataDirs, partitions []string) bool {
	// We need to see a negative result from every partition that this subquery
	// would touch. If one of the partitions may be positive, then there may be
	// results. However, it could be that the different partitions are in different
	// "dataDirs" (e.g. one in /data/fb and one in /mem/fb), so we have to check
	// in multiple places.

PartitionLoop:
	for i := range partitions {
		for j := range dataDirs {
			v, err := evalBloomFilterQuery(query, dataDirs[j], partitions[i])
			if !v && err == nil { // A conclusive negative result.
				continue PartitionLoop
			}
		}
		if len(dataDirs) > 0 {
			// There were positive or inconclusive results for this partition,
			// so there may be results.
			return true
		}
	}
	// We got real negative results for every partition (or there's no partitions/dataDirs).
	return false
}

func evalBloomFilterQuery(query bloomFilterQuery, dataDir, partition string) (bool, error) {
	bf, err := readBloomFilterCDBOrFile(dataDir, partition, query.columnName)
	if err != nil {
		// true because not being able to read this bf means the value may be present.
		return true, err
	}

	v := bf.Test(query.value)
	return v, nil
}

// Search all partitions for a list of values and return the set of values that may
// exist in at least one partition.
func QueryAnyPartitionContains(columnName string, values, dataDirs, partitions []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))

	// By default we assume all values exist in some partition
	for _, value := range values {
		result[value] = struct{}{}
	}

	// If any partition lacks a filter then give up
	filters, err := gatherBloomFilters(dataDirs, partitions, columnName)
	if err != nil {
		return result
	}

SearchLoop:
	for _, value := range values {
		for _, filter := range filters {
			var valueBytes []byte
			switch columnName {
			case InetSrcAddr, InetDstAddr:
				valueBytes = fastbitAddrBytes(parseIPInStringLit(value))
			default:
				valueBytes = []byte(value)
			}

			if filter.Test(valueBytes) {
				continue SearchLoop
			}
		}
		delete(result, value)
	}

	return result
}

// Gather bloom filters for all partitions, or return nil and an error indicating
// why one filter could not be found.
func gatherBloomFilters(dataDirs, partitions []string, columnName string) ([]*bloom.BloomFilter, error) {
	filters := make([]*bloom.BloomFilter, 0, len(partitions))

GatherLoop:
	for _, partition := range partitions {
		var (
			err error
			bf  *bloom.BloomFilter
		)

		for _, dataDir := range dataDirs {
			bf, err = readBloomFilterCDBOrFile(dataDir, partition, columnName)
			if err == nil {
				filters = append(filters, bf)
				continue GatherLoop
			}
		}

		return nil, err
	}

	return filters, nil
}

func readBloomFilterCDBOrFile(dataDir, partition, columnName string) (*bloom.BloomFilter, error) {
	// Try CDB first, since in the future this will be the way.
	bf, err := readBloomFilterCDB(dataDir, partition, columnName)
	if err == nil {
		return bf, nil
	}

	// Fall back to plain files.
	return readBloomFilterFile(filepath.Join(dataDir, partition, strings.ToUpper(columnName)+".bf"))
}

const cdbRootPath = "/data/fbz"

func readBloomFilterCDB(dataDir, partition, columnName string) (*bloom.BloomFilter, error) {

	fbNew, fbOld := fb2fbz(partition)
	cdbPath := filepath.Join(cdbRootPath, fbNew)
	if !existsFile(cdbPath) {
		cdbPath = filepath.Join(cdbRootPath, fbOld)
	}

	cdbFile, err := openFile(cdbPath)
	if err != nil {
		return nil, err
	}
	defer cdbFile.Close()
	db, err := cdb.New(cdbFile, nil)
	if err != nil {
		return nil, err
	}
	compressedBytes, err := db.Get([]byte(strings.ToUpper(columnName) + ".bf"))
	if err != nil {
		return nil, err
	}
	bs, err := zstd.Decompress(nil, compressedBytes)
	if err != nil {
		return nil, err
	}
	return readBloomFilterStream(bytes.NewReader(bs))
}

func readBloomFilterFile(filename string) (*bloom.BloomFilter, error) {
	file, err := openFile(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return readBloomFilterStream(file)
}

// readBloomFilterStream reads a bloom filter in the binary format
// written by writeBloomFilter (three uint64s and bits).
func readBloomFilterStream(stream io.Reader) (*bloom.BloomFilter, error) {
	bf := &bloom.BloomFilter{} // all fields will be overwritten by ReadFrom.
	_, err := bf.ReadFrom(stream)
	if err != nil {
		return nil, err
	}
	return bf, nil
}

//foo#1 -> 1
//foo -> 0
func getShardNum(device string) uint32 {
	if !strings.Contains(device, "#") {
		return 0
	}
	pts := strings.Split(device, "#")
	val, err := strconv.Atoi(pts[1])
	if err != nil {
		return 0
	}
	return uint32(val)
}

// fb2fbz copied from runner with minor modification.
// 23549/pesa0309_01_bbn_mgmt_sin2_secureserver_net/17403/2018/09/27/17/04 -> 23549/17403/MM/2018/09/27/23549-17403-2018-09-27-17:04.cdb
// 2009/qfx_5012/8373/HH/2018/09/27/07 -> 2009/8373/HH/2018/09/27/2009-8373-2018-09-27-07:HH.cdb
func fb2fbz(file string) (string, string) {
	pts := strings.Split(file, "/")
	if len(pts) < 8 {
		return "", ""
	}
	shardNum := getShardNum(pts[1])

	var newDir []string
	var newDirOld []string
	if pts[3] == "HH" {
		newFile := fmt.Sprintf("%s-%s-%s-%s-%s-%s:HH-%d.cdb", pts[0], pts[2], pts[4], pts[5], pts[6], pts[7], shardNum)
		newFileOld := fmt.Sprintf("%s-%s-%s-%s-%s-%s:HH.cdb", pts[0], pts[2], pts[4], pts[5], pts[6], pts[7])
		base := []string{
			pts[0],
			pts[2],
			"HH",
			pts[4],
			pts[5],
			pts[6],
		}
		newDir = append(base, newFile)
		newDirOld = append(base, newFileOld)
	} else {
		newFile := fmt.Sprintf("%s-%s-%s-%s-%s-%s:%s-%d.cdb", pts[0], pts[2], pts[3], pts[4], pts[5], pts[6], pts[7], shardNum)
		newFileOld := fmt.Sprintf("%s-%s-%s-%s-%s-%s:%s.cdb", pts[0], pts[2], pts[3], pts[4], pts[5], pts[6], pts[7])
		base := []string{
			pts[0],
			pts[2],
			"MM",
			pts[3],
			pts[4],
			pts[5],
		}
		newDir = append(base, newFile)
		newDirOld = append(base, newFileOld)
	}
	return strings.Join(newDirOld, "/"), strings.Join(newDir, "/")
}
