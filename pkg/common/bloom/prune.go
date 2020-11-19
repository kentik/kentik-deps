package bloom

import (
	"strings"

	"github.com/kentik/kql"
)

// Rewrite any <COLUMN> IN/NOT IN ('1.1.1.1,2.2.2.2') clauses as a series of explicit
// comparisons. If all partitions contain bloom filters for a column and enabled = true
// then any not present in the filter will be removed entirely.
func MaybeRewriteWhereClause(whereClause string, dataDirs, partitions []string, enabled bool) string {
	expr, err := kql.NewParser(strings.NewReader(whereClause)).ParseExpr()
	if err != nil {
		return whereClause
	}

	rewriteWhereColumnIn(expr, dataDirs, partitions, enabled)

	return expr.String()
}

func rewriteWhereColumnIn(expr kql.Expr, dataDirs, partitions []string, enabled bool) {
	switch e := expr.(type) {
	case *kql.BinaryExpr:
		isIN := e.Op == kql.IN || e.Op == kql.NIN
		if lhs, ok := e.LHS.(*kql.VarRef); ok && isIN {
			columnName := strings.ToLower(lhs.Val)
			_, isBloomCol := ColumnsWithBlooms[columnName]

			if rhs, ok := e.RHS.(*kql.ParenExpr); ok && isBloomCol {
				if list, ok := rhs.Expr.(*kql.StringLiteral); ok {
					var trueExpr = &kql.BinaryExpr{
						Op:  kql.EQ,
						LHS: &kql.NumberLiteral{Val: 1},
						RHS: &kql.NumberLiteral{Val: 1},
					}

					listExpr := rewriteColumnIn(e.Op, columnName, list.Val, dataDirs, partitions, enabled)

					e.Op = kql.AND
					e.LHS = trueExpr
					e.RHS = listExpr

					return
				}
			}
		}
		rewriteWhereColumnIn(e.LHS, dataDirs, partitions, enabled)
		rewriteWhereColumnIn(e.RHS, dataDirs, partitions, enabled)
	case *kql.ParenExpr:
		rewriteWhereColumnIn(e.Expr, dataDirs, partitions, enabled)
	}
}

func rewriteColumnIn(op kql.Token, columnName, list string, dataDirs, partitions []string, enabled bool) kql.Expr {
	addrs := strings.Split(list, ",")
	found := QueryAnyPartitionContains(columnName, addrs, dataDirs, partitions)
	exprs := []*kql.BinaryExpr{}

	var cmpOp, joinOp kql.Token
	switch op {
	case kql.IN:
		cmpOp, joinOp = kql.EQ, kql.OR
	case kql.NIN:
		cmpOp, joinOp = kql.NEQ, kql.AND
	}

	for _, addr := range addrs {
		if _, exists := found[addr]; exists || !enabled {
			exprs = append(exprs, &kql.BinaryExpr{
				Op: cmpOp,
				LHS: &kql.VarRef{
					Val: columnName,
				},
				RHS: &kql.StringLiteral{
					Val: addr,
				},
			})
		}
	}

	return &kql.ParenExpr{
		Expr: buildExprTree(joinOp, exprs),
	}
}

func buildExprTree(joinOp kql.Token, exprs []*kql.BinaryExpr) kql.Expr {
	switch len(exprs) {
	case 0:
		//     IN (...) with 0 matches -> 1 = 0
		// NOT IN (...) with 0 matches -> 1 = 1
		var n float64
		if joinOp == kql.OR {
			n = 0
		} else {
			n = 1
		}

		return &kql.BinaryExpr{
			Op:  kql.EQ,
			LHS: &kql.NumberLiteral{Val: 1},
			RHS: &kql.NumberLiteral{Val: n},
		}
	case 1:
		return exprs[0]
	}

	root := &kql.BinaryExpr{
		Op:  joinOp,
		LHS: exprs[0],
	}
	node := &root.RHS

	for i := 1; i < len(exprs)-1; i++ {
		bin := &kql.BinaryExpr{
			Op:  joinOp,
			LHS: exprs[i],
		}
		*node = bin
		node = &bin.RHS
	}
	*node = exprs[len(exprs)-1]

	return root
}
