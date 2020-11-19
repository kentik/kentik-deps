package kql

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	// DateFormat represents the format for date literals.
	DateFormat = "2006-01-02"

	// DateTimeFormat represents the format for date time literals.
	DateTimeFormat = "2006-01-02 15:04:05-07"
)

// Parser represents an kQL parser.
type Parser struct {
	s *bufScanner
}

var (
	quoteStringReplacer = strings.NewReplacer("\n", `\n`, `\`, `\\`, `'`, `\'`)
	quoteIdentReplacer  = strings.NewReplacer("\n", `\n`, `\`, `\\`, `"`, `\"`)
)

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	return &Parser{s: newBufScanner(r)}
}

// ParseQuery parses a query string and returns its AST representation.
func ParseQuery(s string) (*Query, error) { return NewParser(strings.NewReader(s)).ParseQuery() }

// ParseStatement parses a statement string and returns its AST representation.
func ParseStatement(s string) (Statement, error) {
	return NewParser(strings.NewReader(s)).ParseStatement()
}

// MustParseStatement parses a statement string and returns its AST. Panic on error.
func MustParseStatement(s string) Statement {
	stmt, err := ParseStatement(s)
	if err != nil {
		panic(err.Error())
	}
	return stmt
}

// ParseExpr parses an expression string and returns its AST representation.
func ParseExpr(s string) (Expr, error) { return NewParser(strings.NewReader(s)).ParseExpr() }

// ParseQuery parses an kQL string and returns a Query AST object.
func (p *Parser) ParseQuery() (*Query, error) {
	var statements Statements
	var semi bool

	for {
		if tok, _, _ := p.scanIgnoreWhitespace(); tok == EOF {
			return &Query{Statements: statements}, nil
		} else if !semi && tok == SEMICOLON {
			semi = true
		} else {
			p.unscan()
			s, err := p.ParseStatement()
			if err != nil {
				return nil, err
			}
			statements = append(statements, s)
			semi = false
		}
	}
}

// ParseStatement parses an kQL string and returns a Statement AST object.
func (p *Parser) ParseStatement() (Statement, error) {
	// Inspect the first token.
	tok, pos, lit := p.scanIgnoreWhitespace()
	switch tok {
	case SELECT:
		return p.parseSelectStatement(targetNotRequired)
	default:
		return nil, newParseError(tokstr(tok, lit), []string{"SELECT"}, pos)
	}
}

// parseInt parses a string and returns an integer literal.
func (p *Parser) parseInt(min, max int) (int, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != NUMBER {
		return 0, newParseError(tokstr(tok, lit), []string{"number"}, pos)
	}

	// Return an error if the number has a fractional part.
	if strings.Contains(lit, ".") {
		return 0, &ParseError{Message: "number must be an integer", Pos: pos}
	}

	// Convert string to int.
	n, err := strconv.Atoi(lit)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	} else if min > n || n > max {
		return 0, &ParseError{
			Message: fmt.Sprintf("invalid value %d: must be %d <= n <= %d", n, min, max),
			Pos:     pos,
		}
	}

	return n, nil
}

// parseUInt32 parses a string and returns a 32-bit unsigned integer literal.
func (p *Parser) parseUInt32() (uint32, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != NUMBER {
		return 0, newParseError(tokstr(tok, lit), []string{"number"}, pos)
	}

	// Convert string to unsigned 32-bit integer
	n, err := strconv.ParseUint(lit, 10, 32)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	}

	return uint32(n), nil
}

// parseUInt64 parses a string and returns a 64-bit unsigned integer literal.
func (p *Parser) parseUInt64() (uint64, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != NUMBER {
		return 0, newParseError(tokstr(tok, lit), []string{"number"}, pos)
	}

	// Convert string to unsigned 64-bit integer
	n, err := strconv.ParseUint(lit, 10, 64)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	}

	return uint64(n), nil
}

// parseDuration parses a string and returns a duration literal.
// This function assumes the DURATION token has already been consumed.
func (p *Parser) parseDuration() (time.Duration, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != DURATION_VAL && tok != INF {
		return 0, newParseError(tokstr(tok, lit), []string{"duration"}, pos)
	}

	if tok == INF {
		return 0, nil
	}

	d, err := ParseDuration(lit)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	}

	return d, nil
}

// parseIdent parses an identifier.
func (p *Parser) parseIdent() (string, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != IDENT {
		return "", newParseError(tokstr(tok, lit), []string{"identifier"}, pos)
	}
	return lit, nil
}

// parseIdentList parses a comma delimited list of identifiers.
func (p *Parser) parseIdentList() ([]string, error) {
	// Parse first (required) identifier.
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	idents := []string{ident}

	// Parse remaining (optional) identifiers.
	for {
		if tok, _, _ := p.scanIgnoreWhitespace(); tok != COMMA {
			p.unscan()
			return idents, nil
		}

		if ident, err = p.parseIdent(); err != nil {
			return nil, err
		}

		idents = append(idents, ident)
	}
}

// parseSegmentedIdents parses a segmented identifiers.
// e.g.,  "db"."rp".measurement  or  "db"..measurement
func (p *Parser) parseSegmentedIdents() ([]string, error) {
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	idents := []string{ident}

	// Parse remaining (optional) identifiers.
	for {
		if tok, _, _ := p.scan(); tok != DOT {
			// No more segments so we're done.
			p.unscan()
			break
		}

		if ch := p.peekRune(); ch == '\'' {
			// Next segment is a regex so we're done.
			break
		} else if ch == ':' {
			// Next segment is context-specific so let caller handle it.
			break
		} else if ch == '.' {
			// Add an empty identifier.
			idents = append(idents, "")
			continue
		}

		// Parse the next identifier.
		if ident, err = p.parseIdent(); err != nil {
			return nil, err
		}

		idents = append(idents, ident)
	}

	if len(idents) > 3 {
		msg := fmt.Sprintf("too many segments in %s", QuoteIdent(idents...))
		return nil, &ParseError{Message: msg}
	}

	return idents, nil
}

// parserString parses a string.
func (p *Parser) parseString() (string, error) {
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != STRING {
		return "", newParseError(tokstr(tok, lit), []string{"string"}, pos)
	}
	return lit, nil
}

// parserString parses a string.
func (p *Parser) parseStringList() ([]string, error) {
	// Parse first (required) string.
	str, err := p.parseString()
	if err != nil {
		return nil, err
	}
	strs := []string{str}

	// Parse remaining (optional) strings.
	for {
		if tok, _, _ := p.scanIgnoreWhitespace(); tok != COMMA {
			p.unscan()
			return strs, nil
		}

		if str, err = p.parseString(); err != nil {
			return nil, err
		}

		strs = append(strs, str)
	}
}

// parseSelectStatement parses a select string and returns a Statement AST object.
// This function assumes the SELECT token has already been consumed.
func (p *Parser) parseSelectStatement(tr targetRequirement) (*SelectStatement, error) {
	stmt := &SelectStatement{}
	var err error

	// Parse fields: "FIELD+".
	if stmt.Fields, err = p.parseFields(); err != nil {
		return nil, err
	}

	// Parse target: "INTO"
	if stmt.Target, err = p.parseTarget(tr); err != nil {
		return nil, err
	}

	// Parse source: "FROM".
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != FROM {
		return nil, newParseError(tokstr(tok, lit), []string{"FROM"}, pos)
	}
	if stmt.Sources, err = p.parseSources(); err != nil {
		return nil, err
	}

	// Parse condition: "WHERE EXPR".
	if stmt.Condition, err = p.parseCondition(WHERE); err != nil {
		return nil, err
	}

	// Parse dimensions: "GROUP BY DIMENSION+".
	if stmt.Dimensions, err = p.parseDimensions(); err != nil {
		return nil, err
	}

	// Parse condition: "HAVING EXPR".
	if stmt.HavingCondition, err = p.parseCondition(HAVING); err != nil {
		return nil, err
	}

	// Parse fill options: "fill(<option>)"
	if stmt.Fill, stmt.FillValue, err = p.parseFill(); err != nil {
		return nil, err
	}

	// Parse sort: "ORDER BY FIELD+".
	if stmt.SortFields, err = p.parseOrderBy(); err != nil {
		return nil, err
	}

	// Parse limit: "LIMIT <n>".
	if stmt.Limit, err = p.parseOptionalTokenAndInt(LIMIT); err != nil {
		return nil, err
	}

	// Parse offset: "OFFSET <n>".
	if stmt.Offset, err = p.parseOptionalTokenAndInt(OFFSET); err != nil {
		return nil, err
	}

	// Parse optional query ID
	stmt.QueryId = p.parseOptionalQueryId()

	// Set if the query is a raw data query or one with an aggregate
	stmt.IsRawQuery = true
	WalkFunc(stmt.Fields, func(n Node) {
		if _, ok := n.(*Call); ok {
			stmt.IsRawQuery = false
		}
	})

	if err := stmt.validate(tr); err != nil {
		return nil, err
	}

	return stmt, nil
}

// targetRequirement specifies whether or not a target clause is required.
type targetRequirement int

const (
	targetRequired targetRequirement = iota
	targetNotRequired
)

// parseTarget parses a string and returns a Target.
func (p *Parser) parseTarget(tr targetRequirement) (*Target, error) {
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != INTO {
		if tr == targetRequired {
			return nil, newParseError(tokstr(tok, lit), []string{"INTO"}, pos)
		}
		p.unscan()
		return nil, nil
	}

	// db, rp, and / or measurement
	idents, err := p.parseSegmentedIdents()
	if err != nil {
		return nil, err
	}

	if len(idents) < 3 {
		// Check for source measurement reference.
		if ch := p.peekRune(); ch == ':' {
			//if err := p.parseTokens([]Token{COLON, MEASUREMENT}); err != nil {
			//	return nil, err
			//}
			//// Append empty measurement name.
			idents = append(idents, "")
		}
	}

	t := &Target{Measurement: &Measurement{IsTarget: true}}

	switch len(idents) {
	case 1:
		t.Measurement.Name = idents[0]
	case 2:
		t.Measurement.RetentionPolicy = idents[0]
		t.Measurement.Name = idents[1]
	case 3:
		t.Measurement.Database = idents[0]
		t.Measurement.RetentionPolicy = idents[1]
		t.Measurement.Name = idents[2]
	}

	return t, nil
}

// parseFields parses a list of one or more fields.
func (p *Parser) parseFields() (Fields, error) {
	var fields Fields

	for {
		// Parse the field.
		f, err := p.parseField()
		if err != nil {
			return nil, err
		}

		// Add new field.
		fields = append(fields, f)

		// If there's not a comma next then stop parsing fields.
		if tok, _, _ := p.scan(); tok != COMMA {
			p.unscan()
			break
		}
	}

	return fields, nil
}

// parseField parses a single field.
func (p *Parser) parseField() (*Field, error) {
	f := &Field{}

	_, pos, _ := p.scanIgnoreWhitespace()
	p.unscan()
	// Parse the expression first.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}
	var c validateField
	Walk(&c, expr)
	if c.foundInvalid {
		return nil, fmt.Errorf("invalid operator %s in SELECT clause at line %d, char %d; operator is intended for WHERE clause", c.badToken, pos.Line+1, pos.Char+1)
	}
	f.Expr = expr

	// Parse the alias if the current and next tokens are "WS AS".
	alias, err := p.parseAlias()
	if err != nil {
		return nil, err
	}
	f.Alias = alias

	// Consume all trailing whitespace.
	p.consumeWhitespace()

	return f, nil
}

// validateField checks if the Expr is a valid field. We disallow all binary expression
// that return a boolean
type validateField struct {
	foundInvalid bool
	badToken     Token
}

func (c *validateField) Visit(n Node) Visitor {
	e, ok := n.(*BinaryExpr)
	if !ok {
		return c
	}

	switch e.Op {
	case EQ, NEQ, EQREGEX, EQREGEXCASE, EQLIKE, EQLIKECASE,
		NEQREGEX, NEQREGEXCASE, NEQLIKE, NEQLIKECASE,
		LT, LTE, GT, GTE,
		AND, OR:
		c.foundInvalid = true
		c.badToken = e.Op
		return nil
	}
	return c
}

// parseAlias parses the "AS IDENT" alias for fields and dimensions.
func (p *Parser) parseAlias() (string, error) {
	// Check if the next token is "AS". If not, then unscan and exit.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != AS {
		p.unscan()
		return "", nil
	}

	// Then we should have the alias identifier.
	lit, err := p.parseIdent()
	if err != nil {
		return "", err
	}
	return lit, nil
}

// parseSources parses a comma delimited list of sources.
func (p *Parser) parseSources() (Sources, error) {
	var sources Sources

	for {
		s, err := p.parseSubQuery()
		if err != nil {
			return nil, err
		} else if s == nil {
			s, err = p.parseSource()
			if err != nil {
				return nil, err
			}
		}
		sources = append(sources, s)

		if tok, _, _ := p.scanIgnoreWhitespace(); tok != COMMA {
			p.unscan()
			break
		}
	}

	return sources, nil
}

// peekRune returns the next rune that would be read by the scanner.
func (p *Parser) peekRune() rune {
	r, _, _ := p.s.s.r.ReadRune()
	if r != eof {
		_ = p.s.s.r.UnreadRune()
	}

	return r
}

func (p *Parser) parseSource() (Source, error) {
	m := &Measurement{}

	// Attempt to parse a regex.
	re, err := p.parseRegex(false)
	if err != nil {
		return nil, err
	} else if re != nil {
		m.Regex = re
		// Regex is always last so we're done.
		return m, nil
	}

	// Didn't find a regex so parse segmented identifiers.
	idents, err := p.parseSegmentedIdents()
	if err != nil {
		return nil, err
	}

	// If we already have the max allowed idents, we're done.
	if len(idents) == 3 {
		m.Database, m.RetentionPolicy, m.Name = idents[0], idents[1], idents[2]
		return m, nil
	}
	// Check again for regex.
	re, err = p.parseRegex(false)
	if err != nil {
		return nil, err
	} else if re != nil {
		m.Regex = re
	}

	// Assign identifiers to their proper locations.
	switch len(idents) {
	case 1:
		if re != nil {
			m.RetentionPolicy = idents[0]
		} else {
			m.Name = idents[0]
		}
	case 2:
		if re != nil {
			m.Database, m.RetentionPolicy = idents[0], idents[1]
		} else {
			m.RetentionPolicy, m.Name = idents[0], idents[1]
		}
	}

	if tok, _, _ := p.scanIgnoreWhitespace(); tok == AS {
		lit, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		m.Alias = lit
	} else {
		p.unscan()
	}

	return m, nil
}

// parseSubQuery parse the subquery, if it exists.
func (p *Parser) parseSubQuery() (Source, error) {
	s := &SubQuery{}

	nextRune := p.peekRune()
	if isWhitespace(nextRune) {
		p.consumeWhitespace()
	}
	nextRune = p.peekRune()
	if nextRune != '(' {
		return nil, nil
	}

	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != LPAREN {
		return nil, newParseError(tokstr(tok, lit), []string{"("}, pos)
	}

	stmt, err := p.ParseStatement()
	if err != nil {
		return nil, err
	}

	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != RPAREN {
		return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
	}

	tok, _, lit := p.scanIgnoreWhitespace()
	if tok == AS {
		lit, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		s.Alias = lit
	} else if tok == IDENT {
		s.Alias = lit
	} else {
		p.unscan()
	}

	s.Query = stmt
	return s, nil
}

// parseCondition parses the "WHERE" clause of the query, if it exists.
func (p *Parser) parseCondition(t Token) (Expr, error) {
	// Check if the WHERE token exists.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != t {
		p.unscan()
		return nil, nil
	}

	// Scan the identifier for the source.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	return expr, nil
}

// parseDimensions parses the "GROUP BY" clause of the query, if it exists.
func (p *Parser) parseDimensions() (Dimensions, error) {
	// If the next token is not GROUP then exit.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != GROUP {
		p.unscan()
		return nil, nil
	}

	// Now the next token should be "BY".
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != BY {
		return nil, newParseError(tokstr(tok, lit), []string{"BY"}, pos)
	}

	var dimensions Dimensions
	for {
		// Parse the dimension.
		d, err := p.parseDimension()
		if err != nil {
			return nil, err
		}

		// Add new dimension.
		dimensions = append(dimensions, d)

		// If there's not a comma next then stop parsing dimensions.
		if tok, _, _ := p.scan(); tok != COMMA {
			p.unscan()
			break
		}
	}
	return dimensions, nil
}

// parseDimension parses a single dimension.
func (p *Parser) parseDimension() (*Dimension, error) {
	// Parse the expression first.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	// Consume all trailing whitespace.
	p.consumeWhitespace()

	return &Dimension{Expr: expr}, nil
}

// parseFill parses the fill call and its options.
func (p *Parser) parseFill() (FillOption, interface{}, error) {
	// Parse the expression first.
	expr, err := p.ParseExpr()
	if err != nil {
		p.unscan()
		return NullFill, nil, nil
	}
	lit, ok := expr.(*Call)
	if !ok {
		p.unscan()
		return NullFill, nil, nil
	}
	if strings.ToLower(lit.Name) != "fill" {
		p.unscan()
		return NullFill, nil, nil
	}
	if len(lit.Args) != 1 {
		return NullFill, nil, errors.New("fill requires an argument, e.g.: 0, null, none, previous")
	}
	switch lit.Args[0].String() {
	case "null":
		return NullFill, nil, nil
	case "none":
		return NoFill, nil, nil
	case "previous":
		return PreviousFill, nil, nil
	default:
		num, ok := lit.Args[0].(*NumberLiteral)
		if !ok {
			return NullFill, nil, fmt.Errorf("expected number argument in fill()")
		}
		return NumberFill, num.Val, nil
	}
}

// parseOptionalQueryId parses the specified query identifier
func (p *Parser) parseOptionalQueryId() string {
	tok, _, lit := p.scanIgnoreWhitespace()
	if tok != IDENT {
		p.unscan()
		return ""
	}
	return lit
}

// parseOptionalTokenAndInt parses the specified token followed
// by an int, if it exists.
func (p *Parser) parseOptionalTokenAndInt(t Token) (int, error) {
	// Check if the token exists.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != t {
		p.unscan()
		return 0, nil
	}

	// Scan the number.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok != NUMBER {
		return 0, newParseError(tokstr(tok, lit), []string{"number"}, pos)
	}

	// Return an error if the number has a fractional part.
	if strings.Contains(lit, ".") {
		msg := fmt.Sprintf("fractional parts not allowed in %s", t.String())
		return 0, &ParseError{Message: msg, Pos: pos}
	}

	// Parse number.
	n, _ := strconv.ParseInt(lit, 10, 64)

	if n < 0 {
		msg := fmt.Sprintf("%s must be >= 0", t.String())
		return 0, &ParseError{Message: msg, Pos: pos}
	}

	return int(n), nil
}

// parseOrderBy parses the "ORDER BY" clause of a query, if it exists.
func (p *Parser) parseOrderBy() (SortFields, error) {
	// Return nil result and nil error if no ORDER token at this position.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok != ORDER {
		p.unscan()
		return nil, nil
	}

	// Parse the required BY token.
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok != BY {
		return nil, newParseError(tokstr(tok, lit), []string{"BY"}, pos)
	}

	// Parse the ORDER BY fields.
	fields, err := p.parseSortFields()
	if err != nil {
		return nil, err
	}

	return fields, nil
}

// parseSortFields parses the sort fields for an ORDER BY clause.
func (p *Parser) parseSortFields() (SortFields, error) {
	var fields SortFields

SCANLOOP:
	for {
		tok0, pos0, lit0 := p.scanIgnoreWhitespace()

		switch tok0 {
		case ASC, DESC:
			fields = append(fields, &SortField{Ascending: (tok0 == ASC)})
		default:
			p.unscan()
			expr, err := p.ParseExpr()
			if err != nil {
				return nil, newParseError(tokstr(tok0, lit0), []string{"identifier", "expression", "ASC", "DESC"}, pos0)
			}
			switch expr.(type) {
			case *VarRef, *Call, *DateTimeCall, *BinaryExpr, *ParenExpr:
				tok1, _, _ := p.scanIgnoreWhitespace()
				switch tok1 {
				case ASC, DESC:
					fields = append(fields, &SortField{Expr: expr, Ascending: (tok1 == ASC)})
				default:
					p.unscan()
					fields = append(fields, &SortField{Expr: expr})
				}
			default:
				return nil, newParseError(tokstr(tok0, lit0), []string{"identifier", "expression", "ASC", "DESC"}, pos0)
			}
		}

		tok2, _, _ := p.scanIgnoreWhitespace()
		switch tok2 {
		case COMMA:
			// Do nothing
		default:
			p.unscan()
			break SCANLOOP
		}
	}

	return fields, nil
}

// parseVarRef parses a reference to a measurement or field.
func (p *Parser) parseVarRef() (*VarRef, error) {
	// Parse the segments of the variable ref.
	segments, err := p.parseSegmentedIdents()
	if err != nil {
		return nil, err
	}

	vr := &VarRef{Val: strings.Join(segments, ".")}
	return vr, nil
}

// ParseExpr parses an expression.
func (p *Parser) ParseExpr() (Expr, error) {
	var err error
	// Dummy root node.
	root := &BinaryExpr{}

	// Parse a non-binary expression type to start.
	// This variable will always be the root of the expression tree.
	root.RHS, err = p.parseUnaryExpr()
	if err != nil {
		return nil, err
	}

	// Loop over operations and unary exprs and build a tree based on precendence.
	for {
		var opEx Token
		// If the next token is NOT an operator then return the expression.
		op, pos, _ := p.scanIgnoreWhitespace()
		if !op.isOperator() {
			p.unscan()
			return root.RHS, nil
		} else if op == NOT {
			op1, pos, lit := p.scanIgnoreWhitespace()
			switch op1 {
			case LIKE:
				op = NLIKE
			case IN:
				op = NIN
			case LPAREN:
				p.unscan()
				pe, err := p.parseUnaryExpr()
				if err != nil {
					return nil, err
				}
				return pe, nil
			default:
				return nil, newParseError(tokstr(op1, lit), []string{"LIKE", "IN"}, pos)
			}
		}

		// Otherwise parse the next expression.
		var rhs Expr
		if IsRegexOp(op) {
			// RHS of a regex operator must be a regular expression.
			p.consumeWhitespace()
			if rhs, err = p.parseRegex(IsLikeOp(op)); err != nil {
				return nil, err
			}
			// parseRegex can return an empty type, but we need it to be present
			if rhs.(*RegexLiteral) == nil {
				tok, pos, lit := p.scanIgnoreWhitespace()
				return nil, newParseError(tokstr(tok, lit), []string{"regex"}, pos)
			}
		} else {
			// Check for optional array ops
			op1, _, _ := p.scanIgnoreWhitespace()
			if op1 == ALL || op1 == ANY {
				opEx = op1
			} else {
				p.unscan()
			}

			if rhs, err = p.parseUnaryExpr(); err != nil {
				return nil, err
			}
		}

		if op == IS {
			switch rhs := rhs.(type) {
			case *NullLiteral:
			case *NegateExpr:
				_, ok := rhs.Expr.(*NullLiteral)
				if !ok {
					return nil, newParseError(rhs.String(), []string{"NOT NULL"}, pos)
				}
			default:
				return nil, newParseError(rhs.String(), []string{"NULL"}, pos)
			}
		}

		// Find the right spot in the tree to add the new expression by
		// descending the RHS of the expression tree until we reach the last
		// BinaryExpr or a BinaryExpr whose RHS has an operator with
		// precedence >= the operator being added.
		for node := root; ; {
			r, ok := node.RHS.(*BinaryExpr)
			if !ok || r.Op.Precedence() >= op.Precedence() {
				// Add the new expression here and break.
				node.RHS = &BinaryExpr{LHS: node.RHS, RHS: rhs, Op: op, OpEx: opEx}
				break
			}
			node = r
		}
	}
}

// parseUnaryExpr parses an non-binary expression.
func (p *Parser) parseUnaryExpr() (Expr, error) {
	// If the first token is a LPAREN then parse it as its own grouped expression.
	if tok, _, _ := p.scanIgnoreWhitespace(); tok == LPAREN {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}

		// Expect an RPAREN at the end.
		if tok, pos, lit := p.scanIgnoreWhitespace(); tok != RPAREN {
			return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
		}

		return &ParenExpr{Expr: expr}, nil
	}
	p.unscan()

	// Read next token.
	tok, pos, lit := p.scanIgnoreWhitespace()
	switch tok {
	case IDENT:
		// If the next immediate token is a left parentheses, parse as function call.
		// Otherwise parse as a variable reference.
		if tok0, _, _ := p.scan(); tok0 == LPAREN {
			return p.parseCall(lit)
		} else {
			switch lit {
			case "time", "timestamp", "interval", "date":
				return p.parseDateTimeCall(lit)
			}
		}

		p.unscan() // unscan the last token (wasn't an LPAREN)
		p.unscan() // unscan the IDENT token

		// Parse it as a VarRef.
		return p.parseVarRef()
	case DISTINCT:
		// If the next immediate token is a left parentheses, parse as function call.
		// Otherwise parse as a Distinct expression.
		tok0, pos, lit := p.scan()
		if tok0 == LPAREN {
			return p.parseCall("distinct")
		} else if tok0 == WS {
			tok1, pos, lit := p.scanIgnoreWhitespace()
			if tok1 != IDENT {
				return nil, newParseError(tokstr(tok1, lit), []string{"identifier"}, pos)
			}
			return &Distinct{Val: lit}, nil
		}

		return nil, newParseError(tokstr(tok0, lit), []string{"(", "identifier"}, pos)
	case STRING:
		// If literal looks like a date time then parse it as a time literal.
		if isDateTimeString(lit) {
			t, err := time.Parse(DateTimeFormat, lit)
			if err != nil {
				// try to parse it as an RFCNano time
				t, err := time.Parse(time.RFC3339Nano, lit)
				if err != nil {
					return nil, &ParseError{Message: "unable to parse datetime", Pos: pos}
				}
				return &TimeLiteral{Val: t}, nil
			}
			return &TimeLiteral{Val: t}, nil
		} else if isDateString(lit) {
			t, err := time.Parse(DateFormat, lit)
			if err != nil {
				return nil, &ParseError{Message: "unable to parse date", Pos: pos}
			}
			return &TimeLiteral{Val: t}, nil
		}
		return &StringLiteral{Val: lit}, nil
	case NUMBER:
		v, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return nil, &ParseError{Message: "unable to parse number", Pos: pos}
		}
		return &NumberLiteral{Val: v}, nil
	case TRUE, FALSE:
		return &BooleanLiteral{Val: (tok == TRUE)}, nil
	case DURATION_VAL:
		v, _ := ParseDuration(lit)
		return &DurationLiteral{Val: v}, nil
	case MUL:
		return &Wildcard{}, nil
	case REGEX:
		re, err := regexp.Compile(lit)
		if err != nil {
			return nil, &ParseError{Message: err.Error(), Pos: pos}
		}
		return &RegexLiteral{Val: re}, nil
	case CASE:
		ce, err := p.parseCase()
		if err != nil {
			return nil, &ParseError{Message: err.Error(), Pos: pos}
		}
		return ce, nil
	case SELECT:
		p.unscan()
		stmt, err := p.ParseStatement()
		if err != nil {
			return nil, &ParseError{Message: err.Error(), Pos: pos}
		} else if selstmt, ok := stmt.(*SelectStatement); ok {
			return selstmt, nil
		}
		return nil, &ParseError{Message: "unable to parse subquery", Pos: pos}
	case NOT:
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		return &NegateExpr{Expr: expr}, nil
	case NULL:
		return &NullLiteral{}, nil
	default:
		return nil, newParseError(tokstr(tok, lit), []string{"identifier", "string", "number", "bool"}, pos)
	}
}

// parseRegex parses a regular expression.
func (p *Parser) parseRegex(isLike bool) (*RegexLiteral, error) {
	nextRune := p.peekRune()
	if isWhitespace(nextRune) {
		p.consumeWhitespace()
	}

	// If the next character is not a '\'', then return nils.
	nextRune = p.peekRune()
	if nextRune != '\'' {
		return nil, nil
	}

	tok, pos, lit := p.s.ScanRegex()

	if tok == BADESCAPE {
		msg := fmt.Sprintf("bad escape: %s", lit)
		return nil, &ParseError{Message: msg, Pos: pos}
	} else if tok == BADREGEX {
		msg := fmt.Sprintf("bad regex: %s", lit)
		return nil, &ParseError{Message: msg, Pos: pos}
	} else if tok != REGEX {
		return nil, newParseError(tokstr(tok, lit), []string{"regex"}, pos)
	}

	if shouldQuote(isLike, lit) {
		lit = regexp.QuoteMeta(lit)
	}

	re, err := regexp.Compile(lit)
	if err != nil {
		return nil, &ParseError{Message: err.Error(), Pos: pos}
	}

	return &RegexLiteral{Val: re}, nil
}

// Only quote the RE if it's an operand of a LIKE operator, and NOT a CIDR or
// IP address.
func shouldQuote(isLike bool, s string) bool {
	if !isLike {
		return false
	}
	_, _, err := net.ParseCIDR(s)
	if err == nil { // it's a CIDR, don't quote it
		return false
	}
	if net.ParseIP(s) != nil { // it's an IP, don't quote it
		return false
	}
	// It's a LIKE operand and not a CIDR or IP; quote it
	return true
}

// parseCall parses a function call.
// This function assumes the function name and LPAREN have been consumed.
func (p *Parser) parseCall(name string) (*Call, error) {
	name = strings.ToLower(name)
	// If there's a right paren then just return immediately.
	if tok, _, _ := p.scan(); tok == RPAREN {
		return &Call{Name: name}, nil
	}
	p.unscan()

	// Otherwise parse function call arguments.
	var args []Expr
	for {
		// Parse an expression argument.
		arg, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)

		// If there's not a comma next then stop parsing arguments.
		if tok, _, _ := p.scan(); tok != COMMA {
			p.unscan()
			break
		}
	}

	// There should be a right parentheses at the end.
	if tok, pos, lit := p.scan(); tok != RPAREN {
		return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
	}

	return &Call{Name: name, Args: args}, nil
}

// parseDateTimeCall parses a date/time function call.
func (p *Parser) parseDateTimeCall(name string) (*DateTimeCall, error) {
	c := &DateTimeCall{}
	c.Name = strings.ToLower(name)

	if lit, err := p.parseString(); err != nil {
		p.unscan()
		return c, nil
	} else {
		c.Arg = lit
	}
	return c, nil
}

// parseCase parses a case conditional.
// This function assumes CASE have been consumed.
func (p *Parser) parseCase() (*Case, error) {
	var conds []Expr
	var vals []Expr
	var elseval Expr

	for {
		tok, pos, lit := p.scanIgnoreWhitespace()
		switch tok {
		case WHEN:
			expr1, err := p.ParseExpr()
			if err != nil {
				return nil, err
			}

			tok, pos, lit := p.scanIgnoreWhitespace()
			if tok != THEN {
				return nil, newParseError(tokstr(tok, lit), []string{"THEN"}, pos)
			}

			expr2, err := p.ParseExpr()
			if err != nil {
				return nil, err
			}

			// FIXME add validations for THEN literal expression
			switch expr2.(type) {
			case *StringLiteral,
				*NumberLiteral,
				*TimeLiteral,
				*ParenExpr,
				*VarRef:
				break
			default:
				return nil, newParseError(tokstr(tok, lit), []string{"literal"}, pos)
			}

			conds = append(conds, expr1)
			vals = append(vals, expr2)
		case ELSE:
			expr, err := p.ParseExpr()
			if err != nil {
				return nil, err
			}
			elseval = expr

			tok, pos, lit := p.scanIgnoreWhitespace()
			if tok != END {
				return nil, newParseError(tokstr(tok, lit), []string{"END"}, pos)
			}
			return &Case{Conds: conds, Vals: vals, ElseVal: elseval}, nil

		case END:
			return &Case{Conds: conds, Vals: vals, ElseVal: elseval}, nil

		default:
			return nil, newParseError(tokstr(tok, lit), []string{"WHEN", "ELSE", "END"}, pos)
		}
	}
}

// parseResample parses a RESAMPLE [EVERY <duration>] [FOR <duration>].
// This function assumes RESAMPLE has already been consumed.
// EVERY and FOR are optional, but at least one of the two has to be used.
func (p *Parser) parseResample() (time.Duration, time.Duration, error) {
	var interval time.Duration
	if p.parseTokenMaybe(EVERY) {
		tok, pos, lit := p.scanIgnoreWhitespace()
		if tok != DURATION_VAL {
			return 0, 0, newParseError(tokstr(tok, lit), []string{"duration"}, pos)
		}

		d, err := ParseDuration(lit)
		if err != nil {
			return 0, 0, &ParseError{Message: err.Error(), Pos: pos}
		}
		interval = d
	}

	var maxDuration time.Duration
	if p.parseTokenMaybe(FOR) {
		tok, pos, lit := p.scanIgnoreWhitespace()
		if tok != DURATION_VAL {
			return 0, 0, newParseError(tokstr(tok, lit), []string{"duration"}, pos)
		}

		d, err := ParseDuration(lit)
		if err != nil {
			return 0, 0, &ParseError{Message: err.Error(), Pos: pos}
		}
		maxDuration = d
	}

	// Neither EVERY or FOR were read, so read the next token again
	// so we can return a suitable error message.
	if interval == 0 && maxDuration == 0 {
		tok, pos, lit := p.scanIgnoreWhitespace()
		return 0, 0, newParseError(tokstr(tok, lit), []string{"EVERY", "FOR"}, pos)
	}
	return interval, maxDuration, nil
}

// scan returns the next token from the underlying scanner.
func (p *Parser) scan() (tok Token, pos Pos, lit string) { return p.s.Scan() }

// scanIgnoreWhitespace scans the next non-whitespace token.
func (p *Parser) scanIgnoreWhitespace() (tok Token, pos Pos, lit string) {
	tok, pos, lit = p.scan()
	if tok == WS {
		tok, pos, lit = p.scan()
	}
	return
}

// consumeWhitespace scans the next token if it's whitespace.
func (p *Parser) consumeWhitespace() {
	if tok, _, _ := p.scan(); tok != WS {
		p.unscan()
	}
}

// unscan pushes the previously read token back onto the buffer.
func (p *Parser) unscan() { p.s.Unscan() }

// ParseDuration parses a time duration from a string.
func ParseDuration(s string) (time.Duration, error) {
	// Return an error if the string is blank or one character
	if len(s) < 2 {
		return 0, ErrInvalidDuration
	}

	// Split string into individual runes.
	a := split(s)

	// Extract the unit of measure.
	// If the last two characters are "ms" then parse as milliseconds.
	// Otherwise just use the last character as the unit of measure.
	var num, uom string
	if len(s) > 2 && s[len(s)-2:] == "ms" {
		num, uom = string(a[:len(a)-2]), "ms"
	} else {
		num, uom = string(a[:len(a)-1]), string(a[len(a)-1:])
	}

	// Parse the numeric part.
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return 0, ErrInvalidDuration
	}

	// Multiply by the unit of measure.
	switch uom {
	case "u", "µ":
		return time.Duration(n) * time.Microsecond, nil
	case "ms":
		return time.Duration(n) * time.Millisecond, nil
	case "s":
		return time.Duration(n) * time.Second, nil
	case "m":
		return time.Duration(n) * time.Minute, nil
	case "h":
		return time.Duration(n) * time.Hour, nil
	case "d":
		return time.Duration(n) * 24 * time.Hour, nil
	case "w":
		return time.Duration(n) * 7 * 24 * time.Hour, nil
	default:
		return 0, ErrInvalidDuration
	}
}

// FormatDuration formats a duration to a string.
func FormatDuration(d time.Duration) string {
	if d == 0 {
		return "0s"
	} else if d%(7*24*time.Hour) == 0 {
		return fmt.Sprintf("%dw", d/(7*24*time.Hour))
	} else if d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", d/(24*time.Hour))
	} else if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", d/time.Hour)
	} else if d%time.Minute == 0 {
		return fmt.Sprintf("%dm", d/time.Minute)
	} else if d%time.Second == 0 {
		return fmt.Sprintf("%ds", d/time.Second)
	} else if d%time.Millisecond == 0 {
		return fmt.Sprintf("%dms", d/time.Millisecond)
	}
	// Although we accept both "u" and "µ" when reading microsecond durations,
	// we output with "u", which can be represented in 1 byte,
	// instead of "µ", which requires 2 bytes.
	return fmt.Sprintf("%du", d/time.Microsecond)
}

// parseTokens consumes an expected sequence of tokens.
func (p *Parser) parseTokens(toks []Token) error {
	for _, expected := range toks {
		if tok, pos, lit := p.scanIgnoreWhitespace(); tok != expected {
			return newParseError(tokstr(tok, lit), []string{tokens[expected]}, pos)
		}
	}
	return nil
}

// parseTokenMaybe consumes the next token if it matches the expected one and
// does nothing if the next token is not the next one.
func (p *Parser) parseTokenMaybe(expected Token) bool {
	tok, _, _ := p.scanIgnoreWhitespace()
	if tok != expected {
		p.unscan()
		return false
	}
	return true
}

// QuoteString returns a quoted string.
func QuoteString(s string) string {
	return fmt.Sprintf("'%s'", quoteStringReplacer.Replace(s))
}

// QuoteIdent returns a quoted identifier from multiple bare identifiers.
func QuoteIdent(segments ...string) string {
	var buf bytes.Buffer
	for i, segment := range segments {
		needQuote := IdentNeedsQuotes(segment) ||
			((i < len(segments)-1) && segment != "") // not last segment && not ""

		if needQuote {
			_ = buf.WriteByte('"')
		}

		_, _ = buf.WriteString(quoteIdentReplacer.Replace(segment))

		if needQuote {
			_ = buf.WriteByte('"')
		}

		if i < len(segments)-1 {
			_ = buf.WriteByte('.')
		}
	}
	return buf.String()
}

// IdentNeedsQuotes returns true if the ident string given would require quotes.
func IdentNeedsQuotes(ident string) bool {
	// check if this identifier is a keyword
	tok := Lookup(ident)
	if tok != IDENT {
		return true
	}
	for i, r := range ident {
		if i == 0 && !isIdentFirstChar(r) {
			return true
		} else if i > 0 && !isIdentChar(r) {
			return true
		}
	}
	return false
}

// split splits a string into a slice of runes.
func split(s string) (a []rune) {
	for _, ch := range s {
		a = append(a, ch)
	}
	return
}

// isDateString returns true if the string looks like a date-only time literal.
func isDateString(s string) bool { return dateStringRegexp.MatchString(s) }

// isDateTimeString returns true if the string looks like a date+time time literal.
func isDateTimeString(s string) bool { return dateTimeStringRegexp.MatchString(s) }

var dateStringRegexp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
var dateTimeStringRegexp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}.+`)

// ErrInvalidDuration is returned when parsing a malformatted duration.
var ErrInvalidDuration = errors.New("invalid duration")

// ParseError represents an error that occurred during parsing.
type ParseError struct {
	Message  string
	Found    string
	Expected []string
	Pos      Pos
}

// newParseError returns a new instance of ParseError.
func newParseError(found string, expected []string, pos Pos) *ParseError {
	return &ParseError{Found: found, Expected: expected, Pos: pos}
}

// Error returns the string representation of the error.
func (e *ParseError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s at line %d, char %d", e.Message, e.Pos.Line+1, e.Pos.Char+1)
	}
	return fmt.Sprintf("found %s, expected %s at line %d, char %d", e.Found, strings.Join(e.Expected, ", "), e.Pos.Line+1, e.Pos.Char+1)
}
