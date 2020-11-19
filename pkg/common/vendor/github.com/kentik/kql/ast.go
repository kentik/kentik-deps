package kql

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DataType represents the primitive data types available in kQL.
type DataType int

const (
	// Unknown primitive data type.
	Unknown DataType = 0
	// Float means the data type is a float
	Float = 1
	// Integer means the data type is a integer
	Integer = 2
	// Boolean means the data type is a boolean.
	Boolean = 3
	// String means the data type is a string of text.
	String = 4
	// Time means the data type is a time.
	Time = 5
	// Duration means the data type is a duration of time.
	Duration = 6
)

// InspectDataType returns the data type of a given value.
func InspectDataType(v interface{}) DataType {
	switch v.(type) {
	case float64:
		return Float
	case int64, int32, int:
		return Integer
	case bool:
		return Boolean
	case string:
		return String
	case time.Time:
		return Time
	case time.Duration:
		return Duration
	default:
		return Unknown
	}
}

func (d DataType) String() string {
	switch d {
	case Float:
		return "float"
	case Integer:
		return "integer"
	case Boolean:
		return "boolean"
	case String:
		return "string"
	case Time:
		return "time"
	case Duration:
		return "duration"
	}
	return "unknown"
}

// Node represents a node in the kQL abstract syntax tree.
type Node interface {
	node()
	String() string
}

func (*Query) node()     {}
func (Statements) node() {}

func (*Distinct) node()        {}
func (*SelectStatement) node() {}

func (*BinaryExpr) node()      {}
func (*BooleanLiteral) node()  {}
func (*Call) node()            {}
func (*DateTimeCall) node()    {}
func (*Case) node()            {}
func (*Dimension) node()       {}
func (Dimensions) node()       {}
func (*DurationLiteral) node() {}
func (*Field) node()           {}
func (Fields) node()           {}
func (*Measurement) node()     {}
func (Measurements) node()     {}
func (*nilLiteral) node()      {}
func (*NegateExpr) node()      {}
func (*NullLiteral) node()     {}
func (*NumberLiteral) node()   {}
func (*IntegerLiteral) node()  {}
func (*ParenExpr) node()       {}
func (*RegexLiteral) node()    {}
func (*SortField) node()       {}
func (SortFields) node()       {}
func (Sources) node()          {}
func (*StringLiteral) node()   {}
func (*SubQuery) node()        {}
func (*Target) node()          {}
func (*TimeLiteral) node()     {}
func (*VarRef) node()          {}
func (*Wildcard) node()        {}

// Query represents a collection of ordered statements.
type Query struct {
	Statements Statements
}

// String returns a string representation of the query.
func (q *Query) String() string { return q.Statements.String() }

// Statements represents a list of statements.
type Statements []Statement

// String returns a string representation of the statements.
func (a Statements) String() string {
	var str []string
	for _, stmt := range a {
		str = append(str, stmt.String())
	}
	return strings.Join(str, ";\n")
}

// Statement represents a single command in kQL.
type Statement interface {
	Node
	stmt()
}

func (*SelectStatement) stmt() {}

// Expr represents an expression that can be evaluated to a value.
type Expr interface {
	Node
	expr()
}

func (*SelectStatement) expr() {}

func (*BinaryExpr) expr()      {}
func (*BooleanLiteral) expr()  {}
func (*Call) expr()            {}
func (*DateTimeCall) expr()    {}
func (*Case) expr()            {}
func (*Distinct) expr()        {}
func (*DurationLiteral) expr() {}
func (*nilLiteral) expr()      {}
func (*NegateExpr) expr()      {}
func (*NullLiteral) expr()     {}
func (*NumberLiteral) expr()   {}
func (*IntegerLiteral) expr()  {}
func (*ParenExpr) expr()       {}
func (*RegexLiteral) expr()    {}
func (*StringLiteral) expr()   {}
func (*TimeLiteral) expr()     {}
func (*VarRef) expr()          {}
func (*Wildcard) expr()        {}

// Source represents a source of data for a statement.
type Source interface {
	Node
	source()
}

func (*Measurement) source() {}
func (*SubQuery) source()    {}

// Sources represents a list of sources.
type Sources []Source

// String returns a string representation of a Sources array.
func (a Sources) String() string {
	var buf bytes.Buffer

	ubound := len(a) - 1
	for i, src := range a {
		_, _ = buf.WriteString(src.String())
		if i < ubound {
			_, _ = buf.WriteString(", ")
		}
	}

	return buf.String()
}

// SortField represents a field to sort results by.
type SortField struct {
	// Name of the field
	Expr Expr

	// Sort order.
	Ascending bool
}

// String returns a string representation of a sort field
func (field *SortField) String() string {
	var buf bytes.Buffer
	if field.Expr != nil {
		_, _ = buf.WriteString(field.Expr.String())
		_, _ = buf.WriteString(" ")
	}
	if field.Ascending {
		_, _ = buf.WriteString("ASC")
	} else {
		_, _ = buf.WriteString("DESC")
	}
	return buf.String()
}

// SortFields represents an ordered list of ORDER BY fields
type SortFields []*SortField

// String returns a string representation of sort fields
func (a SortFields) String() string {
	fields := make([]string, 0, len(a))
	for _, field := range a {
		fields = append(fields, field.String())
	}
	return strings.Join(fields, ", ")
}

type FillOption int

const (
	// NullFill means that empty aggregate windows will just have null values.
	NullFill FillOption = iota
	// NoFill means that empty aggregate windows will be purged from the result.
	NoFill
	// NumberFill means that empty aggregate windows will be filled with the given number
	NumberFill
	// PreviousFill means that empty aggregate windows will be filled with whatever the previous aggregate window had
	PreviousFill
)

// SelectStatement represents a command for extracting data from the database.
type SelectStatement struct {
	// Expressions returned from the selection.
	Fields Fields

	// Target (destination) for the result of the select.
	Target *Target

	// Expressions used for grouping the selection.
	Dimensions Dimensions

	// Data sources that fields are extracted from.
	Sources Sources

	// An expression evaluated on data point.
	Condition Expr

	// An Expression evaluated after grouping data.
	HavingCondition Expr

	// Fields to sort results by
	SortFields SortFields

	// Maximum number of rows to be returned. Unlimited if zero.
	Limit int

	// Returns rows starting at an offset from the first row.
	Offset int

	// memoize the group by interval
	groupByInterval time.Duration

	// if it's a query for raw data values (i.e. not an aggregate)
	IsRawQuery bool

	// What fill option the select statement uses, if any
	Fill FillOption

	// Optional query identifier
	QueryId string

	// The value to fill empty aggregate buckets with, if any
	FillValue interface{}
}

// SourceNames returns a list of source names.
func (s *SelectStatement) SourceNames() []string {
	a := make([]string, 0, len(s.Sources))
	for _, src := range s.Sources {
		switch src := src.(type) {
		case *Measurement:
			a = append(a, src.Name)
		}
	}
	return a
}

// HasDerivative returns true if one of the function calls in the statement is a
// derivative aggregate
func (s *SelectStatement) HasDerivative() bool {
	for _, f := range s.FunctionCalls() {
		if f.Name == "derivative" || f.Name == "non_negative_derivative" {
			return true
		}
	}
	return false
}

// IsSimpleDerivative return true if one of the function call is a derivative function with a
// variable ref as the first arg
func (s *SelectStatement) IsSimpleDerivative() bool {
	for _, f := range s.FunctionCalls() {
		if f.Name == "derivative" || f.Name == "non_negative_derivative" {
			// it's nested if the first argument is an aggregate function
			if _, ok := f.Args[0].(*VarRef); ok {
				return true
			}
		}
	}
	return false
}

// HasSimpleCount return true if one of the function calls is a count function with a
// variable ref as the first arg
func (s *SelectStatement) HasSimpleCount() bool {
	// recursively check for a simple count(varref) function
	var hasCount func(f *Call) bool
	hasCount = func(f *Call) bool {
		if f.Name == "count" {
			// it's nested if the first argument is an aggregate function
			if _, ok := f.Args[0].(*VarRef); ok {
				return true
			}
		} else {
			for _, arg := range f.Args {
				if child, ok := arg.(*Call); ok {
					return hasCount(child)
				}
			}
		}
		return false
	}
	for _, f := range s.FunctionCalls() {
		if hasCount(f) {
			return true
		}
	}
	return false
}

// TimeAscending returns true if the time field is sorted in chronological order.
func (s *SelectStatement) TimeAscending() bool {
	return len(s.SortFields) == 0 || s.SortFields[0].Ascending
}

// Clone returns a deep copy of the statement.
func (s *SelectStatement) Clone() *SelectStatement {
	clone := &SelectStatement{
		Fields:          make(Fields, 0, len(s.Fields)),
		Dimensions:      make(Dimensions, 0, len(s.Dimensions)),
		Sources:         cloneSources(s.Sources),
		SortFields:      make(SortFields, 0, len(s.SortFields)),
		Condition:       CloneExpr(s.Condition),
		HavingCondition: CloneExpr(s.HavingCondition),
		Limit:           s.Limit,
		Offset:          s.Offset,
		Fill:            s.Fill,
		FillValue:       s.FillValue,
		IsRawQuery:      s.IsRawQuery,
	}
	if s.Target != nil {
		clone.Target = &Target{
			Measurement: &Measurement{
				Database:        s.Target.Measurement.Database,
				RetentionPolicy: s.Target.Measurement.RetentionPolicy,
				Name:            s.Target.Measurement.Name,
				Regex:           CloneRegexLiteral(s.Target.Measurement.Regex),
			},
		}
	}
	for _, f := range s.Fields {
		clone.Fields = append(clone.Fields, &Field{Expr: CloneExpr(f.Expr), Alias: f.Alias})
	}
	for _, d := range s.Dimensions {
		clone.Dimensions = append(clone.Dimensions, &Dimension{Expr: CloneExpr(d.Expr)})
	}
	for _, f := range s.SortFields {
		clone.SortFields = append(clone.SortFields, &SortField{Expr: f.Expr, Ascending: f.Ascending})
	}
	return clone
}

func cloneSources(sources Sources) Sources {
	clone := make(Sources, 0, len(sources))
	for _, s := range sources {
		clone = append(clone, cloneSource(s))
	}
	return clone
}

func cloneSource(s Source) Source {
	if s == nil {
		return nil
	}

	switch s := s.(type) {
	case *Measurement:
		m := &Measurement{Database: s.Database, RetentionPolicy: s.RetentionPolicy, Name: s.Name}
		if s.Regex != nil {
			m.Regex = CloneRegexLiteral(s.Regex)
		}
		return m
	default:
		panic("unreachable")
	}
}

// RewriteWildcards returns the re-written form of the select statement. Any wildcard query
// fields are replaced with the supplied fields, and any wildcard GROUP BY fields are replaced
// with the supplied dimensions.
func (s *SelectStatement) RewriteWildcards(fields Fields, dimensions Dimensions) *SelectStatement {
	other := s.Clone()
	selectWildcard, groupWildcard := false, false

	// Rewrite all wildcard query fields
	rwFields := make(Fields, 0, len(s.Fields))
	for _, f := range s.Fields {
		switch f.Expr.(type) {
		case *Wildcard:
			// Sort wildcard fields for consistent output
			sort.Sort(fields)
			rwFields = append(rwFields, fields...)
			selectWildcard = true
		default:
			rwFields = append(rwFields, f)
		}
	}
	other.Fields = rwFields

	// Rewrite all wildcard GROUP BY fields
	rwDimensions := make(Dimensions, 0, len(s.Dimensions))
	for _, d := range s.Dimensions {
		switch d.Expr.(type) {
		case *Wildcard:
			rwDimensions = append(rwDimensions, dimensions...)
			groupWildcard = true
		default:
			rwDimensions = append(rwDimensions, d)
		}
	}

	if selectWildcard && !groupWildcard {
		rwDimensions = append(rwDimensions, dimensions...)
	}
	other.Dimensions = rwDimensions

	return other
}

// RewriteDistinct rewrites the expression to be a call for map/reduce to work correctly
// This method assumes all validation has passed
func (s *SelectStatement) RewriteDistinct() {
	//for i, f := range s.Fields {
	//	if d, ok := f.Expr.(*Distinct); ok {
	//		s.Fields[i].Expr = d.NewCall()
	//		s.IsRawQuery = false
	//	}
	//}
}

// ColumnNames will walk all fields and functions and return the appropriate field names for the select statement
// while maintaining order of the field names
func (s *SelectStatement) ColumnNames() []string {
	// Always set the first column to be time, even if they didn't specify it
	columnNames := []string{"time"}

	// First walk each field
	for _, field := range s.Fields {
		switch f := field.Expr.(type) {
		case *Call:
			if f.Name == "top" || f.Name == "bottom" {
				if len(f.Args) == 2 {
					columnNames = append(columnNames, f.Name)
					continue
				}
				continue
			}
			columnNames = append(columnNames, field.Name())
		default:
			// time is always first, and we already added it, so ignore it if they asked for it anywhere else.
			if field.Name() != "time" {
				columnNames = append(columnNames, field.Name())
			}
		}
	}

	return columnNames
}

// HasTimeFieldSpecified will walk all fields and determine if the user explicitly asked for time
// This is needed to determine re-write behaviors for functions like TOP and BOTTOM
func (s *SelectStatement) HasTimeFieldSpecified() bool {
	for _, f := range s.Fields {
		if f.Name() == "time" {
			return true
		}
	}
	return false
}

// String returns a string representation of the select statement.
func (s *SelectStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SELECT ")
	_, _ = buf.WriteString(s.Fields.String())

	if s.Target != nil {
		_, _ = buf.WriteString(" ")
		_, _ = buf.WriteString(s.Target.String())
	}
	if len(s.Sources) > 0 {
		_, _ = buf.WriteString(" FROM ")
		_, _ = buf.WriteString(s.Sources.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.Dimensions) > 0 {
		_, _ = buf.WriteString(" GROUP BY ")
		_, _ = buf.WriteString(s.Dimensions.String())
	}
	if s.HavingCondition != nil {
		_, _ = buf.WriteString(" HAVING ")
		_, _ = buf.WriteString(s.HavingCondition.String())
	}
	switch s.Fill {
	case NoFill:
		_, _ = buf.WriteString(" fill(none)")
	case NumberFill:
		_, _ = buf.WriteString(fmt.Sprintf(" fill(%v)", s.FillValue))
	case PreviousFill:
		_, _ = buf.WriteString(" fill(previous)")
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(&buf, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	if s.QueryId != "" {
		_, _ = buf.WriteString(" " + s.QueryId)
	}
	return buf.String()
}

// HasWildcard returns whether or not the select statement has at least 1 wildcard
func (s *SelectStatement) HasWildcard() bool {
	return s.HasFieldWildcard() || s.HasDimensionWildcard()
}

// HasFieldWildcard returns whether or not the select statement has at least 1 wildcard in the fields
func (s *SelectStatement) HasFieldWildcard() bool {
	for _, f := range s.Fields {
		_, ok := f.Expr.(*Wildcard)
		if ok {
			return true
		}
	}

	return false
}

// HasDimensionWildcard returns whether or not the select statement has
// at least 1 wildcard in the dimensions aka `GROUP BY`
func (s *SelectStatement) HasDimensionWildcard() bool {
	for _, d := range s.Dimensions {
		_, ok := d.Expr.(*Wildcard)
		if ok {
			return true
		}
	}

	return false
}

func (s *SelectStatement) validate(tr targetRequirement) error {
	if err := s.validateFields(); err != nil {
		return err
	}

	if err := s.validateDimensions(); err != nil {
		return err
	}

	if err := s.validateDistinct(); err != nil {
		return err
	}

	if err := s.validateCountDistinct(); err != nil {
		return err
	}

	if err := s.validateAggregates(tr); err != nil {
		return err
	}

	if err := s.validateDerivative(); err != nil {
		return err
	}

	return nil
}

func (s *SelectStatement) validateFields() error {
	ns := s.NamesInSelect()
	if len(ns) == 1 && ns[0] == "time" {
		return fmt.Errorf("at least 1 non-time field must be queried")
	}
	return nil
}

func (s *SelectStatement) validateDimensions() error {
	var dur time.Duration
	for _, dim := range s.Dimensions {
		switch expr := dim.Expr.(type) {
		case *Call:
			// Ensure the call is time() and it only has one duration argument.
			// If we already have a duration
			if expr.Name != "time" {
				return errors.New("only time() calls allowed in dimensions")
			} else if len(expr.Args) != 1 {
				return errors.New("time dimension expected one argument")
			} else if lit, ok := expr.Args[0].(*DurationLiteral); !ok {
				return errors.New("time dimension must have one duration argument")
			} else if dur != 0 {
				return errors.New("multiple time dimensions not allowed")
			} else {
				dur = lit.Val
			}
		case *DateTimeCall:
			if strings.ToLower(expr.Name) == "time" {
				return errors.New("time() is a function and expects at least one argument")
			}
		case *VarRef:
		case *Wildcard:
		default:
			return errors.New("only time and tag dimensions allowed")
		}
	}
	return nil
}

// validSelectWithAggregate determines if a SELECT statement has the correct
// combination of aggregate functions combined with selected fields and tags
// Currently we don't have support for all aggregates, but aggregates that
// can be combined with fields/tags are:
//  MAX, MIN, SUM, ROUND, COUNT
func (s *SelectStatement) validSelectWithAggregate() error {
	calls := map[string]struct{}{}
	numAggregates := 0
	for _, f := range s.Fields {
		fieldCalls := walkFunctionCalls(f.Expr)
		for _, c := range fieldCalls {
			calls[c.Name] = struct{}{}
		}
		if len(fieldCalls) != 0 {
			numAggregates++
		}
	}
	// For MAX, MIN, SUM, ROUND, COUNT (selector functions) it is ok to ask for fields and tags
	// but only if one function is specified.  Combining multiple functions and fields and tags is not currently supported
	onlySelectors := true
	for k := range calls {
		switch k {
		case "max", "min", "sum", "round", "count":
		default:
			onlySelectors = false
			break
		}
	}
	if onlySelectors {
		// If they only have one selector, they can have as many fields or tags as they want
		if numAggregates == 1 {
			return nil
		}
		// If they have multiple selectors, they are not allowed to have any other fields or tags specified
		//if numAggregates > 1 && len(s.Fields) != numAggregates {
		//	return fmt.Errorf("mixing multiple selector functions with tags or fields is not supported")
		//}
	}

	return nil
}

// validTopBottomAggr determines if TOP or BOTTOM aggregates have valid arguments.
func (s *SelectStatement) validTopBottomAggr(expr *Call) error {
	if exp, got := 2, len(expr.Args); got < exp {
		return fmt.Errorf("invalid number of arguments for %s, expected at least %d, got %d", expr.Name, exp, got)
	}
	if len(expr.Args) > 1 {
		callLimit, ok := expr.Args[len(expr.Args)-1].(*NumberLiteral)
		if !ok {
			return fmt.Errorf("expected integer as last argument in %s(), found %s", expr.Name, expr.Args[len(expr.Args)-1])
		}
		// Check if they asked for a limit smaller than what they passed into the call
		if int64(callLimit.Val) > int64(s.Limit) && s.Limit != 0 {
			return fmt.Errorf("limit (%d) in %s function can not be larger than the LIMIT (%d) in the select statement", int64(callLimit.Val), expr.Name, int64(s.Limit))
		}

		for _, v := range expr.Args[:len(expr.Args)-1] {
			if _, ok := v.(*VarRef); !ok {
				return fmt.Errorf("only fields or tags are allowed in %s(), found %s", expr.Name, v)
			}
		}
	}
	return nil
}

// validPercentileAggr determines if PERCENTILE have valid arguments.
func (s *SelectStatement) validPercentileAggr(expr *Call) error {
	if err := s.validSelectWithAggregate(); err != nil {
		return err
	}
	if exp, got := 2, len(expr.Args); got != exp {
		return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, exp, got)
	}
	_, ok := expr.Args[1].(*NumberLiteral)
	if !ok {
		return fmt.Errorf("expected float argument in percentile()")
	}
	return nil
}

func (s *SelectStatement) validateAggregates(tr targetRequirement) error {
	for _, f := range s.Fields {
		for _, expr := range walkFunctionCalls(f.Expr) {
			switch expr.Name {
			case "derivative", "non_negative_derivative":
				if err := s.validSelectWithAggregate(); err != nil {
					return err
				}
				if min, max, got := 1, 2, len(expr.Args); got > max || got < min {
					return fmt.Errorf("invalid number of arguments for %s, expected at least %d but no more than %d, got %d", expr.Name, min, max, got)
				}
				// Validate that if they have grouping by time, they need a sub-call like min/max, etc.
				groupByInterval, err := s.GroupByInterval()
				if err != nil {
					return fmt.Errorf("invalid group interval: %v", err)
				}
				if groupByInterval > 0 {
					c, ok := expr.Args[0].(*Call)
					if !ok {
						return fmt.Errorf("aggregate function required inside the call to %s", expr.Name)
					}
					switch c.Name {
					case "top", "bottom":
						if err := s.validTopBottomAggr(c); err != nil {
							return err
						}
					case "percentile":
						if err := s.validPercentileAggr(c); err != nil {
							return err
						}
					default:
						if exp, got := 1, len(c.Args); got != exp {
							return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", c.Name, exp, got)
						}
					}
				}
			case "top", "bottom":
				if err := s.validTopBottomAggr(expr); err != nil {
					return err
				}
			case "percentile":
				if err := s.validPercentileAggr(expr); err != nil {
					return err
				}
			case "round":
				if exp, got := 1, len(expr.Args); got != exp {
					return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, exp, got)
				}
			case "now":
				if exp, got := 0, len(expr.Args); got != exp {
					return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, exp, got)
				}
			default:
				if err := s.validSelectWithAggregate(); err != nil {
					return err
				}
				if exp, got := 1, len(expr.Args); got != exp {
					return fmt.Errorf("invalid number of arguments for %s, expected %d, got %d", expr.Name, exp, got)
				}
				switch fc := expr.Args[0].(type) {
				case *VarRef:
					// do nothing
				case *Case:
					// do nothing
				case Expr:
					// do nothing
				case *Wildcard:
					if expr.Name != "count" {
						return fmt.Errorf("wildcard argument not supported in %s()", expr.Name)
					}
				case *Call:
					if fc.Name != "distinct" {
						return fmt.Errorf("expected field argument in %s()", expr.Name)
					}
				case *Distinct:
					if expr.Name != "count" {
						return fmt.Errorf("expected field argument in %s()", expr.Name)
					}
				default:
					return fmt.Errorf("expected field argument in %s()", expr.Name)
				}
			}
		}
	}

	// Check that we have valid duration and where clauses for aggregates

	// fetch the group by duration
	groupByDuration, _ := s.GroupByInterval()

	// If we have a group by interval, but no aggregate function, it's an invalid statement
	if s.IsRawQuery && groupByDuration > 0 {
		return fmt.Errorf("GROUP BY requires at least one aggregate function")
	}

	// If we have an aggregate function with a group by time without a where clause, it's an invalid statement
	if tr == targetNotRequired { // ignore create continuous query statements
		if !s.IsRawQuery && groupByDuration > 0 &&
			!HasTimeExpr(s.Condition) && !HasTimeExpr(s.HavingCondition) {
			return fmt.Errorf("aggregate functions with GROUP BY time require a WHERE time clause")
		}
	}
	return nil
}

func (s *SelectStatement) HasDistinct() bool {
	// determine if we have a call named distinct
	for _, f := range s.Fields {
		switch c := f.Expr.(type) {
		case *Call:
			if c.Name == "distinct" {
				return true
			}
		case *Distinct:
			return true
		}
	}
	return false
}

func (s *SelectStatement) validateDistinct() error {
	if !s.HasDistinct() {
		return nil
	}

	if len(s.Fields) > 1 {
		return fmt.Errorf("aggregate function distinct() can not be combined with other functions or fields")
	}

	switch c := s.Fields[0].Expr.(type) {
	case *Call:
		if len(c.Args) == 0 {
			return fmt.Errorf("distinct function requires at least one argument")
		}

		if len(c.Args) != 1 {
			return fmt.Errorf("distinct function can only have one argument")
		}
	}
	return nil
}

func (s *SelectStatement) HasCountDistinct() bool {
	for _, f := range s.Fields {
		if c, ok := f.Expr.(*Call); ok {
			if c.Name == "count" {
				for _, a := range c.Args {
					if _, ok := a.(*Distinct); ok {
						return true
					}
					if c, ok := a.(*Call); ok {
						if c.Name == "distinct" {
							return true
						}
					}
				}
			}
		}
	}
	return false
}

func (s *SelectStatement) validateCountDistinct() error {
	if !s.HasCountDistinct() {
		return nil
	}

	valid := func(e Expr) bool {
		c, ok := e.(*Call)
		if !ok {
			return true
		}
		if c.Name != "count" {
			return true
		}
		for _, a := range c.Args {
			if _, ok := a.(*Distinct); ok {
				return len(c.Args) == 1
			}
			if d, ok := a.(*Call); ok {
				if d.Name == "distinct" {
					return len(d.Args) == 1
				}
			}
		}
		return true
	}

	for _, f := range s.Fields {
		if !valid(f.Expr) {
			return fmt.Errorf("count(distinct <field>) can only have one argument")
		}
	}

	return nil
}

func (s *SelectStatement) validateDerivative() error {
	if !s.HasDerivative() {
		return nil
	}

	// If a derivative is requested, it must be the only field in the query. We don't support
	// multiple fields in combination w/ derivaties yet.
	if len(s.Fields) != 1 {
		return fmt.Errorf("derivative cannot be used with other fields")
	}

	aggr := s.FunctionCalls()
	if len(aggr) != 1 {
		return fmt.Errorf("derivative cannot be used with other fields")
	}

	// Derivative requires two arguments
	derivativeCall := aggr[0]
	if len(derivativeCall.Args) == 0 {
		return fmt.Errorf("derivative requires a field argument")
	}

	// First arg must be a field or aggr over a field e.g. (mean(field))
	_, callOk := derivativeCall.Args[0].(*Call)
	_, varOk := derivativeCall.Args[0].(*VarRef)

	if !(callOk || varOk) {
		return fmt.Errorf("derivative requires a field argument")
	}

	// If a duration arg is pased, make sure it's a duration
	if len(derivativeCall.Args) == 2 {
		// Second must be a duration .e.g (1h)
		if _, ok := derivativeCall.Args[1].(*DurationLiteral); !ok {
			return fmt.Errorf("derivative requires a duration argument")
		}
	}

	return nil
}

// GroupByIterval extracts the time interval, if specified.
func (s *SelectStatement) GroupByInterval() (time.Duration, error) {
	// return if we've already pulled it out
	if s.groupByInterval != 0 {
		return s.groupByInterval, nil
	}

	// Ignore if there are no dimensions.
	if len(s.Dimensions) == 0 {
		return 0, nil
	}

	for _, d := range s.Dimensions {
		if call, ok := d.Expr.(*Call); ok && call.Name == "time" {
			// Make sure there is exactly one argument.
			if len(call.Args) != 1 {
				return 0, errors.New("time dimension expected one argument")
			}

			// Ensure the argument is a duration.
			lit, ok := call.Args[0].(*DurationLiteral)
			if !ok {
				return 0, errors.New("time dimension must have one duration argument")
			}
			s.groupByInterval = lit.Val
			return lit.Val, nil
		}
	}
	return 0, nil
}

// SetTimeRange sets the start and end time of the select statement to [start, end). i.e. start inclusive, end exclusive.
// This is used commonly for continuous queries so the start and end are in buckets.
func (s *SelectStatement) SetTimeRange(start, end time.Time) error {
	cond := fmt.Sprintf("time >= '%s' AND time < '%s'", start.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano))
	if s.Condition != nil {
		cond = fmt.Sprintf("%s AND %s", s.rewriteWithoutTimeDimensions(), cond)
	}

	expr, err := NewParser(strings.NewReader(cond)).ParseExpr()
	if err != nil {
		return err
	}

	// fold out any previously replaced time dimensios and set the condition
	s.Condition = Reduce(expr, nil)

	return nil
}

// rewriteWithoutTimeDimensions will remove any WHERE time... clauses from the select statement
// This is necessary when setting an explicit time range to override any that previously existed.
func (s *SelectStatement) rewriteWithoutTimeDimensions() string {

	n := RewriteFunc(s.Condition, func(n Node) Node {
		switch n := n.(type) {
		case *BinaryExpr:
			if n.LHS.String() == "time" {
				return &BooleanLiteral{Val: true}
			}
			return n
		case *Call:
			return &BooleanLiteral{Val: true}
		default:
			return n
		}
	})

	return n.String()
}

/*

BinaryExpr

SELECT mean(xxx.value) + avg(yyy.value) FROM xxx JOIN yyy WHERE xxx.host = 123

from xxx where host = 123
select avg(value) from yyy where host = 123

SELECT xxx.value FROM xxx WHERE xxx.host = 123
SELECT yyy.value FROM yyy

---

SELECT MEAN(xxx.value) + MEAN(cpu.load.value)
FROM xxx JOIN yyy
GROUP BY host
WHERE (xxx.region == "uswest" OR yyy.region == "uswest") AND xxx.otherfield == "XXX"

select * from (
	select mean + mean from xxx join yyy
	group by time(5m), host
) (xxx.region == "uswest" OR yyy.region == "uswest") AND xxx.otherfield == "XXX"

(seriesIDS for xxx.region = 'uswest' union seriesIDs for yyy.regnion = 'uswest') | seriesIDS xxx.otherfield = 'XXX'

WHERE xxx.region == "uswest" AND xxx.otherfield == "XXX"
WHERE yyy.region == "uswest"


*/

// Substatement returns a single-series statement for a given variable reference.
func (s *SelectStatement) Substatement(ref *VarRef) (*SelectStatement, error) {
	// Copy dimensions and properties to new statement.
	other := &SelectStatement{
		Fields:     Fields{{Expr: ref}},
		Dimensions: s.Dimensions,
		Limit:      s.Limit,
		Offset:     s.Offset,
		SortFields: s.SortFields,
	}

	// If there is only one series source then return it with the whole condition.
	if len(s.Sources) == 1 {
		other.Sources = s.Sources
		other.Condition = s.Condition
		other.HavingCondition = s.HavingCondition
		return other, nil
	}

	// Find the matching source.
	name := MatchSource(s.Sources, ref.Val)
	if name == "" {
		return nil, fmt.Errorf("field source not found: %s", ref.Val)
	}
	other.Sources = append(other.Sources, &Measurement{Name: name})

	// Filter out conditions.
	if s.Condition != nil {
		other.Condition = filterExprBySource(name, s.Condition)
	}

	// Filter out having conditions.
	if s.HavingCondition != nil {
		other.HavingCondition = filterExprBySource(name, s.HavingCondition)
	}

	return other, nil
}

// NamesInWhere returns the field and tag names (idents) referenced in the where clause
func (s *SelectStatement) NamesInWhere() []string {
	var a []string
	if s.Condition != nil {
		a = walkNames(s.Condition)
	}
	return a
}

// NamesInSelect returns the field and tag names (idents) in the select clause
func (s *SelectStatement) NamesInSelect() []string {
	var a []string

	for _, f := range s.Fields {
		a = append(a, walkNames(f.Expr)...)
	}

	return a
}

// NamesInDimension returns the field and tag names (idents) in the group by
func (s *SelectStatement) NamesInDimension() []string {
	var a []string

	for _, d := range s.Dimensions {
		a = append(a, walkNames(d.Expr)...)
	}

	return a
}

// walkNames will walk the Expr and return the database fields
func walkNames(exp Expr) []string {
	switch expr := exp.(type) {
	case *VarRef:
		return []string{expr.Val}
	case *Call:
		if len(expr.Args) == 0 {
			return nil
		}
		lit, ok := expr.Args[0].(*VarRef)
		if !ok {
			return nil
		}

		return []string{lit.Val}
	case *DateTimeCall:
		return []string{expr.Name}
	case *BinaryExpr:
		var ret []string
		ret = append(ret, walkNames(expr.LHS)...)
		ret = append(ret, walkNames(expr.RHS)...)
		return ret
	case *ParenExpr:
		return walkNames(expr.Expr)
	}

	return nil
}

// FunctionCalls returns the Call objects from the query
func (s *SelectStatement) FunctionCalls() []*Call {
	var a []*Call
	for _, f := range s.Fields {
		a = append(a, walkFunctionCalls(f.Expr)...)
	}
	return a
}

// FunctionCallsByPosition returns the Call objects from the query in the order they appear in the select statement
func (s *SelectStatement) FunctionCallsByPosition() [][]*Call {
	var a [][]*Call
	for _, f := range s.Fields {
		a = append(a, walkFunctionCalls(f.Expr))
	}
	return a
}

// walkFunctionCalls walks the Field of a query for any function calls made
func walkFunctionCalls(exp Expr) []*Call {
	switch expr := exp.(type) {
	case *VarRef:
		return nil
	case *Call:
		return []*Call{expr}
	case *BinaryExpr:
		var ret []*Call
		ret = append(ret, walkFunctionCalls(expr.LHS)...)
		ret = append(ret, walkFunctionCalls(expr.RHS)...)
		return ret
	case *ParenExpr:
		return walkFunctionCalls(expr.Expr)
	}

	return nil
}

// filters an expression to exclude expressions unrelated to a source.
func filterExprBySource(name string, expr Expr) Expr {
	switch expr := expr.(type) {
	case *VarRef:
		if !strings.HasPrefix(expr.Val, name) {
			return nil
		}

	case *BinaryExpr:
		lhs := filterExprBySource(name, expr.LHS)
		rhs := filterExprBySource(name, expr.RHS)

		// If an expr is logical then return either LHS/RHS or both.
		// If an expr is arithmetic or comparative then require both sides.
		if expr.Op == AND || expr.Op == OR {
			if lhs == nil && rhs == nil {
				return nil
			} else if lhs != nil && rhs == nil {
				return lhs
			} else if lhs == nil && rhs != nil {
				return rhs
			}
		} else {
			if lhs == nil || rhs == nil {
				return nil
			}
		}
		return &BinaryExpr{Op: expr.Op, LHS: lhs, RHS: rhs}

	case *ParenExpr:
		exp := filterExprBySource(name, expr.Expr)
		if exp == nil {
			return nil
		}
		return &ParenExpr{Expr: exp}
	}
	return expr
}

// MatchSource returns the source name that matches a field name.
// Returns a blank string if no sources match.
func MatchSource(sources Sources, name string) string {
	for _, src := range sources {
		switch src := src.(type) {
		case *Measurement:
			if strings.HasPrefix(name, src.Name) {
				return src.Name
			}
		}
	}
	return ""
}

// Target represents a target (destination) policy, measurement, and DB.
type Target struct {
	// Measurement to write into.
	Measurement *Measurement
}

// String returns a string representation of the Target.
func (t *Target) String() string {
	if t == nil {
		return ""
	}

	var buf bytes.Buffer
	_, _ = buf.WriteString("INTO ")
	_, _ = buf.WriteString(t.Measurement.String())
	if t.Measurement.Name == "" {
		_, _ = buf.WriteString(":MEASUREMENT")
	}

	return buf.String()
}

// Fields represents a list of fields.
type Fields []*Field

// AliasNames returns a list of calculated field names in
// order of alias, function name, then field.
func (a Fields) AliasNames() []string {
	names := []string{}
	for _, f := range a {
		names = append(names, f.Name())
	}
	return names
}

// Names returns a list of field names.
func (a Fields) Names() []string {
	names := []string{}
	for _, f := range a {
		switch expr := f.Expr.(type) {
		case *Call:
			names = append(names, expr.Name)
		case *VarRef:
			names = append(names, expr.Val)
		case *BinaryExpr:
			names = append(names, walkNames(expr)...)
		case *ParenExpr:
			names = append(names, walkNames(expr)...)
		}
	}
	return names
}

// String returns a string representation of the fields.
func (a Fields) String() string {
	var str []string
	for _, f := range a {
		str = append(str, f.String())
	}
	return strings.Join(str, ", ")
}

// Field represents an expression retrieved from a select statement.
type Field struct {
	Expr  Expr
	Alias string
}

// Name returns the name of the field. Returns alias, if set.
// Otherwise uses the function name or variable name.
func (f *Field) Name() string {
	// Return alias, if set.
	if f.Alias != "" {
		return f.Alias
	}

	// Return the function name or variable name, if available.
	switch expr := f.Expr.(type) {
	case *Call:
		return expr.Name
	case *VarRef:
		return expr.Val
	}

	// Otherwise return a blank name.
	return ""
}

// String returns a string representation of the field.
func (f *Field) String() string {
	str := f.Expr.String()

	if f.Alias == "" {
		return str
	}
	return fmt.Sprintf("%s AS %s", str, QuoteIdent(f.Alias))
}

// Sort Interface for Fields
func (f Fields) Len() int           { return len(f) }
func (f Fields) Less(i, j int) bool { return f[i].Name() < f[j].Name() }
func (f Fields) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }

// Dimensions represents a list of dimensions.
type Dimensions []*Dimension

// String returns a string representation of the dimensions.
func (a Dimensions) String() string {
	var str []string
	for _, d := range a {
		str = append(str, d.String())
	}
	return strings.Join(str, ", ")
}

// Normalize returns the interval and tag dimensions separately.
// Returns 0 if no time interval is specified.
func (a Dimensions) Normalize() (time.Duration, []string) {
	var dur time.Duration
	var tags []string

	for _, dim := range a {
		switch expr := dim.Expr.(type) {
		case *Call:
			lit, _ := expr.Args[0].(*DurationLiteral)
			dur = lit.Val
		case *VarRef:
			tags = append(tags, expr.Val)
		}
	}

	return dur, tags
}

// Dimension represents an expression that a select statement is grouped by.
type Dimension struct {
	Expr Expr
}

// String returns a string representation of the dimension.
func (d *Dimension) String() string { return d.Expr.String() }

// Measurements represents a list of measurements.
type Measurements []*Measurement

// String returns a string representation of the measurements.
func (a Measurements) String() string {
	var str []string
	for _, m := range a {
		str = append(str, m.String())
	}
	return strings.Join(str, ", ")
}

// Measurement represents a single measurement used as a datasource.
type Measurement struct {
	Database        string
	RetentionPolicy string
	Name            string
	Regex           *RegexLiteral
	Alias           string
	IsTarget        bool
}

// String returns a string representation of the measurement.
func (m *Measurement) String() string {
	var buf bytes.Buffer
	if m.Database != "" {
		_, _ = buf.WriteString(QuoteIdent(m.Database))
		_, _ = buf.WriteString(".")
	}

	if m.RetentionPolicy != "" {
		_, _ = buf.WriteString(QuoteIdent(m.RetentionPolicy))
	}

	if m.Database != "" || m.RetentionPolicy != "" {
		_, _ = buf.WriteString(`.`)
	}

	if m.Name != "" {
		_, _ = buf.WriteString(QuoteIdent(m.Name))
	} else if m.Regex != nil {
		_, _ = buf.WriteString(m.Regex.String())
	}

	if m.Alias != "" {
		_, _ = buf.WriteString(" AS " + m.Alias)
	}

	return buf.String()
}

// SubQuery represents a subquery.
type SubQuery struct {
	Query Statement
	Alias string
}

// String returns a string representation of the subquery.
func (s *SubQuery) String() string {
	var buf bytes.Buffer

	if t, ok := s.Query.(*SelectStatement); ok {
		_, _ = buf.WriteString(t.String())
	}

	if s.Alias != "" {
		_, _ = buf.WriteString(" AS " + s.Alias)
	}
	return buf.String()
}

// VarRef represents a reference to a variable.
type VarRef struct {
	Val string
}

// String returns a string representation of the variable reference.
func (r *VarRef) String() string {
	return QuoteIdent(r.Val)
}

// Call represents a function call.
type Call struct {
	Name string
	Args []Expr
}

// String returns a string representation of the call.
func (c *Call) String() string {
	// Join arguments.
	var str []string
	for _, arg := range c.Args {
		str = append(str, arg.String())
	}

	// Write function name and args.
	return fmt.Sprintf("%s(%s)", c.Name, strings.Join(str, ","))
}

// Fields will extract any field names from the call.  Only specific calls support this.
func (c *Call) Fields() []string {
	switch c.Name {
	case "top", "bottom":
		// maintain the order the user specified in the query
		keyMap := make(map[string]struct{})
		keys := []string{}
		for i, a := range c.Args {
			if i == 0 {
				// special case, first argument is always the name of the function regardless of the field name
				keys = append(keys, c.Name)
				continue
			}
			switch v := a.(type) {
			case *VarRef:
				if _, ok := keyMap[v.Val]; !ok {
					keyMap[v.Val] = struct{}{}
					keys = append(keys, v.Val)
				}
			}
		}
		return keys
	case "min", "max", "first", "last", "sum", "mean":
		// maintain the order the user specified in the query
		keyMap := make(map[string]struct{})
		keys := []string{}
		for _, a := range c.Args {
			switch v := a.(type) {
			case *VarRef:
				if _, ok := keyMap[v.Val]; !ok {
					keyMap[v.Val] = struct{}{}
					keys = append(keys, v.Val)
				}
			}
		}
		return keys
	default:
		panic(fmt.Sprintf("*call.Fields is unable to provide information on %s", c.Name))
	}
}

// DateTimeCall represents a date/time function call.
type DateTimeCall struct {
	Name string
	Arg  string
}

// String returns a string representation of the DateTimeCall.
func (c *DateTimeCall) String() string {
	s := fmt.Sprintf("%s", c.Name)
	if c.Arg != "" {
		return s + " " + c.Name
	}
	return s
}

// Case represents a CASE conditional.
type Case struct {
	Conds   []Expr
	Vals    []Expr
	ElseVal Expr
}

func (c *Case) String() string {
	var str []string

	for i := range c.Conds {
		str = append(str, "WHEN", c.Conds[i].String(), "THEN", c.Vals[i].String())
	}

	if c.ElseVal != nil {
		str = append(str, "ELSE", c.ElseVal.String())
	}
	return fmt.Sprintf("CASE %s END", strings.Join(str, " "))
}

// Distinct represents a DISTINCT expression.
type Distinct struct {
	// Identifier following DISTINCT
	Val string
}

// String returns a string representation of the expression.
func (d *Distinct) String() string {
	return fmt.Sprintf("DISTINCT %s", d.Val)
}

// NewCall returns a new call expression from this expressions.
func (d *Distinct) NewCall() *Call {
	return &Call{
		Name: "distinct",
		Args: []Expr{
			&VarRef{Val: d.Val},
		},
	}
}

// NumberLiteral represents a numeric literal.
type NumberLiteral struct {
	Val float64
}

// String returns a string representation of the literal.
func (l *NumberLiteral) String() string { return strconv.FormatFloat(l.Val, 'f', -1, 64) }

// IntegerLiteral represents a integer literal.
type IntegerLiteral struct {
	Val int64
}

// String returns a string representation of the literal.
func (l *IntegerLiteral) String() string { return strconv.FormatInt(l.Val, 10) }

// BooleanLiteral represents a boolean literal.
type BooleanLiteral struct {
	Val bool
}

// String returns a string representation of the literal.
func (l *BooleanLiteral) String() string {
	if l.Val {
		return "true"
	}
	return "false"
}

// isTrueLiteral returns true if the expression is a literal "true" value.
func isTrueLiteral(expr Expr) bool {
	if expr, ok := expr.(*BooleanLiteral); ok {
		return expr.Val == true
	}
	return false
}

// isFalseLiteral returns true if the expression is a literal "false" value.
func isFalseLiteral(expr Expr) bool {
	if expr, ok := expr.(*BooleanLiteral); ok {
		return expr.Val == false
	}
	return false
}

// NullLiteral represents NULL
type NullLiteral struct{}

// String returns a string representation of the literal.
func (l *NullLiteral) String() string {
	return "NULL"
}

// StringLiteral represents a string literal.
type StringLiteral struct {
	Val     string
	Unquote bool
}

// String returns a string representation of the literal.
func (l *StringLiteral) String() string {
	if l.Unquote == true {
		return l.Val
	}
	return QuoteString(l.Val)
}

// TimeLiteral represents a point-in-time literal.
type TimeLiteral struct {
	Val time.Time
}

// String returns a string representation of the literal.
func (l *TimeLiteral) String() string {
	return `'` + l.Val.UTC().Format(time.RFC3339Nano) + `'`
}

// DurationLiteral represents a duration literal.
type DurationLiteral struct {
	Val time.Duration
}

// String returns a string representation of the literal.
func (l *DurationLiteral) String() string { return FormatDuration(l.Val) }

// nilLiteral represents a nil literal.
// This is not available to the query language itself. It's only used internally.
type nilLiteral struct{}

// String returns a string representation of the literal.
func (l *nilLiteral) String() string { return `nil` }

// BinaryExpr represents an operation between two expressions.
type BinaryExpr struct {
	Op   Token
	OpEx Token
	LHS  Expr
	RHS  Expr
}

// String returns a string representation of the binary expression.
func (e *BinaryExpr) String() string {
	if e.OpEx == ILLEGAL {
		return fmt.Sprintf("%s %s %s", e.LHS.String(), e.Op.String(), e.RHS.String())
	} else {
		return fmt.Sprintf("%s %s %s %s", e.LHS.String(), e.Op.String(), e.OpEx.String(), e.RHS.String())
	}
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expr
}

// String returns a string representation of the parenthesized expression.
func (e *ParenExpr) String() string { return fmt.Sprintf("(%s)", e.Expr.String()) }

// RegexLiteral represents a regular expression.
type RegexLiteral struct {
	Val *regexp.Regexp
}

// String returns a string representation of the literal.
func (r *RegexLiteral) String() string {
	if r.Val != nil {
		return fmt.Sprintf("'%s'", strings.Replace(r.Val.String(), `/`, `\/`, -1))
	}
	return ""
}

// CloneRegexLiteral returns a clone of the RegexLiteral.
func CloneRegexLiteral(r *RegexLiteral) *RegexLiteral {
	if r == nil {
		return nil
	}

	clone := &RegexLiteral{}
	if r.Val != nil {
		clone.Val = regexp.MustCompile(r.Val.String())
	}

	return clone
}

// Wildcard represents a wild card expression.
type Wildcard struct{}

// String returns a string representation of the wildcard.
func (e *Wildcard) String() string { return "*" }

// Negate unary expression
type NegateExpr struct {
	Expr Expr
}

// String returns a string representation of the unary negate expression.
func (e *NegateExpr) String() string { return "NOT " + e.Expr.String() }

// CloneExpr returns a deep copy of the expression.
func CloneExpr(expr Expr) Expr {
	if expr == nil {
		return nil
	}
	switch expr := expr.(type) {
	case *BinaryExpr:
		return &BinaryExpr{Op: expr.Op, LHS: CloneExpr(expr.LHS), RHS: CloneExpr(expr.RHS)}
	case *BooleanLiteral:
		return &BooleanLiteral{Val: expr.Val}
	case *Call:
		args := make([]Expr, len(expr.Args))
		for i, arg := range expr.Args {
			args[i] = CloneExpr(arg)
		}
		return &Call{Name: expr.Name, Args: args}
	case *DateTimeCall:
		return &DateTimeCall{Name: expr.Name, Arg: expr.Arg}
	case *Case:
		var conds []Expr
		var vals []Expr
		var elseval Expr

		for _, e := range expr.Conds {
			conds = append(conds, CloneExpr(e))
		}
		for _, e := range expr.Vals {
			conds = append(conds, CloneExpr(e))
		}
		elseval = CloneExpr(expr.ElseVal)
		return &Case{Conds: conds, Vals: vals, ElseVal: elseval}
	case *Distinct:
		return &Distinct{Val: expr.Val}
	case *DurationLiteral:
		return &DurationLiteral{Val: expr.Val}
	case *NumberLiteral:
		return &NumberLiteral{Val: expr.Val}
	case *IntegerLiteral:
		return &IntegerLiteral{Val: expr.Val}
	case *ParenExpr:
		return &ParenExpr{Expr: CloneExpr(expr.Expr)}
	case *RegexLiteral:
		return CloneRegexLiteral(expr)
	case *StringLiteral:
		return &StringLiteral{Val: expr.Val}
	case *TimeLiteral:
		return &TimeLiteral{Val: expr.Val}
	case *VarRef:
		return &VarRef{Val: expr.Val}
	case *Wildcard:
		return &Wildcard{}
	}
	panic("unreachable")
}

// HasTimeExpr returns true if the expression has a time term.
func HasTimeExpr(expr Expr) bool {
	switch n := expr.(type) {
	case *BinaryExpr:
		if n.Op == AND || n.Op == OR {
			return HasTimeExpr(n.LHS) || HasTimeExpr(n.RHS)
		}
		if ref, ok := n.LHS.(*DateTimeCall); ok && ref.Name == "time" {
			return true
		}
		return false
	case *ParenExpr:
		// walk down the tree
		return HasTimeExpr(n.Expr)
	default:
		return false
	}
}

// OnlyTimeExpr returns true if the expression only has time constraints.
func OnlyTimeExpr(expr Expr) bool {
	if expr == nil {
		return false
	}
	switch n := expr.(type) {
	case *BinaryExpr:
		if n.Op == AND || n.Op == OR {
			return OnlyTimeExpr(n.LHS) && OnlyTimeExpr(n.RHS)
		}
		if ref, ok := n.LHS.(*DateTimeCall); ok && strings.ToLower(ref.Name) == "time" {
			return true
		}
		return false
	case *ParenExpr:
		// walk down the tree
		return OnlyTimeExpr(n.Expr)
	default:
		return false
	}
}

// TimeRange returns the minimum and maximum times specified by an expression.
// Returns zero times if there is no bound.
func TimeRange(expr Expr) (min, max time.Time) {
	WalkFunc(expr, func(n Node) {
		if n, ok := n.(*BinaryExpr); ok {
			// Extract literal expression & operator on LHS.
			// Check for "time" on the left-hand side first.
			// Otherwise check for for the right-hand side and flip the operator.
			value, op := timeExprValue(n.LHS, n.RHS), n.Op
			if value.IsZero() {
				if value = timeExprValue(n.RHS, n.LHS); value.IsZero() {
					return
				} else if op == LT {
					op = GT
				} else if op == LTE {
					op = GTE
				} else if op == GT {
					op = LT
				} else if op == GTE {
					op = LTE
				}
			}

			// Update the min/max depending on the operator.
			// The GT & LT update the value by +/- 1ns not make them "not equal".
			switch op {
			case GT:
				if min.IsZero() || value.After(min) {
					min = value.Add(time.Nanosecond)
				}
			case GTE:
				if min.IsZero() || value.After(min) {
					min = value
				}
			case LT:
				if max.IsZero() || value.Before(max) {
					max = value.Add(-time.Nanosecond)
				}
			case LTE:
				if max.IsZero() || value.Before(max) {
					max = value
				}
			case EQ:
				if min.IsZero() || value.After(min) {
					min = value
				}
				if max.IsZero() || value.Before(max) {
					max = value
				}
			}
		}
	})
	return
}

// TimeRange returns the minimum and maximum times, as epoch nano, specified by
// and expression. If there is no lower bound, the start of the epoch is returned
// for minimum. If there is no higher bound, now is returned for maximum.
func TimeRangeAsEpochNano(expr Expr) (min, max int64) {
	tmin, tmax := TimeRange(expr)
	if tmin.IsZero() {
		min = time.Unix(0, 0).UnixNano()
	} else {
		min = tmin.UnixNano()
	}
	if tmax.IsZero() {
		max = time.Now().UnixNano()
	} else {
		max = tmax.UnixNano()
	}
	return
}

// timeExprValue returns the time literal value of a "time == <TimeLiteral>" expression.
// Returns zero time if the expression is not a time expression.
func timeExprValue(ref Expr, lit Expr) time.Time {
	if ref, ok := ref.(*DateTimeCall); ok && strings.ToLower(ref.Name) == "time" {
		switch lit := lit.(type) {
		case *TimeLiteral:
			return lit.Val
		case *DurationLiteral:
			return time.Unix(0, int64(lit.Val)).UTC()
		case *NumberLiteral:
			return time.Unix(0, int64(lit.Val)).UTC()
		case *IntegerLiteral:
			return time.Unix(0, int64(lit.Val)).UTC()
		}
	}
	return time.Time{}
}

// Visitor can be called by Walk to traverse an AST hierarchy.
// The Visit() function is called once per node.
type Visitor interface {
	Visit(Node) Visitor
}

// Walk traverses a node hierarchy in depth-first order.
func Walk(v Visitor, node Node) {
	if node == nil {
		return
	}

	if v = v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {
	case *BinaryExpr:
		Walk(v, n.LHS)
		Walk(v, n.RHS)

	case *Call:
		for _, expr := range n.Args {
			Walk(v, expr)
		}

	case *Case:
		// FIXME add case conditional to field validations
		//for i, expr := range n.Conds {
		//	Walk(v, expr)
		//	Walk(v, n.Vals[i])
		//}
		//Walk(v, n.ElseVal)

	case *Dimension:
		Walk(v, n.Expr)

	case Dimensions:
		for _, c := range n {
			Walk(v, c)
		}

	case *Field:
		Walk(v, n.Expr)

	case Fields:
		for _, c := range n {
			Walk(v, c)
		}

	case *ParenExpr:
		Walk(v, n.Expr)

	case *Query:
		Walk(v, n.Statements)

	case *SelectStatement:
		Walk(v, n.Fields)
		Walk(v, n.Target)
		Walk(v, n.Dimensions)
		Walk(v, n.Sources)
		Walk(v, n.Condition)
		Walk(v, n.HavingCondition)
		Walk(v, n.SortFields)

	case SortFields:
		for _, sf := range n {
			Walk(v, sf)
		}

	case Sources:
		for _, s := range n {
			Walk(v, s)
		}

	case Statements:
		for _, s := range n {
			Walk(v, s)
		}

	case *Target:
		if n != nil {
			Walk(v, n.Measurement)
		}
	}
}

// WalkFunc traverses a node hierarchy in depth-first order.
func WalkFunc(node Node, fn func(Node)) {
	Walk(walkFuncVisitor(fn), node)
}

type walkFuncVisitor func(Node)

func (fn walkFuncVisitor) Visit(n Node) Visitor { fn(n); return fn }

// Rewriter can be called by Rewrite to replace nodes in the AST hierarchy.
// The Rewrite() function is called once per node.
type Rewriter interface {
	Rewrite(Node) Node
}

// Rewrite recursively invokes the rewriter to replace each node.
// Nodes are traversed depth-first and rewritten from leaf to root.
func Rewrite(r Rewriter, node Node) Node {
	switch n := node.(type) {
	case *Query:
		n.Statements = Rewrite(r, n.Statements).(Statements)

	case Statements:
		for i, s := range n {
			n[i] = Rewrite(r, s).(Statement)
		}

	case *SelectStatement:
		n.Fields = Rewrite(r, n.Fields).(Fields)
		n.Dimensions = Rewrite(r, n.Dimensions).(Dimensions)
		n.Sources = Rewrite(r, n.Sources).(Sources)
		n.Condition = Rewrite(r, n.Condition).(Expr)
		n.HavingCondition = Rewrite(r, n.HavingCondition).(Expr)

	case Fields:
		for i, f := range n {
			n[i] = Rewrite(r, f).(*Field)
		}

	case *Field:
		n.Expr = Rewrite(r, n.Expr).(Expr)

	case Dimensions:
		for i, d := range n {
			n[i] = Rewrite(r, d).(*Dimension)
		}

	case *Dimension:
		n.Expr = Rewrite(r, n.Expr).(Expr)

	case *BinaryExpr:
		n.LHS = Rewrite(r, n.LHS).(Expr)
		n.RHS = Rewrite(r, n.RHS).(Expr)

	case *ParenExpr:
		n.Expr = Rewrite(r, n.Expr).(Expr)

	case *Call:
		for i, expr := range n.Args {
			n.Args[i] = Rewrite(r, expr).(Expr)
		}
	}

	return r.Rewrite(node)
}

// RewriteFunc rewrites a node hierarchy.
func RewriteFunc(node Node, fn func(Node) Node) Node {
	return Rewrite(rewriterFunc(fn), node)
}

type rewriterFunc func(Node) Node

func (fn rewriterFunc) Rewrite(n Node) Node { return fn(n) }

// Eval evaluates expr against a map.
func Eval(expr Expr, m map[string]interface{}) interface{} {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *BinaryExpr:
		return evalBinaryExpr(expr, m)
	case *BooleanLiteral:
		return expr.Val
	case *NumberLiteral:
		return expr.Val
	case *IntegerLiteral:
		return expr.Val
	case *ParenExpr:
		return Eval(expr.Expr, m)
	case *StringLiteral:
		return expr.Val
	case *VarRef:
		return m[expr.Val]
	default:
		return nil
	}
}

func evalBinaryExpr(expr *BinaryExpr, m map[string]interface{}) interface{} {
	lhs := Eval(expr.LHS, m)
	rhs := Eval(expr.RHS, m)

	// Evaluate if both sides are simple types.
	switch lhs := lhs.(type) {
	case bool:
		rhs, _ := rhs.(bool)
		switch expr.Op {
		case AND:
			return lhs && rhs
		case OR:
			return lhs || rhs
		case EQ:
			return lhs == rhs
		case NEQ:
			return lhs != rhs
		}
	case float64:
		rhs, _ := rhs.(float64)
		switch expr.Op {
		case EQ:
			return lhs == rhs
		case NEQ:
			return lhs != rhs
		case LT:
			return lhs < rhs
		case LTE:
			return lhs <= rhs
		case GT:
			return lhs > rhs
		case GTE:
			return lhs >= rhs
		case ADD:
			return lhs + rhs
		case SUB:
			return lhs - rhs
		case MUL:
			return lhs * rhs
		case DIV:
			if rhs == 0 {
				return float64(0)
			}
			return lhs / rhs
		}
	case int64:
		// we parse all number literals as float 64, so we have to convert from
		// an interface to the float64, then cast to an int64 for comparison
		rhsf, _ := rhs.(float64)
		rhs := int64(rhsf)
		switch expr.Op {
		case EQ:
			return lhs == rhs
		case NEQ:
			return lhs != rhs
		case LT:
			return lhs < rhs
		case LTE:
			return lhs <= rhs
		case GT:
			return lhs > rhs
		case GTE:
			return lhs >= rhs
		case ADD:
			return lhs + rhs
		case SUB:
			return lhs - rhs
		case MUL:
			return lhs * rhs
		case DIV:
			if rhs == 0 {
				return int64(0)
			}
			return lhs / rhs
		}
	case string:
		rhs, _ := rhs.(string)
		switch expr.Op {
		case EQ:
			return lhs == rhs
		case NEQ:
			return lhs != rhs
		}
	}
	return nil
}

// EvalBool evaluates expr and returns true if result is a boolean true.
// Otherwise returns false.
func EvalBool(expr Expr, m map[string]interface{}) bool {
	v, _ := Eval(expr, m).(bool)
	return v
}

// Reduce evaluates expr using the available values in valuer.
// References that don't exist in valuer are ignored.
func Reduce(expr Expr, valuer Valuer) Expr {
	expr = reduce(expr, valuer)

	// Unwrap parens at top level.
	if expr, ok := expr.(*ParenExpr); ok {
		return expr.Expr
	}
	return expr
}

func reduce(expr Expr, valuer Valuer) Expr {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *BinaryExpr:
		return reduceBinaryExpr(expr, valuer)
	case *Call:
		return reduceCall(expr, valuer)
	case *ParenExpr:
		return reduceParenExpr(expr, valuer)
	case *VarRef:
		return reduceVarRef(expr, valuer)
	default:
		return CloneExpr(expr)
	}
}

func reduceBinaryExpr(expr *BinaryExpr, valuer Valuer) Expr {
	// Reduce both sides first.
	op := expr.Op
	lhs := reduce(expr.LHS, valuer)
	rhs := reduce(expr.RHS, valuer)

	// Do not evaluate if one side is nil.
	if lhs == nil || rhs == nil {
		return &BinaryExpr{LHS: lhs, RHS: rhs, Op: expr.Op}
	}

	// If we have a logical operator (AND, OR) and one side is a boolean literal
	// then we need to have special handling.
	if op == AND {
		if isFalseLiteral(lhs) || isFalseLiteral(rhs) {
			return &BooleanLiteral{Val: false}
		} else if isTrueLiteral(lhs) {
			return rhs
		} else if isTrueLiteral(rhs) {
			return lhs
		}
	} else if op == OR {
		if isTrueLiteral(lhs) || isTrueLiteral(rhs) {
			return &BooleanLiteral{Val: true}
		} else if isFalseLiteral(lhs) {
			return rhs
		} else if isFalseLiteral(rhs) {
			return lhs
		}
	}

	// Evaluate if both sides are simple types.
	switch lhs := lhs.(type) {
	case *BooleanLiteral:
		return reduceBinaryExprBooleanLHS(op, lhs, rhs)
	case *DurationLiteral:
		return reduceBinaryExprDurationLHS(op, lhs, rhs)
	case *nilLiteral:
		return reduceBinaryExprNilLHS(op, lhs, rhs)
	case *NumberLiteral:
		return reduceBinaryExprNumberLHS(op, lhs, rhs)
	case *IntegerLiteral:
		return reduceBinaryExprIntegerLHS(op, lhs, rhs)
	case *StringLiteral:
		return reduceBinaryExprStringLHS(op, lhs, rhs)
	case *TimeLiteral:
		return reduceBinaryExprTimeLHS(op, lhs, rhs)
	default:
		return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
	}
}

func reduceBinaryExprBooleanLHS(op Token, lhs *BooleanLiteral, rhs Expr) Expr {
	switch rhs := rhs.(type) {
	case *BooleanLiteral:
		switch op {
		case EQ:
			return &BooleanLiteral{Val: lhs.Val == rhs.Val}
		case NEQ:
			return &BooleanLiteral{Val: lhs.Val != rhs.Val}
		case AND:
			return &BooleanLiteral{Val: lhs.Val && rhs.Val}
		case OR:
			return &BooleanLiteral{Val: lhs.Val || rhs.Val}
		}
	case *nilLiteral:
		return &BooleanLiteral{Val: false}
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
}

func reduceBinaryExprDurationLHS(op Token, lhs *DurationLiteral, rhs Expr) Expr {
	switch rhs := rhs.(type) {
	case *DurationLiteral:
		switch op {
		case ADD:
			return &DurationLiteral{Val: lhs.Val + rhs.Val}
		case SUB:
			return &DurationLiteral{Val: lhs.Val - rhs.Val}
		case EQ:
			return &BooleanLiteral{Val: lhs.Val == rhs.Val}
		case NEQ:
			return &BooleanLiteral{Val: lhs.Val != rhs.Val}
		case GT:
			return &BooleanLiteral{Val: lhs.Val > rhs.Val}
		case GTE:
			return &BooleanLiteral{Val: lhs.Val >= rhs.Val}
		case LT:
			return &BooleanLiteral{Val: lhs.Val < rhs.Val}
		case LTE:
			return &BooleanLiteral{Val: lhs.Val <= rhs.Val}
		}
	case *NumberLiteral:
		switch op {
		case MUL:
			return &DurationLiteral{Val: lhs.Val * time.Duration(rhs.Val)}
		case DIV:
			if rhs.Val == 0 {
				return &DurationLiteral{Val: 0}
			}
			return &DurationLiteral{Val: lhs.Val / time.Duration(rhs.Val)}
		}
	case *TimeLiteral:
		switch op {
		case ADD:
			return &TimeLiteral{Val: rhs.Val.Add(lhs.Val)}
		}
	case *nilLiteral:
		return &BooleanLiteral{Val: false}
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
}

func reduceBinaryExprNilLHS(op Token, lhs *nilLiteral, rhs Expr) Expr {
	switch op {
	case EQ, NEQ:
		return &BooleanLiteral{Val: false}
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
}

func reduceBinaryExprNumberLHS(op Token, lhs *NumberLiteral, rhs Expr) Expr {
	switch rhs := rhs.(type) {
	case *NumberLiteral:
		switch op {
		case ADD:
			return &NumberLiteral{Val: lhs.Val + rhs.Val}
		case SUB:
			return &NumberLiteral{Val: lhs.Val - rhs.Val}
		case MUL:
			return &NumberLiteral{Val: lhs.Val * rhs.Val}
		case DIV:
			if rhs.Val == 0 {
				return &NumberLiteral{Val: 0}
			}
			return &NumberLiteral{Val: lhs.Val / rhs.Val}
		case EQ:
			return &BooleanLiteral{Val: lhs.Val == rhs.Val}
		case NEQ:
			return &BooleanLiteral{Val: lhs.Val != rhs.Val}
		case GT:
			return &BooleanLiteral{Val: lhs.Val > rhs.Val}
		case GTE:
			return &BooleanLiteral{Val: lhs.Val >= rhs.Val}
		case LT:
			return &BooleanLiteral{Val: lhs.Val < rhs.Val}
		case LTE:
			return &BooleanLiteral{Val: lhs.Val <= rhs.Val}
		}
	case *nilLiteral:
		return &BooleanLiteral{Val: false}
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
}

func reduceBinaryExprIntegerLHS(op Token, lhs *IntegerLiteral, rhs Expr) Expr {
	switch rhs := rhs.(type) {
	case *IntegerLiteral:
		switch op {
		case ADD:
			return &IntegerLiteral{Val: lhs.Val + rhs.Val}
		case SUB:
			return &IntegerLiteral{Val: lhs.Val - rhs.Val}
		case MUL:
			return &IntegerLiteral{Val: lhs.Val * rhs.Val}
		case DIV:
			if rhs.Val == 0 {
				return &IntegerLiteral{Val: math.MaxInt64}
			}
			return &NumberLiteral{Val: float64(lhs.Val) / float64(rhs.Val)}
		case EQ:
			return &BooleanLiteral{Val: lhs.Val == rhs.Val}
		case NEQ:
			return &BooleanLiteral{Val: lhs.Val != rhs.Val}
		case GT:
			return &BooleanLiteral{Val: lhs.Val > rhs.Val}
		case GTE:
			return &BooleanLiteral{Val: lhs.Val >= rhs.Val}
		case LT:
			return &BooleanLiteral{Val: lhs.Val < rhs.Val}
		case LTE:
			return &BooleanLiteral{Val: lhs.Val <= rhs.Val}
		}
	case *nilLiteral:
		return &BooleanLiteral{Val: false}
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
}

func reduceBinaryExprStringLHS(op Token, lhs *StringLiteral, rhs Expr) Expr {
	switch rhs := rhs.(type) {
	case *StringLiteral:
		switch op {
		case EQ:
			return &BooleanLiteral{Val: lhs.Val == rhs.Val}
		case NEQ:
			return &BooleanLiteral{Val: lhs.Val != rhs.Val}
		case ADD:
			return &StringLiteral{Val: lhs.Val + rhs.Val}
		}
	case *nilLiteral:
		switch op {
		case EQ, NEQ:
			return &BooleanLiteral{Val: false}
		}
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
}

func reduceBinaryExprTimeLHS(op Token, lhs *TimeLiteral, rhs Expr) Expr {
	switch rhs := rhs.(type) {
	case *DurationLiteral:
		switch op {
		case ADD:
			return &TimeLiteral{Val: lhs.Val.Add(rhs.Val)}
		case SUB:
			return &TimeLiteral{Val: lhs.Val.Add(-rhs.Val)}
		}
	case *TimeLiteral:
		switch op {
		case SUB:
			return &DurationLiteral{Val: lhs.Val.Sub(rhs.Val)}
		case EQ:
			return &BooleanLiteral{Val: lhs.Val.Equal(rhs.Val)}
		case NEQ:
			return &BooleanLiteral{Val: !lhs.Val.Equal(rhs.Val)}
		case GT:
			return &BooleanLiteral{Val: lhs.Val.After(rhs.Val)}
		case GTE:
			return &BooleanLiteral{Val: lhs.Val.After(rhs.Val) || lhs.Val.Equal(rhs.Val)}
		case LT:
			return &BooleanLiteral{Val: lhs.Val.Before(rhs.Val)}
		case LTE:
			return &BooleanLiteral{Val: lhs.Val.Before(rhs.Val) || lhs.Val.Equal(rhs.Val)}
		}
	case *nilLiteral:
		return &BooleanLiteral{Val: false}
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
}

func reduceCall(expr *Call, valuer Valuer) Expr {
	// Evaluate "now()" if valuer is set.
	if expr.Name == "now" && len(expr.Args) == 0 && valuer != nil {
		if v, ok := valuer.Value("now()"); ok {
			v, _ := v.(time.Time)
			return &TimeLiteral{Val: v}
		}
	}

	// Otherwise reduce arguments.
	args := make([]Expr, len(expr.Args))
	for i, arg := range expr.Args {
		args[i] = reduce(arg, valuer)
	}
	return &Call{Name: expr.Name, Args: args}
}

func reduceParenExpr(expr *ParenExpr, valuer Valuer) Expr {
	subexpr := reduce(expr.Expr, valuer)
	if subexpr, ok := subexpr.(*BinaryExpr); ok {
		return &ParenExpr{Expr: subexpr}
	}
	return subexpr
}

func reduceVarRef(expr *VarRef, valuer Valuer) Expr {
	// Ignore if there is no valuer.
	if valuer == nil {
		return &VarRef{Val: expr.Val}
	}

	// Retrieve the value of the ref.
	// Ignore if the value doesn't exist.
	v, ok := valuer.Value(expr.Val)
	if !ok {
		return &VarRef{Val: expr.Val}
	}

	// Return the value as a literal.
	switch v := v.(type) {
	case bool:
		return &BooleanLiteral{Val: v}
	case time.Duration:
		return &DurationLiteral{Val: v}
	case float64:
		return &NumberLiteral{Val: v}
	case int64:
		return &IntegerLiteral{Val: v}
	case string:
		return &StringLiteral{Val: v}
	case time.Time:
		return &TimeLiteral{Val: v}
	default:
		return &nilLiteral{}
	}
}

// Valuer is the interface that wraps the Value() method.
//
// Value returns the value and existence flag for a given key.
type Valuer interface {
	Value(key string) (interface{}, bool)
}

// nowValuer returns only the value for "now()".
type NowValuer struct {
	Now time.Time
}

func (v *NowValuer) Value(key string) (interface{}, bool) {
	if key == "now()" {
		return v.Now, true
	}
	return nil, false
}
