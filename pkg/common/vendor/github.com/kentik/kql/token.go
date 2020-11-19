package kql

import (
	"strings"
)

// Token is a lexical token of the kQL language.
type Token int

const (
	// Special tokens
	ILLEGAL Token = iota
	EOF
	WS

	literal_beg
	// Literals
	IDENT        // main
	NUMBER       // 12345.67
	DURATION_VAL // 13h
	STRING       // "abc"
	BADSTRING    // "abc
	BADESCAPE    // \q
	TRUE         // true
	FALSE        // false
	REGEX        // Regular expressions
	BADREGEX     // `.*
	literal_end

	operator_beg
	// Operators
	ADD    // +
	SUB    // -
	MUL    // *
	DIV    // /
	BITAND // &
	BITOR  // |
	BITXOR // #
	//BITNOT  // ~

	AND   // AND
	OR    // OR
	IN    // IN
	IS    // IS
	NOT   // NOT
	LIKE  // LIKE
	NIN   // NOT IN
	NLIKE // NOT LIKE

	EQ           // =
	NEQ          // !=, <>
	EQREGEX      // ~
	EQREGEXCASE  // ~*
	NEQREGEX     // !~
	NEQREGEXCASE // !~*
	LT           // <
	LTE          // <=
	GT           // >
	GTE          // >=
	EQLIKE       // ~~
	EQLIKECASE   // ~~*
	NEQLIKE      // !~~
	NEQLIKECASE  // !~~*

	// hstore operators
	KVGET    // ->
	KVSET    // =>
	KVCONCAT // ||
	operator_end

	LPAREN    // (
	RPAREN    // )
	COMMA     // ,
	COLON     // :
	SEMICOLON // ;
	DOT       // .

	keyword_beg
	// Keywords
	ALL
	ANY
	AS
	ASC
	BY
	CASE
	DEFAULT
	DESC
	DISTINCT
	DURATION
	ELSE
	END
	EVERY
	EXISTS
	FOR
	FROM
	GROUP
	HAVING
	IF
	INF
	INNER
	INTO
	LIMIT
	NULL
	OFFSET
	ON
	ORDER
	SELECT
	SERIES
	TAG
	THEN
	TO
	VALUES
	WHERE
	WHEN
	WITH
	keyword_end
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	WS:      "WS",

	IDENT:        "IDENT",
	NUMBER:       "NUMBER",
	DURATION_VAL: "DURATION_VAL",
	STRING:       "STRING",
	BADSTRING:    "BADSTRING",
	BADESCAPE:    "BADESCAPE",
	TRUE:         "TRUE",
	FALSE:        "FALSE",
	REGEX:        "REGEX",

	ADD:    "+",
	SUB:    "-",
	MUL:    "*",
	DIV:    "/",
	BITAND: "&",
	BITOR:  "|",
	BITXOR: "#",

	AND:   "AND",
	OR:    "OR",
	IN:    "IN",
	IS:    "IS",
	LIKE:  "LIKE",
	NIN:   "NOT IN",
	NLIKE: "NOT LIKE",

	EQ:           "=",
	NEQ:          "!=",
	EQREGEX:      "~",
	EQREGEXCASE:  "~*",
	NEQREGEX:     "!~",
	NEQREGEXCASE: "!~*",
	LT:           "<",
	LTE:          "<=",
	GT:           ">",
	GTE:          ">=",
	EQLIKE:       "~~",
	EQLIKECASE:   "~~*",
	NEQLIKE:      "!~~",
	NEQLIKECASE:  "!~~*",

	LPAREN:    "(",
	RPAREN:    ")",
	COMMA:     ",",
	COLON:     ":",
	SEMICOLON: ";",
	DOT:       ".",

	ALL:      "ALL",
	ANY:      "ANY",
	AS:       "AS",
	ASC:      "ASC",
	BY:       "BY",
	CASE:     "CASE",
	DEFAULT:  "DEFAULT",
	DESC:     "DESC",
	DISTINCT: "DISTINCT",
	DURATION: "DURATION",
	ELSE:     "ELSE",
	END:      "END",
	EVERY:    "EVERY",
	EXISTS:   "EXISTS",
	FOR:      "FOR",
	FROM:     "FROM",
	GROUP:    "GROUP",
	HAVING:   "HAVING",
	IF:       "IF",
	INF:      "INF",
	INNER:    "INNER",
	INTO:     "INTO",
	LIMIT:    "LIMIT",
	NOT:      "NOT",
	NULL:     "NULL",
	OFFSET:   "OFFSET",
	ON:       "ON",
	ORDER:    "ORDER",
	SELECT:   "SELECT",
	SERIES:   "SERIES",
	TAG:      "TAG",
	THEN:     "THEN",
	TO:       "TO",
	VALUES:   "VALUES",
	WHERE:    "WHERE",
	WHEN:     "WHEN",
	WITH:     "WITH",
}

var keywords map[string]Token

func init() {
	keywords = make(map[string]Token)
	for tok := keyword_beg + 1; tok < keyword_end; tok++ {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	for _, tok := range []Token{AND, OR, IN, NOT, LIKE, NIN, NLIKE} {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	keywords["true"] = TRUE
	keywords["false"] = FALSE
}

// String returns the string representation of the token.
func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		return tokens[tok]
	}
	return ""
}

// Precedence returns the operator precedence of the binary operator token.
func (tok Token) Precedence() int {
	switch tok {
	case OR:
		return 1
	case AND:
		return 2
	case NOT:
		return 3
	case EQ, NEQ:
		return 4
	case LT, GT:
		return 5
	case LTE, GTE:
		return 6
	case BITOR:
		return 7
	case BITXOR:
		return 8
	case BITAND:
		return 9
	case EQREGEX, NEQREGEX, EQREGEXCASE, NEQREGEXCASE,
		EQLIKE, EQLIKECASE, NEQLIKE, NEQLIKECASE,
		LIKE, NLIKE:
		return 10
	case IN, NIN, IS:
		return 11
	case ADD, SUB:
		return 12
	case MUL, DIV:
		return 13
	}
	return 0
}

// isOperator returns true for operator tokens.
func (tok Token) isOperator() bool { return tok > operator_beg && tok < operator_end }

//// isSubqueryOperator returns true for operator tokens.
//func (tok Token) isSubqueryOperator() bool {
//	switch tok {
//	case EXISTS, IN, ANY, ALL:
//		return true
//	default:
//		return false
//	}
//}

// tokstr returns a literal if provided, otherwise returns the token string.
func tokstr(tok Token, lit string) string {
	if lit != "" {
		return lit
	}
	return tok.String()
}

// Lookup returns the token associated with a given string.
func Lookup(ident string) Token {
	if tok, ok := keywords[strings.ToLower(ident)]; ok {
		return tok
	}
	return IDENT
}

// Pos specifies the line and character position of a token.
// The Char and Line are both zero-based indexes.
type Pos struct {
	Line int
	Char int
}
