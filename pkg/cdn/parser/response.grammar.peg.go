package parser

import (
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
)

const endSymbol rune = 1114112

/* The rule types inferred from the grammar are below. */
type pegRule uint8

const (
	ruleUnknown pegRule = iota
	ruleresult
	rulesubpart
	rulecname
	ruleurl
	rulewhitespace
	ruledomain_name
	ruletext
	rulecname_suffix
	ruleipv4
	ruleipv4_suffix
	ruleipv6
	ruleipv6_ip
	rulehexdigits
	ruleipv6_suffix
	ruledotted_quad
	ruleoctet
	rulesoa
	rulesoa_suffix
	rulens
	rulens_suffix
	ruleptr
	ruleptr_suffix
	rulemx
	rulemx_suffix
	ruletxt
	ruletxt_suffix
	rulesuffixes
	rulePegText
	ruleAction0
	ruleAction1
	ruleAction2
	ruleAction3
)

var rul3s = [...]string{
	"Unknown",
	"result",
	"subpart",
	"cname",
	"url",
	"whitespace",
	"domain_name",
	"text",
	"cname_suffix",
	"ipv4",
	"ipv4_suffix",
	"ipv6",
	"ipv6_ip",
	"hexdigits",
	"ipv6_suffix",
	"dotted_quad",
	"octet",
	"soa",
	"soa_suffix",
	"ns",
	"ns_suffix",
	"ptr",
	"ptr_suffix",
	"mx",
	"mx_suffix",
	"txt",
	"txt_suffix",
	"suffixes",
	"PegText",
	"Action0",
	"Action1",
	"Action2",
	"Action3",
}

type token32 struct {
	pegRule
	begin, end uint32
}

func (t *token32) String() string {
	return fmt.Sprintf("\x1B[34m%v\x1B[m %v %v", rul3s[t.pegRule], t.begin, t.end)
}

type node32 struct {
	token32
	up, next *node32
}

func (node *node32) print(pretty bool, buffer string) {
	var print func(node *node32, depth int)
	print = func(node *node32, depth int) {
		for node != nil {
			for c := 0; c < depth; c++ {
				fmt.Printf(" ")
			}
			rule := rul3s[node.pegRule]
			quote := strconv.Quote(string(([]rune(buffer)[node.begin:node.end])))
			if !pretty {
				fmt.Printf("%v %v\n", rule, quote)
			} else {
				fmt.Printf("\x1B[34m%v\x1B[m %v\n", rule, quote)
			}
			if node.up != nil {
				print(node.up, depth+1)
			}
			node = node.next
		}
	}
	print(node, 0)
}

func (node *node32) Print(buffer string) {
	node.print(false, buffer)
}

func (node *node32) PrettyPrint(buffer string) {
	node.print(true, buffer)
}

type tokens32 struct {
	tree []token32
}

func (t *tokens32) Trim(length uint32) {
	t.tree = t.tree[:length]
}

func (t *tokens32) Print() {
	for _, token := range t.tree {
		fmt.Println(token.String())
	}
}

func (t *tokens32) AST() *node32 {
	type element struct {
		node *node32
		down *element
	}
	tokens := t.Tokens()
	var stack *element
	for _, token := range tokens {
		if token.begin == token.end {
			continue
		}
		node := &node32{token32: token}
		for stack != nil && stack.node.begin >= token.begin && stack.node.end <= token.end {
			stack.node.next = node.up
			node.up = stack.node
			stack = stack.down
		}
		stack = &element{node: node, down: stack}
	}
	if stack != nil {
		return stack.node
	}
	return nil
}

func (t *tokens32) PrintSyntaxTree(buffer string) {
	t.AST().Print(buffer)
}

func (t *tokens32) PrettyPrintSyntaxTree(buffer string) {
	t.AST().PrettyPrint(buffer)
}

func (t *tokens32) Add(rule pegRule, begin, end, index uint32) {
	if tree := t.tree; int(index) >= len(tree) {
		expanded := make([]token32, 2*len(tree))
		copy(expanded, tree)
		t.tree = expanded
	}
	t.tree[index] = token32{
		pegRule: rule,
		begin:   begin,
		end:     end,
	}
}

func (t *tokens32) Tokens() []token32 {
	return t.tree
}

type Result struct {
	IP    []net.IP
	Cname []string

	Buffer string
	buffer []rune
	rules  [33]func() bool
	parse  func(rule ...int) error
	reset  func()
	Pretty bool
	tokens32
}

func (p *Result) Parse(rule ...int) error {
	return p.parse(rule...)
}

func (p *Result) Reset() {
	p.reset()
}

type textPosition struct {
	line, symbol int
}

type textPositionMap map[int]textPosition

func translatePositions(buffer []rune, positions []int) textPositionMap {
	length, translations, j, line, symbol := len(positions), make(textPositionMap, len(positions)), 0, 1, 0
	sort.Ints(positions)

search:
	for i, c := range buffer {
		if c == '\n' {
			line, symbol = line+1, 0
		} else {
			symbol++
		}
		if i == positions[j] {
			translations[positions[j]] = textPosition{line, symbol}
			for j++; j < length; j++ {
				if i != positions[j] {
					continue search
				}
			}
			break search
		}
	}

	return translations
}

type parseError struct {
	p   *Result
	max token32
}

func (e *parseError) Error() string {
	tokens, error := []token32{e.max}, "\n"
	positions, p := make([]int, 2*len(tokens)), 0
	for _, token := range tokens {
		positions[p], p = int(token.begin), p+1
		positions[p], p = int(token.end), p+1
	}
	translations := translatePositions(e.p.buffer, positions)
	format := "parse error near %v (line %v symbol %v - line %v symbol %v):\n%v\n"
	if e.p.Pretty {
		format = "parse error near \x1B[34m%v\x1B[m (line %v symbol %v - line %v symbol %v):\n%v\n"
	}
	for _, token := range tokens {
		begin, end := int(token.begin), int(token.end)
		error += fmt.Sprintf(format,
			rul3s[token.pegRule],
			translations[begin].line, translations[begin].symbol,
			translations[end].line, translations[end].symbol,
			strconv.Quote(string(e.p.buffer[begin:end])))
	}

	return error
}

func (p *Result) PrintSyntaxTree() {
	if p.Pretty {
		p.tokens32.PrettyPrintSyntaxTree(p.Buffer)
	} else {
		p.tokens32.PrintSyntaxTree(p.Buffer)
	}
}

func (p *Result) Execute() {
	buffer, _buffer, text, begin, end := p.Buffer, p.buffer, "", 0, 0
	for _, token := range p.Tokens() {
		switch token.pegRule {

		case rulePegText:
			begin, end = int(token.begin), int(token.end)
			text = string(_buffer[begin:end])

		case ruleAction0:
			p.storeCname(text)
		case ruleAction1:
			p.storeIP(text)
		case ruleAction2:
			p.storeIP(text)
		case ruleAction3:
			p.storeIP(text)

		}
	}
	_, _, _, _, _ = buffer, _buffer, text, begin, end
}

func (p *Result) Init() {
	var (
		max                  token32
		position, tokenIndex uint32
		buffer               []rune
	)
	p.reset = func() {
		max = token32{}
		position, tokenIndex = 0, 0

		p.buffer = []rune(p.Buffer)
		if len(p.buffer) == 0 || p.buffer[len(p.buffer)-1] != endSymbol {
			p.buffer = append(p.buffer, endSymbol)
		}
		buffer = p.buffer
	}
	p.reset()

	_rules := p.rules
	tree := tokens32{tree: make([]token32, math.MaxInt16)}
	p.parse = func(rule ...int) error {
		r := 1
		if len(rule) > 0 {
			r = rule[0]
		}
		matches := p.rules[r]()
		p.tokens32 = tree
		if matches {
			p.Trim(tokenIndex)
			return nil
		}
		return &parseError{p, max}
	}

	add := func(rule pegRule, begin uint32) {
		tree.Add(rule, begin, position, tokenIndex)
		tokenIndex++
		if begin != position && position > max.end {
			max = token32{rule, begin, position}
		}
	}

	matchDot := func() bool {
		if buffer[position] != endSymbol {
			position++
			return true
		}
		return false
	}

	/*matchChar := func(c byte) bool {
		if buffer[position] == c {
			position++
			return true
		}
		return false
	}*/

	/*matchRange := func(lower byte, upper byte) bool {
		if c := buffer[position]; c >= lower && c <= upper {
			position++
			return true
		}
		return false
	}*/

	_rules = [...]func() bool{
		nil,
		/* 0 result <- <(';'* subpart (';' subpart?)* !.)> */
		func() bool {
			position0, tokenIndex0 := position, tokenIndex
			{
				position1 := position
			l2:
				{
					position3, tokenIndex3 := position, tokenIndex
					if buffer[position] != rune(';') {
						goto l3
					}
					position++
					goto l2
				l3:
					position, tokenIndex = position3, tokenIndex3
				}
				if !_rules[rulesubpart]() {
					goto l0
				}
			l4:
				{
					position5, tokenIndex5 := position, tokenIndex
					if buffer[position] != rune(';') {
						goto l5
					}
					position++
					{
						position6, tokenIndex6 := position, tokenIndex
						if !_rules[rulesubpart]() {
							goto l6
						}
						goto l7
					l6:
						position, tokenIndex = position6, tokenIndex6
					}
				l7:
					goto l4
				l5:
					position, tokenIndex = position5, tokenIndex5
				}
				{
					position8, tokenIndex8 := position, tokenIndex
					if !matchDot() {
						goto l8
					}
					goto l0
				l8:
					position, tokenIndex = position8, tokenIndex8
				}
				add(ruleresult, position1)
			}
			return true
		l0:
			position, tokenIndex = position0, tokenIndex0
			return false
		},
		/* 1 subpart <- <(whitespace* (cname / ipv4 / ipv6 / soa / ns / ptr / mx / txt))> */
		func() bool {
			position9, tokenIndex9 := position, tokenIndex
			{
				position10 := position
			l11:
				{
					position12, tokenIndex12 := position, tokenIndex
					if !_rules[rulewhitespace]() {
						goto l12
					}
					goto l11
				l12:
					position, tokenIndex = position12, tokenIndex12
				}
				{
					position13, tokenIndex13 := position, tokenIndex
					if !_rules[rulecname]() {
						goto l14
					}
					goto l13
				l14:
					position, tokenIndex = position13, tokenIndex13
					if !_rules[ruleipv4]() {
						goto l15
					}
					goto l13
				l15:
					position, tokenIndex = position13, tokenIndex13
					if !_rules[ruleipv6]() {
						goto l16
					}
					goto l13
				l16:
					position, tokenIndex = position13, tokenIndex13
					if !_rules[rulesoa]() {
						goto l17
					}
					goto l13
				l17:
					position, tokenIndex = position13, tokenIndex13
					if !_rules[rulens]() {
						goto l18
					}
					goto l13
				l18:
					position, tokenIndex = position13, tokenIndex13
					if !_rules[ruleptr]() {
						goto l19
					}
					goto l13
				l19:
					position, tokenIndex = position13, tokenIndex13
					if !_rules[rulemx]() {
						goto l20
					}
					goto l13
				l20:
					position, tokenIndex = position13, tokenIndex13
					if !_rules[ruletxt]() {
						goto l9
					}
				}
			l13:
				add(rulesubpart, position10)
			}
			return true
		l9:
			position, tokenIndex = position9, tokenIndex9
			return false
		},
		/* 2 cname <- <((<domain_name> '/' cname_suffix Action0) / (url '/' '/'? cname_suffix))> */
		func() bool {
			position21, tokenIndex21 := position, tokenIndex
			{
				position22 := position
				{
					position23, tokenIndex23 := position, tokenIndex
					{
						position25 := position
						if !_rules[ruledomain_name]() {
							goto l24
						}
						add(rulePegText, position25)
					}
					if buffer[position] != rune('/') {
						goto l24
					}
					position++
					if !_rules[rulecname_suffix]() {
						goto l24
					}
					if !_rules[ruleAction0]() {
						goto l24
					}
					goto l23
				l24:
					position, tokenIndex = position23, tokenIndex23
					if !_rules[ruleurl]() {
						goto l21
					}
					if buffer[position] != rune('/') {
						goto l21
					}
					position++
					{
						position26, tokenIndex26 := position, tokenIndex
						if buffer[position] != rune('/') {
							goto l26
						}
						position++
						goto l27
					l26:
						position, tokenIndex = position26, tokenIndex26
					}
				l27:
					if !_rules[rulecname_suffix]() {
						goto l21
					}
				}
			l23:
				add(rulecname, position22)
			}
			return true
		l21:
			position, tokenIndex = position21, tokenIndex21
			return false
		},
		/* 3 url <- <((('h' / 'H') ('t' / 'T') ('t' / 'T') ('p' / 'P') ('s' / 'S')? (':' '/' '/'))? text ('/' !cname_suffix text)*)> */
		func() bool {
			position28, tokenIndex28 := position, tokenIndex
			{
				position29 := position
				{
					position30, tokenIndex30 := position, tokenIndex
					{
						position32, tokenIndex32 := position, tokenIndex
						if buffer[position] != rune('h') {
							goto l33
						}
						position++
						goto l32
					l33:
						position, tokenIndex = position32, tokenIndex32
						if buffer[position] != rune('H') {
							goto l30
						}
						position++
					}
				l32:
					{
						position34, tokenIndex34 := position, tokenIndex
						if buffer[position] != rune('t') {
							goto l35
						}
						position++
						goto l34
					l35:
						position, tokenIndex = position34, tokenIndex34
						if buffer[position] != rune('T') {
							goto l30
						}
						position++
					}
				l34:
					{
						position36, tokenIndex36 := position, tokenIndex
						if buffer[position] != rune('t') {
							goto l37
						}
						position++
						goto l36
					l37:
						position, tokenIndex = position36, tokenIndex36
						if buffer[position] != rune('T') {
							goto l30
						}
						position++
					}
				l36:
					{
						position38, tokenIndex38 := position, tokenIndex
						if buffer[position] != rune('p') {
							goto l39
						}
						position++
						goto l38
					l39:
						position, tokenIndex = position38, tokenIndex38
						if buffer[position] != rune('P') {
							goto l30
						}
						position++
					}
				l38:
					{
						position40, tokenIndex40 := position, tokenIndex
						{
							position42, tokenIndex42 := position, tokenIndex
							if buffer[position] != rune('s') {
								goto l43
							}
							position++
							goto l42
						l43:
							position, tokenIndex = position42, tokenIndex42
							if buffer[position] != rune('S') {
								goto l40
							}
							position++
						}
					l42:
						goto l41
					l40:
						position, tokenIndex = position40, tokenIndex40
					}
				l41:
					if buffer[position] != rune(':') {
						goto l30
					}
					position++
					if buffer[position] != rune('/') {
						goto l30
					}
					position++
					if buffer[position] != rune('/') {
						goto l30
					}
					position++
					goto l31
				l30:
					position, tokenIndex = position30, tokenIndex30
				}
			l31:
				if !_rules[ruletext]() {
					goto l28
				}
			l44:
				{
					position45, tokenIndex45 := position, tokenIndex
					if buffer[position] != rune('/') {
						goto l45
					}
					position++
					{
						position46, tokenIndex46 := position, tokenIndex
						if !_rules[rulecname_suffix]() {
							goto l46
						}
						goto l45
					l46:
						position, tokenIndex = position46, tokenIndex46
					}
					if !_rules[ruletext]() {
						goto l45
					}
					goto l44
				l45:
					position, tokenIndex = position45, tokenIndex45
				}
				add(ruleurl, position29)
			}
			return true
		l28:
			position, tokenIndex = position28, tokenIndex28
			return false
		},
		/* 4 whitespace <- <' '> */
		func() bool {
			position47, tokenIndex47 := position, tokenIndex
			{
				position48 := position
				if buffer[position] != rune(' ') {
					goto l47
				}
				position++
				add(rulewhitespace, position48)
			}
			return true
		l47:
			position, tokenIndex = position47, tokenIndex47
			return false
		},
		/* 5 domain_name <- <(text ('/' !suffixes)?)+> */
		func() bool {
			position49, tokenIndex49 := position, tokenIndex
			{
				position50 := position
				if !_rules[ruletext]() {
					goto l49
				}
				{
					position53, tokenIndex53 := position, tokenIndex
					if buffer[position] != rune('/') {
						goto l53
					}
					position++
					{
						position55, tokenIndex55 := position, tokenIndex
						if !_rules[rulesuffixes]() {
							goto l55
						}
						goto l53
					l55:
						position, tokenIndex = position55, tokenIndex55
					}
					goto l54
				l53:
					position, tokenIndex = position53, tokenIndex53
				}
			l54:
			l51:
				{
					position52, tokenIndex52 := position, tokenIndex
					if !_rules[ruletext]() {
						goto l52
					}
					{
						position56, tokenIndex56 := position, tokenIndex
						if buffer[position] != rune('/') {
							goto l56
						}
						position++
						{
							position58, tokenIndex58 := position, tokenIndex
							if !_rules[rulesuffixes]() {
								goto l58
							}
							goto l56
						l58:
							position, tokenIndex = position58, tokenIndex58
						}
						goto l57
					l56:
						position, tokenIndex = position56, tokenIndex56
					}
				l57:
					goto l51
				l52:
					position, tokenIndex = position52, tokenIndex52
				}
				add(ruledomain_name, position50)
			}
			return true
		l49:
			position, tokenIndex = position49, tokenIndex49
			return false
		},
		/* 6 text <- <(!('/' / ';') .)+> */
		func() bool {
			position59, tokenIndex59 := position, tokenIndex
			{
				position60 := position
				{
					position63, tokenIndex63 := position, tokenIndex
					{
						position64, tokenIndex64 := position, tokenIndex
						if buffer[position] != rune('/') {
							goto l65
						}
						position++
						goto l64
					l65:
						position, tokenIndex = position64, tokenIndex64
						if buffer[position] != rune(';') {
							goto l63
						}
						position++
					}
				l64:
					goto l59
				l63:
					position, tokenIndex = position63, tokenIndex63
				}
				if !matchDot() {
					goto l59
				}
			l61:
				{
					position62, tokenIndex62 := position, tokenIndex
					{
						position66, tokenIndex66 := position, tokenIndex
						{
							position67, tokenIndex67 := position, tokenIndex
							if buffer[position] != rune('/') {
								goto l68
							}
							position++
							goto l67
						l68:
							position, tokenIndex = position67, tokenIndex67
							if buffer[position] != rune(';') {
								goto l66
							}
							position++
						}
					l67:
						goto l62
					l66:
						position, tokenIndex = position66, tokenIndex66
					}
					if !matchDot() {
						goto l62
					}
					goto l61
				l62:
					position, tokenIndex = position62, tokenIndex62
				}
				add(ruletext, position60)
			}
			return true
		l59:
			position, tokenIndex = position59, tokenIndex59
			return false
		},
		/* 7 cname_suffix <- <('C' 'N' 'A' 'M' 'E')> */
		func() bool {
			position69, tokenIndex69 := position, tokenIndex
			{
				position70 := position
				if buffer[position] != rune('C') {
					goto l69
				}
				position++
				if buffer[position] != rune('N') {
					goto l69
				}
				position++
				if buffer[position] != rune('A') {
					goto l69
				}
				position++
				if buffer[position] != rune('M') {
					goto l69
				}
				position++
				if buffer[position] != rune('E') {
					goto l69
				}
				position++
				add(rulecname_suffix, position70)
			}
			return true
		l69:
			position, tokenIndex = position69, tokenIndex69
			return false
		},
		/* 8 ipv4 <- <((<dotted_quad> '/' ipv4_suffix Action1) / (':' ':' <dotted_quad> '/' ipv6_suffix Action2))> */
		func() bool {
			position71, tokenIndex71 := position, tokenIndex
			{
				position72 := position
				{
					position73, tokenIndex73 := position, tokenIndex
					{
						position75 := position
						if !_rules[ruledotted_quad]() {
							goto l74
						}
						add(rulePegText, position75)
					}
					if buffer[position] != rune('/') {
						goto l74
					}
					position++
					if !_rules[ruleipv4_suffix]() {
						goto l74
					}
					if !_rules[ruleAction1]() {
						goto l74
					}
					goto l73
				l74:
					position, tokenIndex = position73, tokenIndex73
					if buffer[position] != rune(':') {
						goto l71
					}
					position++
					if buffer[position] != rune(':') {
						goto l71
					}
					position++
					{
						position76 := position
						if !_rules[ruledotted_quad]() {
							goto l71
						}
						add(rulePegText, position76)
					}
					if buffer[position] != rune('/') {
						goto l71
					}
					position++
					if !_rules[ruleipv6_suffix]() {
						goto l71
					}
					if !_rules[ruleAction2]() {
						goto l71
					}
				}
			l73:
				add(ruleipv4, position72)
			}
			return true
		l71:
			position, tokenIndex = position71, tokenIndex71
			return false
		},
		/* 9 ipv4_suffix <- <('a' / 'A')> */
		func() bool {
			position77, tokenIndex77 := position, tokenIndex
			{
				position78 := position
				{
					position79, tokenIndex79 := position, tokenIndex
					if buffer[position] != rune('a') {
						goto l80
					}
					position++
					goto l79
				l80:
					position, tokenIndex = position79, tokenIndex79
					if buffer[position] != rune('A') {
						goto l77
					}
					position++
				}
			l79:
				add(ruleipv4_suffix, position78)
			}
			return true
		l77:
			position, tokenIndex = position77, tokenIndex77
			return false
		},
		/* 10 ipv6 <- <(<ipv6_ip> '/' ipv6_suffix Action3)> */
		func() bool {
			position81, tokenIndex81 := position, tokenIndex
			{
				position82 := position
				{
					position83 := position
					if !_rules[ruleipv6_ip]() {
						goto l81
					}
					add(rulePegText, position83)
				}
				if buffer[position] != rune('/') {
					goto l81
				}
				position++
				if !_rules[ruleipv6_suffix]() {
					goto l81
				}
				if !_rules[ruleAction3]() {
					goto l81
				}
				add(ruleipv6, position82)
			}
			return true
		l81:
			position, tokenIndex = position81, tokenIndex81
			return false
		},
		/* 11 ipv6_ip <- <((':' ':' ('f' / 'F') ('f' / 'F') ('f' / 'F') ('f' / 'F') ':' dotted_quad) / (hexdigits? (':' hexdigits)* (':' ':' hexdigits?)? (':' hexdigits)*))> */
		func() bool {
			{
				position85 := position
				{
					position86, tokenIndex86 := position, tokenIndex
					if buffer[position] != rune(':') {
						goto l87
					}
					position++
					if buffer[position] != rune(':') {
						goto l87
					}
					position++
					{
						position88, tokenIndex88 := position, tokenIndex
						if buffer[position] != rune('f') {
							goto l89
						}
						position++
						goto l88
					l89:
						position, tokenIndex = position88, tokenIndex88
						if buffer[position] != rune('F') {
							goto l87
						}
						position++
					}
				l88:
					{
						position90, tokenIndex90 := position, tokenIndex
						if buffer[position] != rune('f') {
							goto l91
						}
						position++
						goto l90
					l91:
						position, tokenIndex = position90, tokenIndex90
						if buffer[position] != rune('F') {
							goto l87
						}
						position++
					}
				l90:
					{
						position92, tokenIndex92 := position, tokenIndex
						if buffer[position] != rune('f') {
							goto l93
						}
						position++
						goto l92
					l93:
						position, tokenIndex = position92, tokenIndex92
						if buffer[position] != rune('F') {
							goto l87
						}
						position++
					}
				l92:
					{
						position94, tokenIndex94 := position, tokenIndex
						if buffer[position] != rune('f') {
							goto l95
						}
						position++
						goto l94
					l95:
						position, tokenIndex = position94, tokenIndex94
						if buffer[position] != rune('F') {
							goto l87
						}
						position++
					}
				l94:
					if buffer[position] != rune(':') {
						goto l87
					}
					position++
					if !_rules[ruledotted_quad]() {
						goto l87
					}
					goto l86
				l87:
					position, tokenIndex = position86, tokenIndex86
					{
						position96, tokenIndex96 := position, tokenIndex
						if !_rules[rulehexdigits]() {
							goto l96
						}
						goto l97
					l96:
						position, tokenIndex = position96, tokenIndex96
					}
				l97:
				l98:
					{
						position99, tokenIndex99 := position, tokenIndex
						if buffer[position] != rune(':') {
							goto l99
						}
						position++
						if !_rules[rulehexdigits]() {
							goto l99
						}
						goto l98
					l99:
						position, tokenIndex = position99, tokenIndex99
					}
					{
						position100, tokenIndex100 := position, tokenIndex
						if buffer[position] != rune(':') {
							goto l100
						}
						position++
						if buffer[position] != rune(':') {
							goto l100
						}
						position++
						{
							position102, tokenIndex102 := position, tokenIndex
							if !_rules[rulehexdigits]() {
								goto l102
							}
							goto l103
						l102:
							position, tokenIndex = position102, tokenIndex102
						}
					l103:
						goto l101
					l100:
						position, tokenIndex = position100, tokenIndex100
					}
				l101:
				l104:
					{
						position105, tokenIndex105 := position, tokenIndex
						if buffer[position] != rune(':') {
							goto l105
						}
						position++
						if !_rules[rulehexdigits]() {
							goto l105
						}
						goto l104
					l105:
						position, tokenIndex = position105, tokenIndex105
					}
				}
			l86:
				add(ruleipv6_ip, position85)
			}
			return true
		},
		/* 12 hexdigits <- <([a-f] / [A-F] / [0-9])+> */
		func() bool {
			position106, tokenIndex106 := position, tokenIndex
			{
				position107 := position
				{
					position110, tokenIndex110 := position, tokenIndex
					if c := buffer[position]; c < rune('a') || c > rune('f') {
						goto l111
					}
					position++
					goto l110
				l111:
					position, tokenIndex = position110, tokenIndex110
					if c := buffer[position]; c < rune('A') || c > rune('F') {
						goto l112
					}
					position++
					goto l110
				l112:
					position, tokenIndex = position110, tokenIndex110
					if c := buffer[position]; c < rune('0') || c > rune('9') {
						goto l106
					}
					position++
				}
			l110:
			l108:
				{
					position109, tokenIndex109 := position, tokenIndex
					{
						position113, tokenIndex113 := position, tokenIndex
						if c := buffer[position]; c < rune('a') || c > rune('f') {
							goto l114
						}
						position++
						goto l113
					l114:
						position, tokenIndex = position113, tokenIndex113
						if c := buffer[position]; c < rune('A') || c > rune('F') {
							goto l115
						}
						position++
						goto l113
					l115:
						position, tokenIndex = position113, tokenIndex113
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l109
						}
						position++
					}
				l113:
					goto l108
				l109:
					position, tokenIndex = position109, tokenIndex109
				}
				add(rulehexdigits, position107)
			}
			return true
		l106:
			position, tokenIndex = position106, tokenIndex106
			return false
		},
		/* 13 ipv6_suffix <- <('A' 'A' 'A' 'A')> */
		func() bool {
			position116, tokenIndex116 := position, tokenIndex
			{
				position117 := position
				if buffer[position] != rune('A') {
					goto l116
				}
				position++
				if buffer[position] != rune('A') {
					goto l116
				}
				position++
				if buffer[position] != rune('A') {
					goto l116
				}
				position++
				if buffer[position] != rune('A') {
					goto l116
				}
				position++
				add(ruleipv6_suffix, position117)
			}
			return true
		l116:
			position, tokenIndex = position116, tokenIndex116
			return false
		},
		/* 14 dotted_quad <- <(octet '.' octet '.' octet '.' octet)> */
		func() bool {
			position118, tokenIndex118 := position, tokenIndex
			{
				position119 := position
				if !_rules[ruleoctet]() {
					goto l118
				}
				if buffer[position] != rune('.') {
					goto l118
				}
				position++
				if !_rules[ruleoctet]() {
					goto l118
				}
				if buffer[position] != rune('.') {
					goto l118
				}
				position++
				if !_rules[ruleoctet]() {
					goto l118
				}
				if buffer[position] != rune('.') {
					goto l118
				}
				position++
				if !_rules[ruleoctet]() {
					goto l118
				}
				add(ruledotted_quad, position119)
			}
			return true
		l118:
			position, tokenIndex = position118, tokenIndex118
			return false
		},
		/* 15 octet <- <(('2' '5' [0-5]) / ('2' [0-4] [0-9]) / ([0-1] [0-9] [0-9]) / ([0-9] [0-9]) / [0-9])> */
		func() bool {
			position120, tokenIndex120 := position, tokenIndex
			{
				position121 := position
				{
					position122, tokenIndex122 := position, tokenIndex
					if buffer[position] != rune('2') {
						goto l123
					}
					position++
					if buffer[position] != rune('5') {
						goto l123
					}
					position++
					if c := buffer[position]; c < rune('0') || c > rune('5') {
						goto l123
					}
					position++
					goto l122
				l123:
					position, tokenIndex = position122, tokenIndex122
					if buffer[position] != rune('2') {
						goto l124
					}
					position++
					if c := buffer[position]; c < rune('0') || c > rune('4') {
						goto l124
					}
					position++
					if c := buffer[position]; c < rune('0') || c > rune('9') {
						goto l124
					}
					position++
					goto l122
				l124:
					position, tokenIndex = position122, tokenIndex122
					if c := buffer[position]; c < rune('0') || c > rune('1') {
						goto l125
					}
					position++
					if c := buffer[position]; c < rune('0') || c > rune('9') {
						goto l125
					}
					position++
					if c := buffer[position]; c < rune('0') || c > rune('9') {
						goto l125
					}
					position++
					goto l122
				l125:
					position, tokenIndex = position122, tokenIndex122
					if c := buffer[position]; c < rune('0') || c > rune('9') {
						goto l126
					}
					position++
					if c := buffer[position]; c < rune('0') || c > rune('9') {
						goto l126
					}
					position++
					goto l122
				l126:
					position, tokenIndex = position122, tokenIndex122
					if c := buffer[position]; c < rune('0') || c > rune('9') {
						goto l120
					}
					position++
				}
			l122:
				add(ruleoctet, position121)
			}
			return true
		l120:
			position, tokenIndex = position120, tokenIndex120
			return false
		},
		/* 16 soa <- <(domain_name? '/' soa_suffix)> */
		func() bool {
			position127, tokenIndex127 := position, tokenIndex
			{
				position128 := position
				{
					position129, tokenIndex129 := position, tokenIndex
					if !_rules[ruledomain_name]() {
						goto l129
					}
					goto l130
				l129:
					position, tokenIndex = position129, tokenIndex129
				}
			l130:
				if buffer[position] != rune('/') {
					goto l127
				}
				position++
				if !_rules[rulesoa_suffix]() {
					goto l127
				}
				add(rulesoa, position128)
			}
			return true
		l127:
			position, tokenIndex = position127, tokenIndex127
			return false
		},
		/* 17 soa_suffix <- <(('s' / 'S') ('o' / 'O') ('a' / 'A'))> */
		func() bool {
			position131, tokenIndex131 := position, tokenIndex
			{
				position132 := position
				{
					position133, tokenIndex133 := position, tokenIndex
					if buffer[position] != rune('s') {
						goto l134
					}
					position++
					goto l133
				l134:
					position, tokenIndex = position133, tokenIndex133
					if buffer[position] != rune('S') {
						goto l131
					}
					position++
				}
			l133:
				{
					position135, tokenIndex135 := position, tokenIndex
					if buffer[position] != rune('o') {
						goto l136
					}
					position++
					goto l135
				l136:
					position, tokenIndex = position135, tokenIndex135
					if buffer[position] != rune('O') {
						goto l131
					}
					position++
				}
			l135:
				{
					position137, tokenIndex137 := position, tokenIndex
					if buffer[position] != rune('a') {
						goto l138
					}
					position++
					goto l137
				l138:
					position, tokenIndex = position137, tokenIndex137
					if buffer[position] != rune('A') {
						goto l131
					}
					position++
				}
			l137:
				add(rulesoa_suffix, position132)
			}
			return true
		l131:
			position, tokenIndex = position131, tokenIndex131
			return false
		},
		/* 18 ns <- <(domain_name? '/' ns_suffix)> */
		func() bool {
			position139, tokenIndex139 := position, tokenIndex
			{
				position140 := position
				{
					position141, tokenIndex141 := position, tokenIndex
					if !_rules[ruledomain_name]() {
						goto l141
					}
					goto l142
				l141:
					position, tokenIndex = position141, tokenIndex141
				}
			l142:
				if buffer[position] != rune('/') {
					goto l139
				}
				position++
				if !_rules[rulens_suffix]() {
					goto l139
				}
				add(rulens, position140)
			}
			return true
		l139:
			position, tokenIndex = position139, tokenIndex139
			return false
		},
		/* 19 ns_suffix <- <(('n' / 'N') ('s' / 'S'))> */
		func() bool {
			position143, tokenIndex143 := position, tokenIndex
			{
				position144 := position
				{
					position145, tokenIndex145 := position, tokenIndex
					if buffer[position] != rune('n') {
						goto l146
					}
					position++
					goto l145
				l146:
					position, tokenIndex = position145, tokenIndex145
					if buffer[position] != rune('N') {
						goto l143
					}
					position++
				}
			l145:
				{
					position147, tokenIndex147 := position, tokenIndex
					if buffer[position] != rune('s') {
						goto l148
					}
					position++
					goto l147
				l148:
					position, tokenIndex = position147, tokenIndex147
					if buffer[position] != rune('S') {
						goto l143
					}
					position++
				}
			l147:
				add(rulens_suffix, position144)
			}
			return true
		l143:
			position, tokenIndex = position143, tokenIndex143
			return false
		},
		/* 20 ptr <- <(domain_name? '/' ptr_suffix)> */
		func() bool {
			position149, tokenIndex149 := position, tokenIndex
			{
				position150 := position
				{
					position151, tokenIndex151 := position, tokenIndex
					if !_rules[ruledomain_name]() {
						goto l151
					}
					goto l152
				l151:
					position, tokenIndex = position151, tokenIndex151
				}
			l152:
				if buffer[position] != rune('/') {
					goto l149
				}
				position++
				if !_rules[ruleptr_suffix]() {
					goto l149
				}
				add(ruleptr, position150)
			}
			return true
		l149:
			position, tokenIndex = position149, tokenIndex149
			return false
		},
		/* 21 ptr_suffix <- <(('p' / 'P') ('t' / 'T') ('r' / 'R'))> */
		func() bool {
			position153, tokenIndex153 := position, tokenIndex
			{
				position154 := position
				{
					position155, tokenIndex155 := position, tokenIndex
					if buffer[position] != rune('p') {
						goto l156
					}
					position++
					goto l155
				l156:
					position, tokenIndex = position155, tokenIndex155
					if buffer[position] != rune('P') {
						goto l153
					}
					position++
				}
			l155:
				{
					position157, tokenIndex157 := position, tokenIndex
					if buffer[position] != rune('t') {
						goto l158
					}
					position++
					goto l157
				l158:
					position, tokenIndex = position157, tokenIndex157
					if buffer[position] != rune('T') {
						goto l153
					}
					position++
				}
			l157:
				{
					position159, tokenIndex159 := position, tokenIndex
					if buffer[position] != rune('r') {
						goto l160
					}
					position++
					goto l159
				l160:
					position, tokenIndex = position159, tokenIndex159
					if buffer[position] != rune('R') {
						goto l153
					}
					position++
				}
			l159:
				add(ruleptr_suffix, position154)
			}
			return true
		l153:
			position, tokenIndex = position153, tokenIndex153
			return false
		},
		/* 22 mx <- <(domain_name? '/' mx_suffix)> */
		func() bool {
			position161, tokenIndex161 := position, tokenIndex
			{
				position162 := position
				{
					position163, tokenIndex163 := position, tokenIndex
					if !_rules[ruledomain_name]() {
						goto l163
					}
					goto l164
				l163:
					position, tokenIndex = position163, tokenIndex163
				}
			l164:
				if buffer[position] != rune('/') {
					goto l161
				}
				position++
				if !_rules[rulemx_suffix]() {
					goto l161
				}
				add(rulemx, position162)
			}
			return true
		l161:
			position, tokenIndex = position161, tokenIndex161
			return false
		},
		/* 23 mx_suffix <- <(('m' / 'M') ('x' / 'X'))> */
		func() bool {
			position165, tokenIndex165 := position, tokenIndex
			{
				position166 := position
				{
					position167, tokenIndex167 := position, tokenIndex
					if buffer[position] != rune('m') {
						goto l168
					}
					position++
					goto l167
				l168:
					position, tokenIndex = position167, tokenIndex167
					if buffer[position] != rune('M') {
						goto l165
					}
					position++
				}
			l167:
				{
					position169, tokenIndex169 := position, tokenIndex
					if buffer[position] != rune('x') {
						goto l170
					}
					position++
					goto l169
				l170:
					position, tokenIndex = position169, tokenIndex169
					if buffer[position] != rune('X') {
						goto l165
					}
					position++
				}
			l169:
				add(rulemx_suffix, position166)
			}
			return true
		l165:
			position, tokenIndex = position165, tokenIndex165
			return false
		},
		/* 24 txt <- <(('/' txt_suffix) / (. txt))> */
		func() bool {
			position171, tokenIndex171 := position, tokenIndex
			{
				position172 := position
				{
					position173, tokenIndex173 := position, tokenIndex
					if buffer[position] != rune('/') {
						goto l174
					}
					position++
					if !_rules[ruletxt_suffix]() {
						goto l174
					}
					goto l173
				l174:
					position, tokenIndex = position173, tokenIndex173
					if !matchDot() {
						goto l171
					}
					if !_rules[ruletxt]() {
						goto l171
					}
				}
			l173:
				add(ruletxt, position172)
			}
			return true
		l171:
			position, tokenIndex = position171, tokenIndex171
			return false
		},
		/* 25 txt_suffix <- <(('t' / 'T') ('x' / 'X') ('t' / 'T'))> */
		func() bool {
			position175, tokenIndex175 := position, tokenIndex
			{
				position176 := position
				{
					position177, tokenIndex177 := position, tokenIndex
					if buffer[position] != rune('t') {
						goto l178
					}
					position++
					goto l177
				l178:
					position, tokenIndex = position177, tokenIndex177
					if buffer[position] != rune('T') {
						goto l175
					}
					position++
				}
			l177:
				{
					position179, tokenIndex179 := position, tokenIndex
					if buffer[position] != rune('x') {
						goto l180
					}
					position++
					goto l179
				l180:
					position, tokenIndex = position179, tokenIndex179
					if buffer[position] != rune('X') {
						goto l175
					}
					position++
				}
			l179:
				{
					position181, tokenIndex181 := position, tokenIndex
					if buffer[position] != rune('t') {
						goto l182
					}
					position++
					goto l181
				l182:
					position, tokenIndex = position181, tokenIndex181
					if buffer[position] != rune('T') {
						goto l175
					}
					position++
				}
			l181:
				add(ruletxt_suffix, position176)
			}
			return true
		l175:
			position, tokenIndex = position175, tokenIndex175
			return false
		},
		/* 26 suffixes <- <(cname_suffix / ipv4_suffix / ipv6_suffix / soa_suffix / ns_suffix / ptr_suffix / mx_suffix / txt_suffix)> */
		func() bool {
			position183, tokenIndex183 := position, tokenIndex
			{
				position184 := position
				{
					position185, tokenIndex185 := position, tokenIndex
					if !_rules[rulecname_suffix]() {
						goto l186
					}
					goto l185
				l186:
					position, tokenIndex = position185, tokenIndex185
					if !_rules[ruleipv4_suffix]() {
						goto l187
					}
					goto l185
				l187:
					position, tokenIndex = position185, tokenIndex185
					if !_rules[ruleipv6_suffix]() {
						goto l188
					}
					goto l185
				l188:
					position, tokenIndex = position185, tokenIndex185
					if !_rules[rulesoa_suffix]() {
						goto l189
					}
					goto l185
				l189:
					position, tokenIndex = position185, tokenIndex185
					if !_rules[rulens_suffix]() {
						goto l190
					}
					goto l185
				l190:
					position, tokenIndex = position185, tokenIndex185
					if !_rules[ruleptr_suffix]() {
						goto l191
					}
					goto l185
				l191:
					position, tokenIndex = position185, tokenIndex185
					if !_rules[rulemx_suffix]() {
						goto l192
					}
					goto l185
				l192:
					position, tokenIndex = position185, tokenIndex185
					if !_rules[ruletxt_suffix]() {
						goto l183
					}
				}
			l185:
				add(rulesuffixes, position184)
			}
			return true
		l183:
			position, tokenIndex = position183, tokenIndex183
			return false
		},
		nil,
		/* 29 Action0 <- <{ p.storeCname(text) }> */
		func() bool {
			{
				add(ruleAction0, position)
			}
			return true
		},
		/* 30 Action1 <- <{ p.storeIP(text) }> */
		func() bool {
			{
				add(ruleAction1, position)
			}
			return true
		},
		/* 31 Action2 <- <{ p.storeIP(text) }> */
		func() bool {
			{
				add(ruleAction2, position)
			}
			return true
		},
		/* 32 Action3 <- <{ p.storeIP(text) }> */
		func() bool {
			{
				add(ruleAction3, position)
			}
			return true
		},
	}
	p.rules = _rules
}
