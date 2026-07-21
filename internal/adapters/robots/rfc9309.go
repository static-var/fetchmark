package robots

import (
	"bytes"
	"errors"
	"strings"
	"unicode/utf8"
)

type policy struct {
	groups []policyGroup
}

type policyGroup struct {
	agents []string
	rules  []policyRule
}

type policyRule struct {
	pattern     string
	allow       bool
	specificity int
}

const (
	maxPolicyRules              = 16 << 10
	maxPolicyRequestTargetBytes = 8 << 10
	maxPolicyMatchSteps         = 1 << 20
)

func parsePolicy(body []byte) (*policy, error) {
	if !utf8.Valid(body) {
		return nil, errors.New("robots: policy is not valid UTF-8")
	}
	body = bytes.TrimPrefix(body, []byte{0xef, 0xbb, 0xbf})
	lines := strings.ReplaceAll(string(body), "\r\n", "\n")
	lines = strings.ReplaceAll(lines, "\r", "\n")

	parsed := &policy{}
	var current policyGroup
	sawRule := false
	ruleCount := 0
	groupRules := make(map[string]struct{})
	flush := func() {
		if len(current.agents) > 0 {
			parsed.groups = append(parsed.groups, current)
		}
		current = policyGroup{}
		sawRule = false
		clear(groupRules)
	}
	for _, rawLine := range strings.Split(lines, "\n") {
		if comment := strings.IndexByte(rawLine, '#'); comment >= 0 {
			rawLine = rawLine[:comment]
		}
		rawLine = strings.Trim(rawLine, " \t")
		if rawLine == "" {
			continue
		}
		separator := strings.IndexByte(rawLine, ':')
		if separator < 0 {
			continue
		}
		key := strings.ToLower(strings.Trim(rawLine[:separator], " \t"))
		value := strings.Trim(rawLine[separator+1:], " \t")
		switch key {
		case "user-agent":
			if sawRule {
				flush()
			}
			agent := strings.ToLower(value)
			if validPolicyAgent(agent) {
				current.agents = append(current.agents, agent)
			}
		case "allow", "disallow":
			if len(current.agents) == 0 {
				continue
			}
			sawRule = true
			if value == "" {
				continue
			}
			pattern, ok := normalizePolicyOctets(value, true)
			if !ok || !strings.HasPrefix(pattern, "/") {
				continue
			}
			allow := key == "allow"
			ruleKey := pattern + "\x00" + key
			if _, duplicate := groupRules[ruleKey]; duplicate {
				continue
			}
			if ruleCount >= maxPolicyRules {
				return nil, errors.New("robots: policy exceeds rule limit")
			}
			groupRules[ruleKey] = struct{}{}
			ruleCount++
			current.rules = append(current.rules, policyRule{
				pattern: pattern, allow: allow, specificity: policySpecificity(pattern),
			})
		}
	}
	flush()
	return parsed, nil
}

func validPolicyAgent(value string) bool {
	if value == "*" {
		return true
	}
	if value == "" {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if (character < 'a' || character > 'z') && character != '-' && character != '_' {
			return false
		}
	}
	return true
}

func (parsed *policy) allowed(productToken, requestTarget string) bool {
	target, ok := normalizePolicyOctets(requestTarget, false)
	if !ok || len(target) > maxPolicyRequestTargetBytes {
		return false
	}
	productToken = strings.ToLower(productToken)
	exact := false
	for _, group := range parsed.groups {
		for _, agent := range group.agents {
			if agent == productToken {
				exact = true
				break
			}
		}
	}
	bestSpecificity := -1
	bestAllow := true
	remainingSteps := maxPolicyMatchSteps
	seenRules := make(map[string]struct{})
	for _, group := range parsed.groups {
		applicable := false
		for _, agent := range group.agents {
			if exact && agent == productToken || !exact && agent == "*" {
				applicable = true
				break
			}
		}
		if !applicable {
			continue
		}
		for _, rule := range group.rules {
			ruleKey := rule.pattern + "\x00"
			if rule.allow {
				ruleKey += "allow"
			} else {
				ruleKey += "disallow"
			}
			if _, duplicate := seenRules[ruleKey]; duplicate {
				continue
			}
			seenRules[ruleKey] = struct{}{}
			matched, complete := policyPatternMatches(rule.pattern, target, &remainingSteps)
			if !complete {
				return false
			}
			if !matched {
				continue
			}
			if rule.specificity > bestSpecificity || rule.specificity == bestSpecificity && rule.allow {
				bestSpecificity = rule.specificity
				bestAllow = rule.allow
			}
		}
	}
	if bestSpecificity < 0 {
		return true
	}
	return bestAllow
}

func normalizePolicyOctets(raw string, pattern bool) (string, bool) {
	if !utf8.ValidString(raw) {
		return "", false
	}
	const hexadecimal = "0123456789ABCDEF"
	var normalized strings.Builder
	normalized.Grow(len(raw))
	for index := 0; index < len(raw); index++ {
		value := raw[index]
		if value == '%' {
			if index+2 >= len(raw) {
				return "", false
			}
			high, highOK := hexadecimalValue(raw[index+1])
			low, lowOK := hexadecimalValue(raw[index+2])
			if !highOK || !lowOK {
				return "", false
			}
			decoded := high<<4 | low
			if unreserved(decoded) {
				normalized.WriteByte(decoded)
			} else {
				normalized.WriteByte('%')
				normalized.WriteByte(hexadecimal[decoded>>4])
				normalized.WriteByte(hexadecimal[decoded&0x0f])
			}
			index += 2
			continue
		}
		if value >= utf8.RuneSelf {
			normalized.WriteByte('%')
			normalized.WriteByte(hexadecimal[value>>4])
			normalized.WriteByte(hexadecimal[value&0x0f])
			continue
		}
		if value <= 0x20 || value == 0x7f {
			return "", false
		}
		if !pattern && (value == '*' || value == '$') {
			normalized.WriteByte('%')
			normalized.WriteByte(hexadecimal[value>>4])
			normalized.WriteByte(hexadecimal[value&0x0f])
			continue
		}
		normalized.WriteByte(value)
	}
	return normalized.String(), true
}

func hexadecimalValue(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	default:
		return 0, false
	}
}

func unreserved(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' ||
		value >= '0' && value <= '9' || value == '-' || value == '.' || value == '_' || value == '~'
}

func policySpecificity(pattern string) int {
	end := len(pattern)
	if end > 0 && pattern[end-1] == '$' {
		end--
	}
	specificity := 0
	for index := 0; index < end; index++ {
		switch pattern[index] {
		case '*':
		case '%':
			specificity++
			index += 2
		default:
			specificity++
		}
	}
	return specificity
}

func policyPatternMatches(pattern, target string, remainingSteps *int) (bool, bool) {
	anchored := strings.HasSuffix(pattern, "$")
	if anchored {
		pattern = strings.TrimSuffix(pattern, "$")
	}
	if !strings.Contains(pattern, "*") {
		comparisonBytes := min(len(pattern), len(target)) + 1
		if comparisonBytes > *remainingSteps {
			*remainingSteps = 0
			return false, false
		}
		*remainingSteps -= comparisonBytes
		if anchored {
			return pattern == target, true
		}
		return strings.HasPrefix(target, pattern), true
	}
	if !anchored {
		pattern += "*"
	}
	patternIndex, targetIndex := 0, 0
	starIndex, starTarget := -1, 0
	for targetIndex < len(target) {
		if *remainingSteps == 0 {
			return false, false
		}
		*remainingSteps--
		switch {
		case patternIndex < len(pattern) && pattern[patternIndex] == '*':
			starIndex = patternIndex
			patternIndex++
			starTarget = targetIndex
		case patternIndex < len(pattern) && pattern[patternIndex] == target[targetIndex]:
			patternIndex++
			targetIndex++
		case starIndex >= 0:
			patternIndex = starIndex + 1
			starTarget++
			targetIndex = starTarget
		default:
			return false, true
		}
	}
	for patternIndex < len(pattern) && pattern[patternIndex] == '*' {
		if *remainingSteps == 0 {
			return false, false
		}
		*remainingSteps--
		patternIndex++
	}
	return patternIndex == len(pattern), true
}
