package resp

import (
	"fmt"
	"log"
	"strconv"
)

type RespParser struct {
	parts  []string
	index  int
	debug  bool
}

func NewRespParserFromParts(parts []string, debug bool) *RespParser {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	return &RespParser{parts: parts, debug: debug}
}

// ParseSentinelReplicas парсит parts в []map[string]string
func (p *RespParser) ParseSentinelReplicas() ([]map[string]string, error) {
	if p.debug {
		log.Println("DEBUG: Начинаем парсинг SENTINEL REPLICAS из parts")
	}

	if p.index >= len(p.parts) {
		return nil, fmt.Errorf("no more data")
	}

	arrayHeader := p.parts[p.index]
	if p.debug {
		log.Printf("DEBUG readLine: %q", arrayHeader)
	}
	p.index++

	if arrayHeader[0] != '*' {
		return nil, fmt.Errorf("expected array *N, got %s", arrayHeader)
	}
	n, err := strconv.Atoi(arrayHeader[1:])
	if err != nil {
		return nil, err
	}
	if p.debug {
		log.Printf("DEBUG: Массив из %d элементов", n)
	}

	replicas := make([]map[string]string, 0, n)
	for i := 0; i < n; i++ {
		replica, err := p.parseHash()
		if err != nil {
			return nil, err
		}
		replicas = append(replicas, replica)
	}

	if p.debug {
		log.Printf("DEBUG: Найдено %d реплик", len(replicas))
		for i, rep := range replicas {
			log.Printf("DEBUG: Реплика %d: ip=%s port=%s flags=%v",
				i, rep["ip"], rep["port"], rep["flags"])
		}
	}
	return replicas, nil
}

func (p *RespParser) parseHash() (map[string]string, error) {
	if p.index >= len(p.parts) {
		return nil, fmt.Errorf("unexpected EOF in hash")
	}

	hashHeader := p.parts[p.index]
	if p.debug {
		log.Printf("DEBUG readLine: %q", hashHeader)
	}
	p.index++

	if hashHeader[0] != '*' {
		return nil, fmt.Errorf("expected hash *N, got %s", hashHeader)
	}
	hashLen, err := strconv.Atoi(hashHeader[1:])
	if err != nil {
		return nil, err
	}
	if p.debug {
		log.Printf("DEBUG: Hash с %d полями", hashLen)
	}

	m := make(map[string]string, hashLen)
	for j := 0; j < hashLen; j++ {
		if p.index+1 >= len(p.parts) {
			return nil, fmt.Errorf("unexpected EOF reading field %d", j)
		}

		keyLen := p.parts[p.index]
		p.index++
		key, err := p.readBulkString(keyLen)
		if err != nil {
			return nil, err
		}

		valLen := p.parts[p.index]
		p.index++
		val, err := p.readBulkString(valLen)
		if err != nil {
			return nil, err
		}

		m[key] = val
		if p.debug {
			log.Printf("DEBUG: поле %s = %s", key, val)
		}
	}
	return m, nil
}

func (p *RespParser) readBulkString(lenHeader string) (string, error) {
	if lenHeader[0] != '$' {
		return "", fmt.Errorf("expected bulk $N, got %s", lenHeader)
	}
	n, err := strconv.Atoi(lenHeader[1:])
	if err != nil {
		return "", err
	}

	if p.index >= len(p.parts) {
		return "", fmt.Errorf("unexpected EOF reading bulk string of len %d", n)
	}

	value := p.parts[p.index]
	p.index++
	if len(value) != n {
		return "", fmt.Errorf("bulk string length mismatch: expected %d, got %d", n, len(value))
	}

	if p.debug {
		log.Printf("DEBUG readBulk value: %q (len=%d)", value, n)
	}
	return value, nil
}
