package common

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/kentik/cdn-attribution/pkg/pgpass"
	"github.com/kentik/golog/logger"
)

const (
	OTHER_ERROR = iota + 1
	INVALID_LOG
	INVALID_USAGE
	INVALID_RES
	INVALID_ASNS
	UPDATE_ERROR
	DB_ERROR
)

type RE struct {
	ID        int
	CDN       string
	RE, NegRE *regexp.Regexp
}

func GetMainDBConn(server, port string) (*sql.DB, error) {
	pw := os.Getenv("KENTIK_PG_PASS")
	if pw == "" {
		return nil, errors.New("KENTIK_PG_PASS is not set, cannot connect to Postgres")
	}

	connStr := pgpass.ConnStr(server, port, "ch_www", "www_user", pw)

	var db *sql.DB
	var err error
	if db, err = sql.Open("postgres", connStr); err != nil {
		return nil, err
	}
	return db, nil
}

func ReadREs(l *logger.Logger, logPrefix, fname string) ([]RE, error) {
	in, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	buf := bufio.NewReader(in)
	csvs := csv.NewReader(buf)
	csvs.Comment = '#'
	csvs.FieldsPerRecord = -1 // Allow a variable # of fields.  In particular, make the 4th field optional.
	rec := 0
	REs := []RE{}
	seenIDs := map[int]string{}
	for {
		fields, err := csvs.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		rec++
		if len(fields) < 3 || len(fields) > 4 {
			return nil, fmt.Errorf("Error parsing %s:%d: expected 3-4 fields, got %d",
				fname, rec, len(fields))
		}

		re, err := makeRE(l, logPrefix, seenIDs, fields)
		if err != nil {
			return nil, err
		}

		REs = append(REs, re)
	}

	return REs, nil
}

func makeRE(l *logger.Logger, logPrefix string, seenIDs map[int]string, fields []string) (RE, error) {
	idS, newCDN, reString := fields[0], fields[1], fields[2]
	var negReString string
	if len(fields) == 4 {
		negReString = fields[3]
	}

	id, err := strconv.Atoi(idS)
	if err != nil {
		return RE{}, fmt.Errorf("Error parsing '%s' as an int; error was %v", idS, err)
	}

	if existingCDN, present := seenIDs[id]; present {
		return RE{}, fmt.Errorf("Duplicate CDN IDs: %d -> '%s' and %d -> '%s'",
			id, existingCDN, id, newCDN)
	}
	seenIDs[id] = newCDN

	if strings.Index(newCDN, "\t") != -1 {
		l.Errorf(logPrefix, `CDN name '%s' has a tab in it; removing it.`, newCDN)
		newCDN = strings.Replace(newCDN, "\t", "", -1)
	}

	reString = strings.TrimSpace(reString)
	if strings.HasSuffix(reString, "|") {
		return RE{}, fmt.Errorf("Regex must not end in '|', cdn: %d/%s, re: %s",
			id, newCDN, reString)
	}

	re, err := regexp.Compile(reString)
	if err != nil {
		return RE{}, fmt.Errorf("Error parsing cdn '%s', regex '%s': %v", newCDN, fields[1], err)
	}

	var negRE *regexp.Regexp
	if negReString != "" {
		negReString = strings.TrimSpace(negReString)
		if strings.HasSuffix(negReString, "|") {
			return RE{}, fmt.Errorf("Regex must not end in '|', cdn: %d/%s, re: %s",
				id, newCDN, negReString)
		}
		negRE, err = regexp.Compile(negReString)
		if err != nil {
			return RE{}, fmt.Errorf("Error parsing cdn '%s', negative regex '%s': %v", newCDN, fields[4], err)
		}
	}

	l.Debugf(logPrefix, "RE: '%s', '%s'", newCDN, reString)
	return RE{ID: id, CDN: newCDN, RE: re, NegRE: negRE}, nil
}
