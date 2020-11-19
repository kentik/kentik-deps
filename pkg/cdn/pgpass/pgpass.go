package pgpass

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strings"
)

func Lookup(wantedHost, wantedPort, wantedDb, wantedUser string) (pw string, err error) {
	home := os.Getenv("HOME")
	if home == "" {
		return "", fmt.Errorf("HOME not set")
	}
	fname := path.Join(home, ".pgpass")

	f, err := os.Open(fname)
	if err != nil {
		return "", err
	}
	defer f.Close()

	bf := bufio.NewReader(f)
	for {
		s, err := bf.ReadString('\n')
		// You can get valid data *and* an error, so check the data first.
		if s != "" {
			if s[0] == '#' {
				continue
			}
			s = s[:len(s)-1] // snip the '\n'
			fields := strings.Split(s, ":")
			host, port, dbName, user, pw := fields[0], fields[1], fields[2], fields[3], fields[4]
			if host == wantedHost && port == wantedPort && dbName == wantedDb && user == wantedUser {
				return pw, nil
			}
		}
		if err != nil {
			break
		}
	}

	return "", fmt.Errorf("Host:port:db:user %s:%s:%s:%s not found in ~/.pgpass",
		wantedHost, wantedPort, wantedDb, wantedUser)
}

func ConnStr(host, port, db, user string, pw string) string {
	return fmt.Sprintf("user=%s dbname=%s sslmode=disable host=%s port=%s password=%s",
		user, db, host, port, pw)
}
