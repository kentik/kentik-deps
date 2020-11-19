package ktds

import (
	"fmt"

	"github.com/kentik/chf-alert/pkg/kt"

	"github.com/jmoiron/sqlx"
	"github.com/kentik/eggs/pkg/database"
	"github.com/kentik/eggs/pkg/logger"
)

func NewCredentialsDataSource(pgx *sqlx.DB, log logger.Underlying) (CredentialsDataSource, error) {
	return &credentialsDataSource{
		pgx: pgx,
		log: log,
	}, nil
}

type credentialsDataSource struct {
	pgx *sqlx.DB
	log logger.Underlying
}

func (ds *credentialsDataSource) Close() {
	database.CloseStatements(ds)
}

func (ds *credentialsDataSource) GetAPICredentials(cid kt.Cid, userEmail string) (string, string, error) {
	cidP := &cid
	if cid <= 0 {
		cidP = nil
	}
	userEmailP := &userEmail
	if userEmail == "" {
		userEmailP = nil
	}
	rows, err := ds.pgx.Query(`
		SELECT
			user_email,
			user_kvs -> 'api_token' AS api_token
		FROM
			mn_user
		WHERE
			company_id = COALESCE($1, company_id)
			AND user_level >= 1
			AND user_email = COALESCE($2, user_email)
			AND (user_kvs -> 'api_token') IS NOT NULL
		LIMIT 1;
    `, cidP, userEmailP)
	if err != nil {
		return "", "", fmt.Errorf("Executing GetAPICredentials query: %s", err)
	}
	defer rows.Close()

	// find the record
	apiEmail := ""
	apiToken := ""
	for rows.Next() {
		if err := rows.Scan(&apiEmail, &apiToken); err != nil {
			return "", "", fmt.Errorf("Scanning row from GetAPICredentials query: %s", err)
		}
	}

	// check for iteration error
	if err := rows.Err(); err != nil {
		return "", "", fmt.Errorf("Iterating over GetAPICredentials rows: %s", err)
	}

	// all or nothing return
	if apiEmail == "" || apiToken == "" {
		return "", "", fmt.Errorf("Could not find an API user or token from GetAPICredentials query")
	}
	return apiEmail, apiToken, nil
}
