package kt

import (
	"database/sql"
	"errors"

	"github.com/kentik/eggs/pkg/logger"

	"github.com/jmoiron/sqlx"
)

// ErrNilTransaction denotes empty transaction
var ErrNilTransaction = errors.New("nil transaction passed")

// ChwwwTransaction is a chwww transaction
type ChwwwTransaction struct {
	*sqlx.Tx
	logger.ContextL
}

// Commit finishes transaction
func (ptx *ChwwwTransaction) Commit() error {
	return ptx.Tx.Commit()
}

// Rollback reverts transaction
// if transaction got commited rollback does nothing
func (ptx *ChwwwTransaction) Rollback() {
	err := ptx.Tx.Rollback()
	if err != nil && err != sql.ErrTxDone {
		ptx.Errorf("chwww rollback failed: %+v", err)
	}
}

// Chalertv3Transaction is a chalertv3 transaction
type Chalertv3Transaction struct {
	*sqlx.Tx
	logger.ContextL
}

// Commit finishes transaction
func (ptx *Chalertv3Transaction) Commit() error {
	return ptx.Tx.Commit()
}

// Rollback reverts transaction
// if transaction got commited rollback does nothing
func (ptx *Chalertv3Transaction) Rollback() {
	err := ptx.Tx.Rollback()
	if err != nil {
		ptx.Errorf("chalertv3 rollback failed: %+v", err)
	}
}
