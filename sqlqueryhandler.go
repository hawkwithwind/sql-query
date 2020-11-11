package sqlquery

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/hawkwithwind/gohandler"
)

type Database struct {
	Conn *sqlx.DB
}

type SqlQueryHandler struct {
	errp *error
}

func (o *SqlQueryHandler) Error() bool {
	return (*o.errp) != nil
}

func (o *SqlQueryHandler) Init(errp *error) {
	o.errp = errp
}

func (o *SqlQueryHandler) Set(err error) {
	*o.errp = err
}

type Queryable interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Rebind(query string) string
}

func (o *SqlQueryHandler) DefaultContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func (o *SqlQueryHandler) Connect(db *Database, driverName string, dataSourceName string) {
	if o.Error() {
		return
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	var err error
	db.Conn, err = sqlx.ConnectContext(ctx, driverName, dataSourceName)
	o.Set(err)
}

func (o *SqlQueryHandler) Begin(db *Database) *sqlx.Tx {
	if o.Error() {
		return nil
	}

	if db.Conn != nil {
		ctx, _ := o.DefaultContext()
		tx, err := db.Conn.BeginTxx(ctx, nil)
		if err != nil {
			o.Set(err)
			return nil
		}
		return tx
	} else {
		o.Set(fmt.Errorf("db.Conn is null upon calling db.BeginTxx"))
		return nil
	}
}

func (o *SqlQueryHandler) Rollback(tx *sqlx.Tx) {
	// wont check o.Err when rollback. always rollback.
	// because rollback should be done after some error occurs.

	if tx != nil {
		err := tx.Rollback()
		if err != nil {
			o.Set(err)
		}
	} else {
		if !o.Error() {
			o.Set(fmt.Errorf("tx is null upon calling tx.Rollback"))
		}
	}
}

func (o *SqlQueryHandler) Commit(tx *sqlx.Tx) {
	if o.Error() {
		return
	}

	if tx != nil {
		o.Set(tx.Commit())
	} else {
		o.Set(fmt.Errorf("tx is null upon calling tx.Commit"))
	}
}

func (o *SqlQueryHandler) CommitOrRollback(tx *sqlx.Tx) {
	if tx == nil && !o.Error() {
		o.Set(fmt.Errorf("tx is null upon calling CommitOrRollback"))
		return
	}
	
	if o.Error() {
		if tx != nil {
			tx.Rollback()
		}
	} else {
		o.Set(tx.Commit())
	}
}

func (o *SqlQueryHandler) AndEqualString(fieldName string, field sql.NullString) string {
	if o.Error() {
		return ""
	}

	if field.Valid {
		return fmt.Sprintf("  AND `%s`=?", fieldName)
	} else {
		return fmt.Sprintf("  AND (1=1 OR `%s`=?)", fieldName)
	}
}

func (o *SqlQueryHandler) AndEqualStringT(tableName string, fieldName string, field sql.NullString) string {
	if o.Error() {
		return ""
	}

	if field.Valid {
		return fmt.Sprintf("  AND `%s`,`%s`=?", tableName, fieldName)
	} else {
		return fmt.Sprintf("  AND (1=1 OR `%s`.`%s`=?)", tableName, fieldName)
	}
}

func (o *SqlQueryHandler) AndLikeString(fieldName string, field sql.NullString) string {
	if o.Error() {
		return ""
	}

	if field.Valid {
		return fmt.Sprintf("  AND `%s` like ? ", fieldName)
	} else {
		return fmt.Sprintf("  AND (1=1 OR `%s`=?)", fieldName)
	}
}

func (o *SqlQueryHandler) AndLikeStringT(tableName string, fieldName string, field sql.NullString) string {
	if o.Error() {
		return ""
	}

	if field.Valid {
		return fmt.Sprintf("  AND `%s`.`%s` like ? ", tableName, fieldName)
	} else {
		return fmt.Sprintf("  AND (1=1 OR `%s`.`%s`=?)", tableName, fieldName)
	}
}

func (o *SqlQueryHandler) AndEqual(s Searchable, fieldName string, _ interface{}) string {
	if o.Error() {
		return ""
	}

	var fn Field
	fn, o.Err = s.CriteriaAlias(fieldName)
	if o.Error() {
		return ""
	}
	return fmt.Sprintf(" AND `%s`.`%s` = ?", fn.Table, fn.Name)
}

func (o *SqlQueryHandler) AndLike(s Searchable, fieldName string, _ interface{}) string {
	if o.Error() {
		return ""
	}

	var fn Field
	fn, o.Err = s.CriteriaAlias(fieldName)
	if o.Error() {
		return ""
	}
	return fmt.Sprintf(" AND `%s`.`%s` like ?", fn.Table, fn.Name)
}

func (o *SqlQueryHandler) AndGreaterThan(s Searchable, fieldName string, _ interface{}) string {
	if o.Error() {
		return ""
	}

	var fn Field
	fn, o.Err = s.CriteriaAlias(fieldName)
	if o.Error() {
		return ""
	}
	return fmt.Sprintf("  AND `%s`.`%s` > ? ", fn.Table, fn.Name)
}

func (o *SqlQueryHandler) AndGreaterThanEqual(s Searchable, fieldName string, _ interface{}) string {
	if o.Error() {
		return ""
	}

	var fn Field
	fn, o.Err = s.CriteriaAlias(fieldName)
	if o.Error() {
		return ""
	}
	return fmt.Sprintf("  AND `%s`.`%s` >= ? ", fn.Table, fn.Name)
}

func (o *SqlQueryHandler) AndLessThan(s Searchable, fieldName string, _ interface{}) string {
	if o.Error() {
		return ""
	}

	var fn Field
	fn, o.Err = s.CriteriaAlias(fieldName)
	if o.Error() {
		return ""
	}
	return fmt.Sprintf("  AND `%s`.`%s` < ? ", fn.Table, fn.Name)
}

func (o *SqlQueryHandler) AndLessThanEqual(s Searchable, fieldName string, _ interface{}) string {
	if o.Error() {
		return ""
	}

	var fn Field
	fn, o.Err = s.CriteriaAlias(fieldName)
	if o.Error() {
		return ""
	}
	return fmt.Sprintf("  AND `%s`.`%s` <= ? ", fn.Table, fn.Name)
}

func (o *SqlQueryHandler) AndIsIn(s Searchable, fieldName string, rhs interface{}) string {
	if o.Error() {
		return ""
	}

	fn, err := s.CriteriaAlias(fieldName)
	if err != nil {
		o.Set(err)
		return ""
	}

	switch list := rhs.(type) {
	case []interface{}:
		var placeholders []string
		if len(list) == 0 {
			return ""
		}

		for _, _ = range list {
			placeholders = append(placeholders, "?")
		}

		return fmt.Sprintf("  AND `%s`.`%s` IN (%s) ", fn.Table, fn.Name, strings.Join(placeholders, ","))
	default:
		o.Set(fmt.Errorf("where clause operator IN not support rhs type %T, should be list", rhs))
		return ""
	}
}
