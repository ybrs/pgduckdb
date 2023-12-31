package main

import "C"
import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	_ "github.com/marcboeker/go-duckdb"
	"io"
	"net"
	"strings"
)

// Postgres settings.
const (
	ServerVersion = "13.0.0"
)

type DuckDbBackend struct {
	backend *pgproto3.Backend
	conn    net.Conn
	db      *sql.DB
	ctx     context.Context
	cancel  func()
}

func NewDuckDbBackend(conn net.Conn, db *sql.DB, ctx context.Context, cancel func()) *DuckDbBackend {
	backend := pgproto3.NewBackend(conn, conn)

	connHandler := &DuckDbBackend{
		backend: backend,
		conn:    conn,
		db:      db,
		ctx:     ctx,
		cancel:  cancel,
	}

	return connHandler
}

func writeMessages(w io.Writer, msgs ...pgproto3.Message) error {
	var buf []byte
	for _, msg := range msgs {
		buf = msg.Encode(buf)
	}
	_, err := w.Write(buf)
	return err
}

var duckdbTypeMap = map[string]int{
	"INVALID":      pgtype.UnknownOID, // No direct equivalent
	"BOOLEAN":      pgtype.BoolOID,
	"TINYINT":      pgtype.Int2OID, // Best guess, as there's no tinyint in PostgreSQL
	"SMALLINT":     pgtype.Int2OID,
	"INTEGER":      pgtype.Int4OID,
	"BIGINT":       pgtype.Int8OID,
	"UTINYINT":     pgtype.Int2OID, // Best guess
	"USMALLINT":    pgtype.Int4OID, // Best guess
	"UINTEGER":     pgtype.Int8OID, // Best guess
	"UBIGINT":      pgtype.Int8OID, // Best guess
	"FLOAT":        pgtype.Float4OID,
	"DOUBLE":       pgtype.Float8OID,
	"TIMESTAMP":    pgtype.TimestampOID,
	"DATE":         pgtype.DateOID,
	"TIME":         pgtype.TimeOID,
	"INTERVAL":     pgtype.IntervalOID,
	"HUGEINT":      pgtype.NumericOID, // Best guess
	"VARCHAR":      pgtype.VarcharOID,
	"BLOB":         pgtype.ByteaOID,
	"DECIMAL":      pgtype.NumericOID,
	"TIMESTAMP_S":  pgtype.TimestampOID, // Assuming standard timestamp
	"TIMESTAMP_MS": pgtype.TimestampOID, // Assuming standard timestamp
	"TIMESTAMP_NS": pgtype.TimestampOID, // Assuming standard timestamp
	"ENUM":         pgtype.TextOID,      // No direct equivalent
	"LIST":         pgtype.JSONOID,      // Best guess
	"STRUCT":       pgtype.RecordOID,    // Best guess
	"MAP":          pgtype.JSONOID,      // Best guess
	"UUID":         pgtype.UUIDOID,
}

// duckdbTypeToPgType converts DuckDB type names to corresponding PostgreSQL type
// OIDs.
// See duckdb.rows.typeName
func duckdbTypeToPgType(typeName string) int {

	pgType, ok := duckdbTypeMap[typeName]
	if !ok {
		return pgtype.TextOID // Default to text if no match is found
	}
	return pgType
}

func toRowDescription(cols []*sql.ColumnType) *pgproto3.RowDescription {
	var desc pgproto3.RowDescription

	for _, col := range cols {
		desc.Fields = append(desc.Fields, pgproto3.FieldDescription{
			Name:                 []byte(col.Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          pgtype.TextOID,
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		})
	}
	return &desc
}

func scanRow(rows *sql.Rows, cols []*sql.ColumnType) (*pgproto3.DataRow, error) {
	refs := make([]interface{}, len(cols))
	values := make([]interface{}, len(cols))
	for i := range refs {
		refs[i] = &values[i]
	}

	// Scan from Duckdb database.
	if err := rows.Scan(refs...); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}

	// Convert to TEXT values to return over Postgres wire protocol.
	row := pgproto3.DataRow{Values: make([][]byte, len(values))}
	for i := range values {
		row.Values[i] = []byte(fmt.Sprint(values[i]))
	}
	return &row, nil
}

func (p *DuckDbBackend) HandleQuery(query string) error {
	_, err := p.db.Prepare(query)
	if err != nil {
		fmt.Println("coulnt handle query", query)
		writeMessages(p.conn,
			&pgproto3.ErrorResponse{Message: err.Error()},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		)
		return nil
	}

	rows, err := p.db.QueryContext(p.ctx, query)
	if err != nil {
		fmt.Println("couldnt handle query 2", query)
		writeMessages(p.conn,
			&pgproto3.ErrorResponse{Message: err.Error()},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		)
		return nil
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("column types: %w", err)
	}
	buf := toRowDescription(cols).Encode(nil)

	// Iterate over each row and encode it to the wire protocol.
	for rows.Next() {
		row, err := scanRow(rows, cols)
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		buf = row.Encode(buf)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}
	buf = (&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}).Encode(buf)
	buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	_, err = p.conn.Write(buf)
	//fmt.Println("sending buf", buf)
	if err != nil {
		return fmt.Errorf("error writing query response: %w", err)
	}

	return nil
}

// CREATE OR REPLACE FUNCTION _pg_expandarray(anyarray) AS (SELECT [unnest(anyarray), generate_subscripts(anyarray, 1)]);
// CREATE OR REPLACE FUNCTION array_upper(anyarray, f) AS len(anyarray) + 1;
func rewriteQuery(query string) string {
	// poor mans attempt to pass parse commands
	// make pycharm/goland/jetbrains ide database tool work
	// ideally we should use something like https://pkg.go.dev/github.com/dallen66/pg_query_go/v2#section-readme
	// to properly map things (or have a pgcatalog database??)
	query = strings.ReplaceAll(query, "pgcatalog.current_database(", "current_database(")
	query = strings.ReplaceAll(query, "pg_catalog.current_schema(", "current_schema(")
	query = strings.ReplaceAll(query, "pg_catalog.", "")
	query = strings.ReplaceAll(query, "::regclass", "::oid")
	query = strings.ReplaceAll(query, "SHOW TRANSACTION ISOLATION LEVEL", "select 'read committed'")
	query = strings.ReplaceAll(query, ` trim(both '"' from pg_get_indexdef(tmp.CI_OID, tmp.ORDINAL_POSITION, false)) `, `'' `)
	query = strings.ReplaceAll(query, `(information_schema._pg_expandarray(i.indkey)).n `, `1`)
	query = strings.ReplaceAll(query, `information_schema._pg_expandarray(`, `_pg_expandarray( `)
	query = strings.ReplaceAll(query, ` (result.KEYS).x `, ` result.KEYS[0] `)
	query = strings.ReplaceAll(query, `::regproc`, ``)
	return query
}

func (p *DuckDbBackend) Run() error {
	defer p.Close()

	err := p.handleStartup()
	if err != nil {
		return err
	}

	for {
		msg, err := p.backend.Receive()
		if err != nil {
			return fmt.Errorf("error receiving message: %w", err)
		}

		switch msg.(type) {
		case *pgproto3.Query:
			//fmt.Println("msg", msg)
			query := msg.(*pgproto3.Query)
			p.HandleQuery(query.String)
		case *pgproto3.Terminate:
			return nil

		case *pgproto3.Parse:
			// For now we simply ignore parse messages
			// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
			q := msg.(*pgproto3.Parse)
			//fmt.Println("parse >>>", q)
			query := q.Query
			if strings.HasPrefix(q.Query, "SET ") ||
				strings.Contains(q.Query, "proargmodes") ||
				strings.Contains(q.Query, "prokind") ||
				strings.Contains(q.Query, "from pg_catalog.pg_locks") {
				//fmt.Println("skipped query returning ok")
				buf := (&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}).Encode(nil)
				buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
				//fmt.Println("parse complete write buf")
				_, err = p.conn.Write(buf)
				if err != nil {
					return fmt.Errorf("error writing query response: %w", err)
				}
				continue

			}
			//fmt.Println("run parse query", query)
			if query == "" {
				buf := (&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}).Encode(nil)
				buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
				buf = (&pgproto3.ParseComplete{}).Encode(buf)
				//fmt.Println("parse complete write buf")
				_, err = p.conn.Write(buf)
				if err != nil {
					return fmt.Errorf("error writing query response: %w", err)
				}

				continue
			}

			query = rewriteQuery(query)
			//fmt.Println("running rewritten >", query)
			p.HandleQuery(query)
			//buf := (&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}).Encode(nil)
			//buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
			//fmt.Println("parse complete write buf")
			//_, err = p.conn.Write(buf)
			//if err != nil {
			//	return fmt.Errorf("error writing query response: %w", err)
			//}

		case *pgproto3.Bind:
			continue
		case *pgproto3.Execute:
			continue
		case *pgproto3.Sync:
			continue
		case *pgproto3.Describe:
			continue
		default:
			print("coming to default ??", msg)
			return fmt.Errorf("received message other than Query from client: %#v", msg)
		}
	}
}

func (p *DuckDbBackend) handleStartup() error {
	startupMessage, err := p.backend.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("error receiving startup message: %w", err)
	}

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
		buf = (&pgproto3.ParameterStatus{Name: "server_version", Value: ServerVersion}).Encode(buf)
		buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		_, err = p.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("error sending ready for query: %w", err)
		}
	case *pgproto3.SSLRequest:
		_, err = p.conn.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("error sending deny SSL request: %w", err)
		}
		return p.handleStartup()
	default:
		return fmt.Errorf("unknown startup message: %#v", startupMessage)
	}

	return nil
}

func (p *DuckDbBackend) Close() error {
	return p.conn.Close()
}
