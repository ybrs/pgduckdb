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

	// Scan from SQLite database.
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
			fmt.Println("msg", msg)
			query := msg.(*pgproto3.Query)

			_, err = p.db.Prepare(query.String)
			if err != nil {
				writeMessages(p.conn,
					&pgproto3.ErrorResponse{Message: err.Error()},
					&pgproto3.ReadyForQuery{TxStatus: 'I'},
				)
				continue
			}

			rows, err := p.db.QueryContext(p.ctx, query.String)
			if err != nil {
				writeMessages(p.conn,
					&pgproto3.ErrorResponse{Message: err.Error()},
					&pgproto3.ReadyForQuery{TxStatus: 'I'},
				)
				continue
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
			if err != nil {
				return fmt.Errorf("error writing query response: %w", err)
			}
		case *pgproto3.Terminate:
			return nil
		default:
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
