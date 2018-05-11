package canal

import (
	"fmt"

	"github.com/juju/errors"
	"go-mysql/schema"
)

const (
	UpdateAction = "update"
	InsertAction = "insert"
	DeleteAction = "delete"
)

type RowsEvent struct {
	Table  *schema.Table `json:"table"`
	Action string `json:"action"`
	// changed row list
	// binlog has three update event version, v0, v1 and v2.
	// for v1 and v2, the rows number must be even.
	// Two rows for one event, format is [before update row, after update row]
	// for update v0, only one row for a event, and we don't support this version.

	// 这些都应该不可变，不传递指针
	Rows []Row `json:"rows"`
}


type Row struct {
	BeforeColumns []schema.TableColumn `json:"before"`
	AfterColumns  []schema.TableColumn `json:"after"`
}


func newRowsEvent(table *schema.Table, action string, rows [][]interface{}) *RowsEvent {
	e := new(RowsEvent)

	e.Table = table
	e.Action = action
	e.Rows = newRows(table, action, rows)

	return e
}

func newRows(table *schema.Table,  action string, originRows [][]interface{}) (rows []Row) {
	rowsLen := len(originRows)
	switch action {
	case DeleteAction:
		rows = make([]Row, 0, rowsLen)
		for _, originRow := range originRows {
			row := Row{}
			row.BeforeColumns = make([]schema.TableColumn, 0, len(originRow))
			for i, r := range originRow {
				row.BeforeColumns = append(row.BeforeColumns, table.Columns[i].GenerateNewColumn(r))
			}
			rows = append(rows, row)
		}
	case InsertAction:
		rows = make([]Row, 0, rowsLen)
		for _, originRow := range originRows {
			row := Row{}
			row.AfterColumns = make([]schema.TableColumn, 0, len(originRow))
			for i, r := range originRow {
				row.AfterColumns = append(row.AfterColumns, table.Columns[i].GenerateNewColumn(r))
			}
			rows = append(rows, row)
		}
	case UpdateAction:
		rows = make([]Row, 0, rowsLen / 2)
		for i := 0; i < rowsLen; i += 2 {
			row := Row{}
			originRow := originRows[i]
			row.BeforeColumns = make([]schema.TableColumn, 0, len(originRow))
			row.AfterColumns = make([]schema.TableColumn, 0, len(originRow))

			for i, r := range originRow {
				row.BeforeColumns = append(row.BeforeColumns, table.Columns[i].GenerateNewColumn(r))
			}
			originRow = originRows[i + 1]
			for i, r := range originRow {
				row.AfterColumns = append(row.AfterColumns, table.Columns[i].GenerateNewColumn(r))
			}
			rows = append(rows, row)
		}
	default:
		panic(errors.New("error row event"))
	}
	return
}

func (row Row) String() string {
	return fmt.Sprintf("Before [%v] After [%v]", row.BeforeColumns, row.AfterColumns)
}

// Get primary keys in one row for a table, a table may use multi fields as the PK
func GetPKValues(table *schema.Table, row []interface{}) ([]interface{}, error) {
	indexes := table.PKColumns
	if len(indexes) == 0 {
		return nil, errors.Errorf("table %s has no PK", table)
	} else if len(table.Columns) != len(row) {
		return nil, errors.Errorf("table %s has %d columns, but row data %v len is %d", table,
			len(table.Columns), row, len(row))
	}

	values := make([]interface{}, 0, len(indexes))

	for _, index := range indexes {
		values = append(values, row[index])
	}

	return values, nil
}

// Get term column's value
func GetColumnValue(table *schema.Table, column string, row []interface{}) (interface{}, error) {
	index := table.FindColumn(column)
	if index == -1 {
		return nil, errors.Errorf("table %s has no column name %s", table, column)
	}

	return row[index], nil
}

// String implements fmt.Stringer interface.
func (r *RowsEvent) String() string {
	return fmt.Sprintf("%s %s %v", r.Action, r.Table, r.Rows)
}
