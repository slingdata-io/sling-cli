package database

import (
	"context"
	"reflect"
	"strings"

	"github.com/flarco/g"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cast"
)

// Debug prints queries when true
var Debug = false

// DbX is db express
type DbX struct {
	db          *sqlx.DB    `json:"-"`
	tx          Transaction `json:"-"`
	whereClause WhereClause `json:"-"`
}

// ModelDbX is the base for any SQL model
type ModelDbX struct {
	Ptr          interface{}            `json:"-"`
	RowsAffected int                    `json:"-"`
	rec          map[string]interface{} `json:"-"`
	selected     g.Strings              `json:"-"`
	whereClause  WhereClause            `json:"-"`
}

// WhereClause is the where clause
type WhereClause []interface{}

// Clause returns the string where clause
func (wc WhereClause) Clause() string {
	if len(wc) == 0 {
		return "1=1"
	}
	return cast.ToString(wc[0])
}

// Args returns the where clause arguments
func (wc WhereClause) Args() []interface{} {
	args := []interface{}{}
	if len(wc) < 2 {
		return args
	}
	for i, v := range wc {
		if i < 1 {
			continue
		}
		args = append(args, v)
	}
	return args
}

// placeholder returns the placeholder
func placeholder(db *sqlx.DB, i int) string {
	switch db.DriverName() {
	case "postgres":
		return g.F("$%d", i+1)
	default:
		return "?"
	}
}

// prep preps the query as needed
func prep(db *sqlx.DB, sql string) string {
	count := strings.Count(sql, "?")
	if count > 0 {
		for i := 0; i < count; i++ {
			sql = strings.Replace(sql, "?", placeholder(db, i), 1)
		}
	}
	if Debug {
		g.Debug(sql)
	}
	return sql
}

// Bind extracts values from provided echo context
func (m *ModelDbX) Bind(bindFunc func(p interface{}) error, objPtr interface{}) (err error) {
	//check if pointer
	if !g.IsPointer(objPtr) {
		return g.Error("`objPtr` pointer is invalid (nil or non-pointer)")
	}

	m.rec = map[string]interface{}{}
	if err = bindFunc(&m.rec); err != nil { // populate rec
		return g.Error(err, "Invalid request")
	}

	if m.Ptr == nil {
		m.Ptr = objPtr
	}
	err = g.Unmarshal(g.Marshal(m.rec), objPtr)
	if err != nil {
		return g.Error(err, "error doing unmarshal")
	}

	return nil
}

// Rec returns the record
func (m *ModelDbX) Rec() map[string]interface{} {
	return m.rec
}

// TableName returns the table name of the underlying pointer
func (m *ModelDbX) TableName(objPtr interface{}) string {
	if objPtr != nil {
		m.Ptr = objPtr
	}
	return "users9"
}

// Fields returns the model fields
func (m *ModelDbX) Fields() (fields []string) {
	if m.rec != nil && len(m.rec) > 0 {
		for k := range m.rec {
			fields = append(fields, k)
		}
		return
	}
	for _, field := range g.StructFields(m.Ptr) {
		if field.Field.Name == "SqlX" || field.JKey == "" || field.JKey == "-" {
			continue
		}
		fields = append(fields, field.JKey)
	}
	return
}

// Where adds a where clause
func (m *ModelDbX) Where(where ...interface{}) *ModelDbX {
	m.whereClause = WhereClause(where)
	return m
}

// Where adds a where clause
func (m *ModelDbX) Values(fields []string) (values []interface{}, err error) {
	values = make([]interface{}, len(fields))

	if m.rec == nil {
		m.rec = map[string]interface{}{}
	}

	for _, field := range g.StructFields(m.Ptr) {
		if field.Field.Name == "SqlX" || field.JKey == "" || field.JKey == "-" {
			continue
		}
		m.rec[field.JKey] = field.Value.Interface()
	}

	for i, f := range fields {
		v, ok := m.rec[f]
		if !ok {
			err = g.Error("did not find value for field: %s", f)
		}
		values[i] = v
	}

	return
}

// Insert inserts one records
func (m *ModelDbX) Insert(db *sqlx.DB, fields ...string) (err error) {
	defer m.Where() // reset where clause
	if db == nil {
		return g.Error("db pointer is nil")
	} else if !g.IsPointer(m.Ptr) {
		return g.Error("`m.Ptr` pointer is invalid (nil or non-pointer)")
	}

	if len(fields) == 0 {
		fields = m.Fields()
	}

	table := m.TableName(m.Ptr)
	values, err := m.Values(fields)
	if err != nil {
		return g.Error(err, "error making values")
	}
	placeholders := make(g.Strings, len(values))
	for i := range values {
		placeholders[i] = "?"
	}

	sql := g.F("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(fields, ", "), placeholders.Join(", "))
	sql = prep(db, sql)

	res, err := db.Exec(sql, values...)
	if err != nil {
		return g.Error(err, "Error inserting: %s\n%s", UID(m.Ptr), sql)
	}
	m.RowsAffected = cast.ToInt(res.RowsAffected)
	return
}

// Insert inserts one records
func (m *ModelDbX) Update(db *sqlx.DB, fields ...string) (err error) {
	defer m.Where() // reset where clause
	if db == nil {
		return g.Error("db pointer is nil")
	} else if !g.IsPointer(m.Ptr) {
		return g.Error("`m.Ptr` pointer is invalid (nil or non-pointer)")
	}

	if len(fields) == 0 {
		return g.Error("did not provide fields for update")
	}

	table := m.TableName(m.Ptr)
	if len(m.whereClause) == 0 {
		return g.Error("did not provide where clause for update")
	}

	values, err := m.Values(fields)
	if err != nil {
		return g.Error(err, "error making values")
	}

	setValues := make(g.Strings, len(values))
	for i := range values {
		setValues[i] = g.F("%s = ?", fields[i])
	}

	sql := g.F("UPDATE %s SET %s WHERE %s", table, strings.Join(setValues, ", "), m.whereClause.Clause())
	sql = prep(db, sql)

	res, err := db.Exec(sql, append(values, m.whereClause.Args()...)...)
	if err != nil {
		return g.Error(err, "Error updating: %s\n%s", UID(m.Ptr), sql)
	}
	m.RowsAffected = cast.ToInt(res.RowsAffected)
	return
}

// Get get the first record
func (m *ModelDbX) Get(db *sqlx.DB, fields ...string) (err error) {
	defer m.Where() // reset where clause

	if db == nil {
		return g.Error("db pointer is nil")
	} else if !g.IsPointer(m.Ptr) {
		return g.Error("`m.Ptr` pointer is invalid (nil or non-pointer)")
	}

	if len(fields) == 0 {
		fields = g.Strings{"*"}
	}

	table := m.TableName(m.Ptr)
	sql := g.F("SELECT %s FROM %s WHERE %s", strings.Join(fields, ", "), table, m.whereClause.Clause())
	sql = prep(db, sql)

	rows, err := db.Queryx(sql, m.whereClause.Args()...)
	if err != nil {
		g.Error(err, "Error getting record: %s\n%s", UID(m.Ptr), sql)
	} else if !rows.Next() {
		return g.Error("no record found for: ", UID(m.Ptr))
	}

	err = rows.StructScan(m.Ptr)
	if err != nil {
		g.Error(err, "Error scanning record: %s", UID(m.Ptr))
	}
	m.RowsAffected = 1

	if m.rec == nil {
		m.rec = map[string]interface{}{}
	}

	for _, field := range g.StructFields(m.Ptr) {
		if field.Field.Name == "SqlX" || field.JKey == "" || field.JKey == "-" {
			continue
		}
		m.rec[field.JKey] = field.Value.Interface()
	}
	return
}

// Select returns multiple records
func (m *ModelDbX) Select(db *sqlx.DB, objPtr interface{}, fields ...string) (err error) {
	defer m.Where() // reset where clause

	if db == nil {
		return g.Error("db pointer is nil")
	} else if !g.IsPointer(objPtr) {
		return g.Error("`objPtr` pointer is invalid (nil or non-pointer)")
	}

	if len(fields) == 0 {
		fields = g.Strings{"*"}
	}

	table := m.TableName(m.Ptr)
	sql := g.F("SELECT %s FROM %s WHERE %s", strings.Join(fields, ", "), table, m.whereClause.Clause())
	sql = prep(db, sql)
	err = db.Select(objPtr, sql, m.whereClause.Args()...)
	if err != nil {
		g.Error(err, "Error getting records: %s\n%s", UID(m), sql)
	}
	return
}

// Delete deletes a record
func (m *ModelDbX) Delete(db *sqlx.DB) (err error) {
	defer m.Where() // reset where clause

	if !g.IsPointer(m.Ptr) {
		return g.Error("model pointer is invalid (nil or non-pointer)")
	} else if db == nil {
		return g.Error("db pointer is nil")
	}

	table := m.TableName(m.Ptr)
	if len(m.whereClause) == 0 {
		return g.Error("did not provide where clause for delete")
	}
	sql := g.F("DELETE FROM %s WHERE %s", table, m.whereClause.Clause())
	sql = prep(db, sql)
	res, err := db.Exec(sql, m.whereClause.Args()...)
	if err != nil {
		return g.Error(err, "Error deleting: %s\n%s", UID(m.Ptr), sql)
	}
	m.RowsAffected = cast.ToInt(res.RowsAffected)
	return
}

// UID returns a unique ID for that object
func UID(obj interface{}) string {
	var t reflect.Type
	value := reflect.ValueOf(obj)
	if value.Kind() == reflect.Ptr {
		t = reflect.Indirect(value).Type()
	} else {
		t = reflect.TypeOf(obj)
	}

	uidArr := []string{strings.ToLower(t.Name())}
	for _, field := range g.StructFields(obj) {
		f := field.Field
		if f.Tag.Get("gorm") == "primaryKey" || f.Tag.Get("gorm") == "updateKey" {
			val := field.Value.Interface()
			if field.Value.Kind() == reflect.Ptr && !field.Value.IsZero() {
				val = field.Value.Elem().Interface()
			}
			uidArr = append(uidArr, cast.ToString(val))
		}
	}

	return strings.Join(uidArr, "-")
}

// PK returns the primary keys of a model
func PK(obj interface{}) (pk []string) {
	var t reflect.Type
	value := reflect.ValueOf(obj)
	if value.Kind() == reflect.Ptr {
		t = reflect.Indirect(value).Type()
	} else {
		t = reflect.TypeOf(obj)
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Tag.Get("gorm") == "primaryKey" || field.Tag.Get("gorm") == "updateKey" || field.Tag.Get("dbx") == "primaryKey" {
			pk = append(pk, field.Tag.Get("json"))
		}
	}
	return pk
}

// Where adds a where clause
func (x *DbX) Where(where ...interface{}) *DbX {
	x.whereClause = WhereClause(where)
	return x
}

// Insert inserts object or slice
func (x *DbX) Insert(o interface{}, fields ...string) (err error) {
	return
}

// Get retrieves object
func (x *DbX) Get(o interface{}, fields ...string) (err error) {
	defer x.Where() // reset where clause
	if !g.IsPointer(o) {
		return g.Error("`o` pointer is invalid (nil or non-pointer)")
	}

	if len(fields) == 0 {
		fields = g.Strings{"*"}
	}

	table := x.TableName(o)
	sql := g.F("SELECT %s FROM %s WHERE %s", strings.Join(fields, ", "), table, x.whereClause.Clause())
	sql = prep(x.db, sql)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rows, err := x.db.QueryxContext(ctx, sql, x.whereClause.Args()...)
	if err != nil {
		g.Error(err, "Error getting record: %s\n%s", UID(o), sql)
	} else if !rows.Next() {
		return g.Error("no record found for: ", UID(o))
	}

	err = rows.StructScan(o)
	if err != nil {
		g.Error(err, "Error scanning record: %s", UID(o))
	}
	return
}

// Select retrieves objects
func (x *DbX) Select(o interface{}, fields ...string) (err error) {
	defer x.Where() // reset where clause
	if !g.IsSlice(o) {
		return g.Error("`o` not a slice")
	}

	if len(fields) == 0 {
		fields = g.Strings{"*"}
	}

	table := x.TableName(o)
	sql := g.F("SELECT %s FROM %s WHERE %s", strings.Join(fields, ", "), table, x.whereClause.Clause())
	sql = prep(x.db, sql)
	err = x.db.Select(o, sql, x.whereClause.Args()...)
	if err != nil {
		g.Error(err, "Error getting records: %s\n%s", UID(o), sql)
	}
	return
}

// Update updates from object or slice
func (x *DbX) Update(o interface{}, fields ...string) (cnt int, err error) {
	defer x.Where() // reset where clause
	return
}

// Upsert upserts from object or slice
func (x *DbX) Upsert(o interface{}, fields ...string) (cnt int, err error) {
	defer x.Where() // reset where clause
	return
}

// Delete deletes from object or slice
func (x *DbX) Delete(o interface{}) (cnt int, err error) {
	defer x.Where() // reset where clause
	return
}

// TableName returns the table name of object or slice
func (x *DbX) TableName(o interface{}) (name string) {
	return
}

type User struct {
	ModelDbX
	Name string
	Age  int
}

func NewUser() *User {
	v := new(User)
	v.Ptr = v
	return v
}
