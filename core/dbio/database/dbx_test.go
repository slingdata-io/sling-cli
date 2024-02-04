package database_test

import (
	"os"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/stretchr/testify/assert"
)

type User9 struct {
	database.ModelDbX
	Name      string    `json:"name" db:"name" gorm:"primaryKey"`
	Age       int       `json:"age" db:"age"`
	Mature    bool      `json:"mature" db:"mature"`
	Birthdate time.Time `json:"birthdate" db:"birthdate"`
	UpdatedDt time.Time `json:"updated_dt" db:"updated_dt" gorm:"autoUpdateTime"`
	CreatedDt time.Time `json:"created_dt" db:"created_dt" gorm:"autoCreateTime"`
}

func NewUser(u User9) *User9 {
	u.Ptr = &u
	return &u
}

func TestDbX(t *testing.T) {
	database.Debug = true
	conn, err := database.NewConn(os.Getenv("POSTGRES_URL"))
	if !assert.NoError(t, err) {
		return
	}

	_, err = conn.Exec("drop table if exists users9")
	if !assert.NoError(t, err) {
		return
	}
	_, err = conn.Exec(`
		create table users9 (
			name text,
			age int,
			mature boolean,
			birthdate timestamp,
			updated_dt timestamp,
			created_dt timestamp
		)
	`)
	if !assert.NoError(t, err) {
		return
	}

	// insert
	db := conn.Db()
	user := NewUser(User9{Name: "Fritz", Age: 35, Mature: true})
	err = user.Insert(db)
	if !assert.NoError(t, err) {
		g.LogError(err)
		return
	}

	// read
	user.Age = 6
	user.Mature = true
	err = user.Get(db)
	assert.NoError(t, err)
	assert.Equal(t, 35, user.Age)
	assert.Equal(t, true, user.Mature)

	// update
	user = NewUser(User9{Name: "Fritz Larco", Age: 36, Mature: true})
	err = user.Where("name = ?", "Fritz").Update(db, "name", "age")
	assert.NoError(t, err)

	// read
	user = NewUser(User9{})
	err = user.Get(db)
	assert.NoError(t, err)
	assert.Equal(t, 36, user.Age)
	assert.Equal(t, "Fritz Larco", user.Name)

	// upsert
	// read

	// delete
	err = user.Where("age = ?", 36).Delete(db)
	assert.NoError(t, err)

	// read
	user = NewUser(User9{})
	err = user.Get(db)
	assert.Error(t, err)

	// dbx := conn.DbX()

}

/*


type Users []User
type User {
	database.ModelDbX
	Name string
	Age int
}

func NewUser() *User {
	u := new(User)
	u.Ptr = u
	return u
}

func NewUsers() []*User {
	us := Users{}
	u.Ptr = u
	return u
}

fields := []string{} => ("Name", "Age")
where := []interface{}{} => ("name in (?, ?)", "Jason", "Tina")
limit := 10
values ;= map[string]interface{}{"name": "Fritz", "age": 37}

s.Config{
	Operation: s.INSERT,
	Fields: []strings{"Name", "Age"},
	Where: g.M()
}

single.Bind(rec, &single)
many.Bind(recs, &many)

db.Insert(&single, fields)
db.Get(&single, fields, where...)  // also use PK
db.Update(&single, fields, where...) // also use PK
db.Upsert(&single, fields, where...) // also use PK
db.Delete(&single, where...) // also use PK

db.Insert(&many, fields)
db.Get(&many, fields, where...)  // also use PK
db.Update(&many, fields, where...) // also use PK
db.Upsert(&many, fields, where...) // also use PK
db.Delete(&many, where...) // also use PK

single.Where("name in (?, ?)", "Jason", "Tina").Insert()
single.Insert(fields)
single.Get(fields, where...)
single.Update(fields, where...) // also use PK
single.Upsert(fields, where...) // also use PK
single.Delete(where...) // also use PK

many.Insert(fields)
many.Get(fields, where)
many.Update(fields, where...) // also use PK
many.Upsert(fields, where...) // also use PK
many.Delete(where...) // also use PK

users := []User{}
err = users.Get("name in (?, ?)", "Jason", "Tina")

user := User{}
err = user.Get("name = ?", "Jason")

err = user.Delete("name = ?", "Fritz")

err = user.Update("name = ?", "Fritz")

*/
