package entcache_test

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"go.uber.org/mock/gomock"

	"github.com/DeltaLaboratory/entcache"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
	"github.com/DATA-DOG/go-sqlmock"
	ruemock "github.com/redis/rueidis/mock"
)

func TestDriver_ContextLevel(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	drv := sql.OpenDB(dialect.MySQL, db)

	t.Run("One", func(t *testing.T) {
		drv := entcache.NewDriver(drv, entcache.ContextLevel())
		mock.ExpectQuery("SELECT id FROM users").
			WillReturnRows(
				sqlmock.NewRows([]string{"id"}).
					AddRow(1).
					AddRow(2).
					AddRow(3),
			)
		ctx := entcache.NewContext(context.Background())
		// Enable caching explicitly
		cacheCtx := entcache.Cache(ctx)
		expectQuery(cacheCtx, t, drv, "SELECT id FROM users", []any{int64(1), int64(2), int64(3)})
		expectQuery(cacheCtx, t, drv, "SELECT id FROM users", []any{int64(1), int64(2), int64(3)})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Multi", func(t *testing.T) {
		drv := entcache.NewDriver(drv, entcache.ContextLevel())
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		ctx1 := entcache.NewContext(context.Background())
		// Enable caching explicitly
		cacheCtx1 := entcache.Cache(ctx1)
		expectQuery(cacheCtx1, t, drv, "SELECT name FROM users", []any{"a8m"})
		ctx2 := entcache.NewContext(context.Background())
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		// Enable caching explicitly
		cacheCtx2 := entcache.Cache(ctx2)
		expectQuery(cacheCtx2, t, drv, "SELECT name FROM users", []any{"a8m"})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("TTL", func(t *testing.T) {
		drv := entcache.NewDriver(drv, entcache.ContextLevel(), entcache.TTL(-1))
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		ctx := entcache.NewContext(context.Background())
		// With cache being optional by default, we need to execute two separate queries
		expectQuery(ctx, t, drv, "SELECT name FROM users", []any{"a8m"})
		expectQuery(ctx, t, drv, "SELECT name FROM users", []any{"a8m"})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestDriver_Levels(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	drv := sql.OpenDB(dialect.Postgres, db)

	t.Run("One", func(t *testing.T) {
		drv := entcache.NewDriver(drv, entcache.TTL(time.Second))
		mock.ExpectQuery("SELECT age FROM users").
			WillReturnRows(
				sqlmock.NewRows([]string{"age"}).
					AddRow(20.1).
					AddRow(30.2).
					AddRow(40.5),
			)
		// Enable caching explicitly
		ctx := entcache.Cache(context.Background())
		expectQuery(ctx, t, drv, "SELECT age FROM users", []any{20.1, 30.2, 40.5})
		expectQuery(ctx, t, drv, "SELECT age FROM users", []any{20.1, 30.2, 40.5})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Multi", func(t *testing.T) {
		drv := entcache.NewDriver(
			drv,
			entcache.Levels(
				entcache.NewLRU(-1), // Nop.
				entcache.NewLRU(0),  // No limit.
			),
		)
		mock.ExpectQuery("SELECT age FROM users").
			WillReturnRows(
				sqlmock.NewRows([]string{"age"}).
					AddRow(20.1).
					AddRow(30.2).
					AddRow(40.5),
			)
		// Enable caching explicitly
		ctx := entcache.Cache(context.Background())
		expectQuery(ctx, t, drv, "SELECT age FROM users", []any{20.1, 30.2, 40.5})
		expectQuery(ctx, t, drv, "SELECT age FROM users", []any{20.1, 30.2, 40.5})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Redis", func(t *testing.T) {
		var (
			rdb = ruemock.NewClient(gomock.NewController(t))
			drv = entcache.NewDriver(
				drv,
				entcache.Levels(
					entcache.NewLRU(-1),
					entcache.NewRedis(rdb),
				),
				entcache.Hash(func(string, []any) (entcache.Key, error) {
					return 1, nil
				}),
			)
		)
		// Enable caching explicitly
		ctx := entcache.Cache(context.Background())

		rdb.EXPECT().Do(ctx, ruemock.Match("GET", "1")).Return(ruemock.Result(ruemock.RedisNil()))
		mock.ExpectQuery("SELECT active FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"active"}).AddRow(true).AddRow(false))

		buf, _ := entcache.Entry{Values: [][]driver.Value{{true}, {false}}}.MarshalBinary()
		rdb.EXPECT().Do(ctx, ruemock.Match("SET", "1", rueidis.BinaryString(buf), "EX", "0")).Return(ruemock.Result(ruemock.RedisNil()))
		expectQuery(ctx, t, drv, "SELECT active FROM users", []any{true, false})

		rdb.EXPECT().Do(ctx, ruemock.Match("GET", "1")).Return(ruemock.Result(ruemock.RedisString(rueidis.BinaryString(buf))))
		expectQuery(ctx, t, drv, "SELECT active FROM users", []any{true, false})

		expected := entcache.Stats{Gets: 2, Hits: 1}
		if s := drv.Stats(); s != expected {
			t.Errorf("unexpected stats: %v != %v", s, expected)
		}
	})
}

func TestDriver_ContextOptions(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	drv := sql.OpenDB(dialect.MySQL, db)

	t.Run("Skip", func(t *testing.T) {
		drv := entcache.NewDriver(drv)
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		ctx := context.Background()
		// First query without cache
		expectQuery(ctx, t, drv, "SELECT name FROM users", []any{"a8m"})
		// Second query without cache, should hit the database again
		expectQuery(ctx, t, drv, "SELECT name FROM users", []any{"a8m"})

		// Now try with cache enabled
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		cacheCtx := entcache.Cache(ctx)
		expectQuery(cacheCtx, t, drv, "SELECT name FROM users", []any{"a8m"})
		expectQuery(cacheCtx, t, drv, "SELECT name FROM users", []any{"a8m"})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Evict", func(t *testing.T) {
		drv := entcache.NewDriver(drv)
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		ctx := context.Background()
		cacheCtx := entcache.Cache(ctx)
		expectQuery(cacheCtx, t, drv, "SELECT name FROM users", []any{"a8m"})
		expectQuery(cacheCtx, t, drv, "SELECT name FROM users", []any{"a8m"})
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		evictCtx := entcache.Evict(ctx)
		expectQuery(evictCtx, t, drv, "SELECT name FROM users", []any{"a8m"})
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		expectQuery(ctx, t, drv, "SELECT name FROM users", []any{"a8m"})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("WithTTL", func(t *testing.T) {
		drv := entcache.NewDriver(drv)
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		ctx := context.Background()
		// Enable caching and set TTL
		ttlCtx := entcache.Cache(entcache.WithTTL(ctx, -1))
		expectQuery(ttlCtx, t, drv, "SELECT name FROM users", []any{"a8m"})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("WithKey", func(t *testing.T) {
		drv := entcache.NewDriver(drv)
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		ctx := context.Background()
		// Enable caching and set key
		keyCtx := entcache.Cache(entcache.WithKey(ctx, "cache-key"))
		expectQuery(keyCtx, t, drv, "SELECT name FROM users", []any{"a8m"})
		expectQuery(keyCtx, t, drv, "SELECT name FROM users", []any{"a8m"})
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		// Regular context without cache
		expectQuery(ctx, t, drv, "SELECT name FROM users", []any{"a8m"})
		if err := drv.Cache.Del(ctx, "cache-key"); err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		expectQuery(keyCtx, t, drv, "SELECT name FROM users", []any{"a8m"})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		expected := entcache.Stats{Gets: 3, Hits: 1}
		if s := drv.Stats(); s != expected {
			t.Errorf("unexpected stats: %v != %v", s, expected)
		}
	})
}

func TestDriver_SkipInsert(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	drv := entcache.NewDriver(sql.OpenDB(dialect.Postgres, db), entcache.Hash(func(string, []any) (entcache.Key, error) {
		t.Fatal("Driver.Query should not be called for INSERT statements")
		return nil, nil
	}))
	mock.ExpectQuery("INSERT INTO users DEFAULT VALUES RETURNING id").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	expectQuery(context.Background(), t, drv, "INSERT INTO users DEFAULT VALUES RETURNING id", []any{int64(1)})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	var expected entcache.Stats
	if s := drv.Stats(); s != expected {
		t.Errorf("unexpected stats: %v != %v", s, expected)
	}
}

func expectQuery(ctx context.Context, t *testing.T, drv dialect.Driver, query string, args []any) {
	rows := &sql.Rows{}
	if err := drv.Query(ctx, query, []any{}, rows); err != nil {
		t.Fatalf("unexpected query failure: %q: %v", query, err)
	}
	var dest []any
	for rows.Next() {
		var v any
		if err := rows.Scan(&v); err != nil {
			t.Fatal("unexpected Rows.Scan failure:", err)
		}
		dest = append(dest, v)
	}
	if len(dest) != len(args) {
		t.Fatalf("mismatch rows length: %d != %d", len(dest), len(args))
	}
	for i := range dest {
		if dest[i] != args[i] {
			t.Fatalf("mismatch values: %v %T != %v %T", dest[i], dest[i], args[i], args[i])
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
}
