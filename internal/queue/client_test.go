package queue

import "testing"

func TestNewClient(t *testing.T) {
	cases := []struct {
		addr     string
		wantAddr string
		user     string
		pass     string
		db       int
		tls      bool
	}{
		{addr: "localhost:6379", wantAddr: "localhost:6379"},
		{addr: "redis://cache.internal:6380/3", wantAddr: "cache.internal:6380", db: 3},
		{addr: "redis://default:s3cret@cache.internal:6379/0", wantAddr: "cache.internal:6379", user: "default", pass: "s3cret"},
		{addr: "rediss://:s3cret@managed.example.com:6380", wantAddr: "managed.example.com:6380", pass: "s3cret", tls: true},
	}
	for _, c := range cases {
		rdb, err := NewClient(c.addr)
		if err != nil {
			t.Fatalf("%s: %v", c.addr, err)
		}
		o := rdb.Options()
		if o.Addr != c.wantAddr || o.Username != c.user || o.Password != c.pass || o.DB != c.db || (o.TLSConfig != nil) != c.tls {
			t.Errorf("%s: got addr=%s user=%q pass=%q db=%d tls=%v", c.addr, o.Addr, o.Username, o.Password, o.DB, o.TLSConfig != nil)
		}
		rdb.Close()
	}

	if _, err := NewClient("redis://host:6379/notanumber"); err == nil {
		t.Error("expected an error for an invalid database number")
	}
}
