package gosstables

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/magiconair/properties"
	"github.com/thomasjungblut/go-sstables/simpledb"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// properties
const (
	gosstablesPath          = "gosstables.path"
	memstoreSizeBytes       = "gosstables.memstoreSizeBytes"
	enableAsyncWal          = "gosstables.asyncWal"
	disableCompaction       = "gosstables.disableCompactions"
	compactionFileThreshold = "gosstables.compactionFileThreshold"
	compactionMaxSizeBytes  = "gosstables.compactionMaxSizeBytes"
)

type gosstablesCreator struct{}

type gosstablesDb struct {
	p  *properties.Properties
	db *simpledb.DB
}

func init() {
	ycsb.RegisterDBCreator("gosstables", gosstablesCreator{})
}

func (c gosstablesCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	path := p.GetString(gosstablesPath, "/tmp/gosstables-simpledb")

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		log.Printf("deleted datadir at %s", path)
		err := os.RemoveAll(path)
		if err != nil {
			return nil, err
		}
	}

	err := os.MkdirAll(path, 0755)
	if err != nil {
		return nil, err
	}

	var opts []simpledb.ExtraOption
	pm := p.Map()
	if _, ok := pm[memstoreSizeBytes]; ok {
		opts = append(opts, simpledb.MemstoreSizeBytes(p.MustGetUint64(memstoreSizeBytes)))
	}

	if _, ok := pm[enableAsyncWal]; ok {
		opts = append(opts, simpledb.EnableAsyncWAL())
	}

	if _, ok := pm[disableCompaction]; ok {
		opts = append(opts, simpledb.DisableCompactions())
	}

	if _, ok := pm[compactionFileThreshold]; ok {
		opts = append(opts, simpledb.CompactionFileThreshold(p.MustGetInt(compactionFileThreshold)))
	}

	if _, ok := pm[compactionMaxSizeBytes]; ok {
		opts = append(opts, simpledb.CompactionMaxSizeBytes(p.MustGetUint64(compactionMaxSizeBytes)))
	}

	db, err := simpledb.NewSimpleDB(path, opts...)
	if err != nil {
		return nil, err
	}

	err = db.Open()
	if err != nil {
		return nil, err
	}

	return &gosstablesDb{
		p:  p,
		db: db,
	}, nil
}

func (db *gosstablesDb) Close() error {
	return db.db.Close()
}

func (db *gosstablesDb) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *gosstablesDb) CleanupThread(_ context.Context) {
}

func getRowKey(table string, key string) string {
	return fmt.Sprintf("%s:%s", table, key)
}

func (db *gosstablesDb) Read(ctx context.Context, table string, key string, _ []string) (map[string][]byte, error) {
	rkey := getRowKey(table, key)
	value, err := db.db.Get(rkey)
	if err != nil {
		return nil, err
	}

	var r map[string][]byte
	err = json.NewDecoder(strings.NewReader(value)).Decode(&r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (db *gosstablesDb) Scan(ctx context.Context, table string, startKey string, count int, _ []string) ([]map[string][]byte, error) {
	// TODO
	return nil, nil
}

func (db *gosstablesDb) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	rkey := getRowKey(table, key)
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	err = db.db.Put(rkey, string(data))
	if err != nil {
		return err
	}

	return nil
}

func (db *gosstablesDb) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.Update(ctx, table, key, values)
}

func (db *gosstablesDb) Delete(ctx context.Context, table string, key string) error {
	err := db.db.Delete(getRowKey(table, key))
	if err != nil {
		return err
	}
	return nil
}
