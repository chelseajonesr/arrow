package parquet_test

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
)

func TestOpen(t *testing.T) {
	// The file panic.parquet was generated as follows in pyspark:
	//
	// schema = StructType([StructField('add', StructType([StructField('tags', MapType(StringType(), StringType(), True), True)]), True)])
	// data = [{'add':{'tags':None}}]
	// spark.createDataFrame(data=data, schema=schema).repartition(1).write.parquet('/tmp/test')
	//
	// The following data will also panic:
	// data = [{'add':None}]
	// data = [{'add':{'tags':None}}, {'add':{}}]
	// data = [{'add':{'tags':None}}, {'add':None}]
	// data = [{'add':{'tags':None}}, {'add':{'tags':{}}}]
	//
	// But this does not panic:
	// data = [{'add':{'tags':None}}, {'add':{'tags':{'a':'b'}}}]
	p := "panic.parquet"

	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	parquetReader, err := file.NewParquetReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer parquetReader.Close()
	fileReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: 1, Parallel: false}, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}

	tbl, err := fileReader.ReadTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Release()
}
