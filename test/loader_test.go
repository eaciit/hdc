package loader_test

import(
	"github.com/eaciit/toolkit"
	"github.com/eaciit/hdc"
	"github.com/eaciit/hdc/hive"
	"testing"
)

func fatalCheck(t *testing.T, what string, e error){
	if e!=nil {
		t.Fatalf("%s: %s", what, e.Error())
	}
}

func TestLoadFile(t *testing.T){
	fp := "path-to-file"

	h := hive.HiveConfig(...)
	e := hdc.LoadFile(h, fp, "table", true)
	fatalCheck(t, "LoadFile", e)

	q := "select count(*) recordCount from table"
	var ms []toolkit.M
	e = h.Populate(q, &ms)
	fatalCheck(t, "Populate", e)

	if ms[0].GetInt("recordCount", 0)==0 {
		t.Fatalf("No data imported")
	}
}
