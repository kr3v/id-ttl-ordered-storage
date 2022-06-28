package id_ttl_ordered_storage

import (
	"log"
	"os"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

const dbPath = "./benchmark.db"

var (
	content1 = []byte("super1dupercontentsuperdupercontent3contentsuperdupercontentsu1perdupercontentsuperdupercontent")
	content2 = []byte("superduper2contentcontentcontentsuperduperc2ontentsuperdupercontentsuperdupercontent")
	content3 = []byte("superdupercontentdupercontentcontentsupe3rdupercontentsuperdupercontentsuperdupe11rcontent")

	contents = [][]byte{content1, content2, content3}
)

func BenchmarkDB_Put(b *testing.B) {
	now := strings.ReplaceAll(time.Now().Format(time.RFC3339), ":", "_")
	mem, err := os.Create("./heap/pprof." + now)
	if err != nil {
		b.Fatal(err)
	}
	defer mem.Close()
	cpu, err := os.Create("./cpu/pprof." + now)
	if err != nil {
		b.Fatal(err)
	}
	defer cpu.Close()
	if err := pprof.StartCPUProfile(cpu); err != nil {
		b.Fatal(err)
	}
	defer pprof.StopCPUProfile()

	defer os.RemoveAll(dbPath)

	db, err := NewDB(Options{
		MaxBufferSize: 8 * 1024 * 1024,
		TTL:           1 * time.Minute,
		Path:          dbPath,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Put(content1)
		if err != nil {
			b.Fatal(err)
		}
		_, err = db.Put(content2)
		if err != nil {
			b.Fatal(err)
		}
		_, err = db.Put(content3)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	if err := pprof.WriteHeapProfile(mem); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkDB_Get(b *testing.B) {
	//now := strings.ReplaceAll(time.Now().Format(time.RFC3339), ":", "_")
	//mem, err := os.Create("./heap/pprof." + now)
	//if err != nil {
	//	b.Fatal(err)
	//}
	//defer mem.Close()
	//cpu, err := os.Create("./cpu/pprof." + now)
	//if err != nil {
	//	b.Fatal(err)
	//}
	//defer cpu.Close()
	//if err := pprof.StartCPUProfile(cpu); err != nil {
	//	b.Fatal(err)
	//}
	//defer pprof.StopCPUProfile()

	db, err := NewDB(Options{
		MaxBufferSize: 16 * 1024,
		TTL:           1 * time.Minute,
		Path:          dbPath,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dbPath)

	type id struct {
		id ID
		k  int
	}

	var ids []id

	for i := 0; i < 1*1024; i++ {
		id1, err := db.Put(content1)
		if err != nil {
			b.Fatal(err)
		}
		id2, err := db.Put(content2)
		if err != nil {
			b.Fatal(err)
		}
		id3, err := db.Put(content3)
		if err != nil {
			b.Fatal(err)
		}
		if i%99 == 0 {
			ids = append(ids, id{id1, 1})
		}
		if i%98 == 0 {
			ids = append(ids, id{id2, 2})
		}
		if i%97 == 0 {
			ids = append(ids, id{id3, 3})
		}
	}

	var countersT [3]int
	var countersF [3]int

	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for _, id := range ids {
			v, err := db.Get(id.id)
			if err != nil {
				b.Fatal(err)
			}
			if len(v) == len(contents[id.k-1]) {
				countersT[id.k-1]++
			} else {
				countersF[id.k-1]++
			}
		}
	}
	b.StopTimer()

	log.Println(countersT)
	log.Println(countersF)

	//if err := pprof.WriteHeapProfile(mem); err != nil {
	//	b.Fatal(err)
	//}
}

func BenchmarkDB_GetB(b *testing.B) {
	//now := strings.ReplaceAll(time.Now().Format(time.RFC3339), ":", "_")
	//mem, err := os.Create("./heap/pprof." + now)
	//if err != nil {
	//	b.Fatal(err)
	//}
	//defer mem.Close()
	//cpu, err := os.Create("./cpu/pprof." + now)
	//if err != nil {
	//	b.Fatal(err)
	//}
	//defer cpu.Close()
	//if err := pprof.StartCPUProfile(cpu); err != nil {
	//	b.Fatal(err)
	//}
	//defer pprof.StopCPUProfile()
	//defer func() {
	//	if err := pprof.WriteHeapProfile(mem); err != nil {
	//		b.Fatal(err)
	//	}
	//}()

	db, err := NewDB(Options{
		MaxBufferSize: 16 * 1024,
		TTL:           1 * time.Minute,
		Path:          dbPath,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dbPath)

	type id struct {
		id ID
		k  int
	}

	var ids []id

	for i := 0; i < 1*1024; i++ {
		id1, err := db.Put(content1)
		if err != nil {
			b.Fatal(err)
		}
		id2, err := db.Put(content2)
		if err != nil {
			b.Fatal(err)
		}
		id3, err := db.Put(content3)
		if err != nil {
			b.Fatal(err)
		}
		if i%99 == 0 {
			ids = append(ids, id{id1, 1})
		}
		if i%98 == 0 {
			ids = append(ids, id{id2, 2})
		}
		if i%97 == 0 {
			ids = append(ids, id{id3, 3})
		}
	}

	var countersT [3]int
	var countersF [3]int

	var v []byte

	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for _, id := range ids {
			v, err = db.GetB(id.id, v)
			if err != nil {
				b.Fatal(err)
			}
			c := contents[id.k-1]
			if len(v) == len(c) && v[0] == c[0] && v[len(c)/2] == c[len(v)/2] {
				countersT[id.k-1]++
			} else {
				countersF[id.k-1]++
			}
		}
	}
	b.StopTimer()

	log.Println("countersT", countersT)
	log.Println("countersF", countersF)
}

func BenchmarkDB_GetMany(b *testing.B) {
	//now := strings.ReplaceAll(time.Now().Format(time.RFC3339), ":", "_")
	//mem, err := os.Create("./heap/pprof." + now)
	//if err != nil {
	//	b.Fatal(err)
	//}
	//defer mem.Close()
	//cpu, err := os.Create("./cpu/pprof." + now)
	//if err != nil {
	//	b.Fatal(err)
	//}
	//defer cpu.Close()
	//if err := pprof.StartCPUProfile(cpu); err != nil {
	//	b.Fatal(err)
	//}
	//defer pprof.StopCPUProfile()
	//defer func() {
	//	if err := pprof.WriteHeapProfile(mem); err != nil {
	//		b.Fatal(err)
	//	}
	//}()

	db, err := NewDB(Options{
		MaxBufferSize: 16 * 1024,
		TTL:           1 * time.Minute,
		Path:          dbPath,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dbPath)

	type id struct {
		id ID
		k  int
	}

	var ids1 []id
	var ids2 []ID

	for i := 0; i < 1*1024; i++ {
		id1, err := db.Put(content1)
		if err != nil {
			b.Fatal(err)
		}
		id2, err := db.Put(content2)
		if err != nil {
			b.Fatal(err)
		}
		id3, err := db.Put(content3)
		if err != nil {
			b.Fatal(err)
		}
		if i%99 == 0 {
			ids1 = append(ids1, id{id1, 1})
			ids2 = append(ids2, id1)
		}
		if i%98 == 0 {
			ids1 = append(ids1, id{id2, 2})
			ids2 = append(ids2, id2)
		}
		if i%97 == 0 {
			ids1 = append(ids1, id{id3, 3})
			ids2 = append(ids2, id3)
		}
	}

	var countersT [3]int
	var countersF [3]int

	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		vals, err := db.GetMany(ids2)
		if err != nil {
			b.Fatal(err)
		}
		for i, v := range vals {
			id := ids1[i]
			c := contents[id.k-1]
			if len(v) == len(c) && v[0] == c[0] && v[len(c)/2] == c[len(v)/2] {
				countersT[id.k-1]++
			} else {
				countersF[id.k-1]++
			}
		}
	}
	b.StopTimer()

	log.Println("countersT", countersT)
	log.Println("countersF", countersF)
}

func BenchmarkDB_GetManyB(b *testing.B) {
	now := strings.ReplaceAll(time.Now().Format(time.RFC3339), ":", "_")
	mem, err := os.Create("./heap/pprof." + now)
	if err != nil {
		b.Fatal(err)
	}
	defer mem.Close()
	cpu, err := os.Create("./cpu/pprof." + now)
	if err != nil {
		b.Fatal(err)
	}
	defer cpu.Close()
	if err := pprof.StartCPUProfile(cpu); err != nil {
		b.Fatal(err)
	}
	defer pprof.StopCPUProfile()
	defer func() {
		if err := pprof.WriteHeapProfile(mem); err != nil {
			b.Fatal(err)
		}
	}()

	db, err := NewDB(Options{
		MaxBufferSize: 16 * 1024,
		TTL:           1 * time.Minute,
		Path:          dbPath,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dbPath)

	type id struct {
		id ID
		k  int
	}

	var ids1 []id
	var ids2 []ID
	var vals [][]byte
	var pathBuilder StringBuilderUnsafe

	for i := 0; i < 1*1024; i++ {
		id1, err := db.Put(content1)
		if err != nil {
			b.Fatal(err)
		}
		id2, err := db.Put(content2)
		if err != nil {
			b.Fatal(err)
		}
		id3, err := db.Put(content3)
		if err != nil {
			b.Fatal(err)
		}
		if i%99 == 0 {
			ids1 = append(ids1, id{id1, 1})
			ids2 = append(ids2, id1)
		}
		if i%98 == 0 {
			ids1 = append(ids1, id{id2, 2})
			ids2 = append(ids2, id2)
		}
		if i%97 == 0 {
			ids1 = append(ids1, id{id3, 3})
			ids2 = append(ids2, id3)
		}
	}

	var countersT [3]int
	var countersF [3]int

	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		vals, err = db.GetManyB(ids2, vals, &pathBuilder)
		if err != nil {
			b.Fatal(err)
		}
		for i, v := range vals {
			id := ids1[i]
			c := contents[id.k-1]
			if len(v) == len(c) && v[0] == c[0] && v[len(c)/2] == c[len(v)/2] {
				countersT[id.k-1]++
			} else {
				countersF[id.k-1]++
			}
		}
	}
	b.StopTimer()

	log.Println("countersT", countersT)
	log.Println("countersF", countersF)
}
