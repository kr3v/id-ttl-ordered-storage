package id_ttl_ordered_storage

import (
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"syscall"
	"testing"
	"time"
)

const dbPath = "./benchmark.db/"

var (
	content1 = []byte("super1dupercontentsuperdupercontent3contentsuperdupercontentsu1perdupercontentsuperdupercontent")
	content2 = []byte("superduper2contentcontentcontentsuperduperc2ontentsuperdupercontentsuperdupercontent")
	content3 = []byte("superdupercontentdupercontentcontentsupe3rdupercontentsuperdupercontentsuperdupe11rcontent")

	contents = [][]byte{content1, content2, content3}
)

func BenchmarkDB_Put(b *testing.B) {
	cpu, mem := profiling(b)
	defer profilingDefer(b, mem, cpu)

	db, err := NewDB(Options{
		MaxBufferSize: 8 * 1024 * 1024,
		TTL:           1 * time.Minute,
		Path:          dbPath,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dbPath)
	defer db.Close()

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

	rusage()
}

func BenchmarkDB_GetManyMMapB(b *testing.B) {
	cpu, mem := profiling(b)
	defer profilingDefer(b, mem, cpu)

	db, err := NewDB(Options{
		MaxBufferSize: 8 * 1024 * 1024,
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

	for i := 0; i < 256*1024; i++ {
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
		vals, err = db.GetMany(ids2, vals)
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

	rusage()
}

func profiling(b *testing.B) (*os.File, *os.File) {
	now := strings.ReplaceAll(time.Now().Format(time.RFC3339), ":", "_")
	mem, err := os.Create("./heap/pprof." + now)
	if err != nil {
		b.Fatal(err)
	}
	cpu, err := os.Create("./cpu/pprof." + now)
	if err != nil {
		b.Fatal(err)
	}
	if err := pprof.StartCPUProfile(cpu); err != nil {
		b.Fatal(err)
	}
	return cpu, mem
}

func profilingDefer(b *testing.B, mem *os.File, cpu *os.File) {
	defer mem.Close()
	defer cpu.Close()
	defer pprof.StopCPUProfile()
	defer func() {
		if err := pprof.WriteHeapProfile(mem); err != nil {
			b.Fatal(err)
		}
	}()
}

func rusage() {
	var ru syscall.Rusage
	log.Println(syscall.Getrusage(syscall.RUSAGE_SELF, &ru))
	log.Println("Utime", ru.Utime)
	log.Println("Stime", ru.Stime)
	log.Println("Maxrss", ru.Maxrss)
	log.Println("Ixrss", ru.Ixrss)
	log.Println("Idrss", ru.Idrss)
	log.Println("Isrss", ru.Isrss)
	log.Println("Minflt", ru.Minflt)
	log.Println("Majflt", ru.Majflt)
	log.Println("Nswap", ru.Nswap)
	log.Println("Inblock", ru.Inblock)
	log.Println("Oublock", ru.Oublock)
	log.Println("Msgsnd", ru.Msgsnd)
	log.Println("Msgrcv", ru.Msgrcv)
	log.Println("Nsignals", ru.Nsignals)
	log.Println("Nvcsw", ru.Nvcsw)
	log.Println("Nivcsw", ru.Nivcsw)

	log.Println(syscall.Getrusage(syscall.RUSAGE_CHILDREN, &ru))
	log.Println("Maxrss", ru.Maxrss)

	var totalSize int64 = 0
	filepath.Walk(dbPath, func(path string, info fs.FileInfo, err error) error {
		if info != nil && info.Mode().IsRegular() {
			totalSize += info.Size()
		}
		return nil
	})
	log.Print(totalSize)
}
