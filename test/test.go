package main

/*
// #include <waldump.h>
*/
import "C"
import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
	"github.com/pierrec/lz4"
)

// func test1() {
//     err := _PFSMount("disk", "loop4", 0x1011)
//     if err != nil {
//         fmt.Printf("mount error: %s\n", err.Error())
//         return
//     }
//     walname := "/loop4/data/pg_wal/000000020000000000000001"
//     walSegSize := int(C.InitWalSize(C.CString(walname), 1))
//     fmt.Printf("walsize: %d\n", walSegSize)
//     var info C.WallInfo
//     C.GetWalInfo(C.CString(walname), C.int(walSegSize), 1, &info)
//     fmt.Printf("%d, %d\n", int64(info.starttime), int64(info.endtime))
//     fmt.Printf("%x, %x, %x\n", uint64(info.startlsn), uint64(info.endlsn), uint64(info.prevlsn))
// }

func testMultiMount() {
	param := make(map[string]interface{})
	conf := make(map[string]interface{})

	conf["Pbd"] = "loop0"
	conf["Mode"] = "file"
	conf["Flags"] = "backup"
	conf["DBPath"] = "/data"
	conf["Cluster"] = "disk"
	conf["LogPath"] = "/home/louxiao.lx/test.log"

	content, _ := json.Marshal(&conf)

	param["conf"] = content
	_, err := PFSInit(nil, param)
	if err != nil {
		fmt.Printf("mount error: %s\n", err.Error())
	}
	_, err = PFSInit(nil, param)
	if err != nil {
		fmt.Printf("mount error: %s\n", err.Error())
	}

	conf["Pbd"] = "loop1"
	conf["LogPath"] = "/home/louxiao.lx/test2.log"
	content, _ = json.Marshal(&conf)
	param["conf"] = content
	_, err = PFSInit(nil, param)
	if err != nil {
		fmt.Printf("mount error: %s\n", err.Error())
	}
}

func testReadDir() {
	param := make(map[string]interface{})
	conf := make(map[string]interface{})

	conf["Pbd"] = "loop3"
	conf["Mode"] = "file"
	conf["Flags"] = "backup"
	conf["DBPath"] = "/data"
	conf["Cluster"] = "disk"
	conf["LogPath"] = "/home/louxiao.lx/test.log"

	content, _ := json.Marshal(&conf)

	param["conf"] = content
	ctx, err := PFSInit(nil, param)
	if err != nil {
		fmt.Printf("mount error: %s\n", err.Error())
	}

	param["path"] = ""
	currentTime := time.Now()
	files, err := PFSReaddir_r(ctx, param)
	if err != nil {
		fmt.Println("readdir err:", err.Error())
	} else {
		fmt.Println("len of files:", len(files.([]string)))
		fmt.Println("time used:", time.Now().Sub(currentTime))
	}
}

func testCreateFiles() {
	param := make(map[string]interface{})
	conf := make(map[string]interface{})

	conf["Pbd"] = "loop3"
	conf["Mode"] = "file"
	conf["Flags"] = "restore"
	conf["DBPath"] = "/data"
	conf["Cluster"] = "disk"
	conf["LogPath"] = "/home/louxiao.lx/test.log"

	content, _ := json.Marshal(&conf)

	param["conf"] = content
	ctx, err := PFSInit(nil, param)
	if err != nil {
		fmt.Printf("mount error: %s\n", err.Error())
	}

	for i := 1; i <= 819200; i++ {

		param["name"] = "testfiles/file_" + strconv.Itoa(i)
		param["mode"] = uint(0600)

		PFSCreate(ctx, param)
		PFSClose(ctx, param)
	}
}

func CopyBuffer(dst io.Writer, src io.Reader, buf []byte) (written int64, crc uint32, err error) {
	if buf == nil {
		return 0, 0, errors.New("buf cannot be nil")
	}
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			crc = crc32.Update(crc, crc32.IEEETable, buf[:nr])
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = errors.New("write error:" + ew.Error())
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = errors.New("read error:" + er.Error())
			}
			break
		}
	}
	return written, crc, err
}

func testWriteFiles() {
    tb := time.Now()

	buf := make([]byte, 256 * 1024)

	param := make(map[string]interface{})
	conf := make(map[string]interface{})

	conf["Pbd"] = "mapper_36005076708808132980000000000010a"
	conf["Mode"] = "file"
	conf["Flags"] = "restore"
	conf["DBPath"] = "/data"
	conf["Cluster"] = "disk"
	conf["LogPath"] = "/tmp/test.log"

	content, _ := json.Marshal(&conf)

	param["conf"] = content
	ctx, err := PFSInit(nil, param)
	if err != nil {
		fmt.Printf("mount error: %s\n", err.Error())
	}

    qu := NewSingleNodeDispatchTask()
    for i := 1; i <= 32; i++ { 
        qu.AddTask(NewTask("file_" + strconv.Itoa(i)))
    }
    var wait sync.WaitGroup

	for i := 1; i <= 16; i++ {
        wait.Add(1)
		go func() {
            for {
                task := qu.Get()
                if task == nil {
                    break
                }

                file := task.name

				param := make(map[string]interface{})
				
                param["name"] = "testfiles/" + file
                param["mode"] = uint(0600)

                frontendfile, err := PFSCreate(ctx, param)
                if err != nil {
                    fmt.Printf("create error: %s\n", err.Error())
                    break
                }
				fmt.Printf("create %s\n", file)
				
				param["file"] = frontendfile
				param["size"] = int64(1073741824)
				_, err = PFSFallocate(ctx, param)
				if err != nil {
                    fmt.Printf("fallocate error: %s\n", err.Error())
                    break
				}
				fmt.Printf("fallocate %s\n", file)

                backupendfile, err := os.Open(file)
                if err != nil {
                    fmt.Printf("open error: %s\n", err.Error())
                    break
                }
                fmt.Printf("open %s\n", file)

				CopyBuffer(frontendfile.(io.Writer), backupendfile, buf)
				fmt.Printf("copy %s done\n", file)

                PFSClose(ctx, param)
                
                fmt.Printf("write %s done\n", file)
            }
            
            wait.Done()
		}()
    }
    
    wait.Wait()

    fmt.Println(time.Now().Sub(tb))
}

func CopyBufferN(dst io.Writer, src io.Reader, buf []byte, n int64) (written int64, crc uint32, err error) {
    if n == 0 {
        return 0, 0, nil
    }

    size := len(buf)
    count := n / int64(size)
    left := n % int64(size)

    var i int64
    for i = 0; i < count; i++ {
        nr, er := src.Read(buf)
        if nr > 0 {
            crc = crc32.Update(crc, crc32.IEEETable, buf[:nr])
            nw, ew := dst.Write(buf[0:nr])
            if nw > 0 {
                written += int64(nw)
            }
            if ew != nil {
                err = errors.New("write error:" + ew.Error())
                break
            }
            if nr != nw {
                err = io.ErrShortWrite
                break
            }
        }
        if er != nil {
            if er != io.EOF {
                err = errors.New("read error:" + er.Error())
            }
            break
        }
    }

    if err != nil {
        return written, crc, err
    }

    if left > 0 {
        buf = buf[0:left]
        nr, er := src.Read(buf)
        if nr > 0 {
            crc = crc32.Update(crc, crc32.IEEETable, buf[:nr])
            nw, ew := dst.Write(buf[0:nr])
            if nw > 0 {
                written += int64(nw)
            }
            if ew != nil {
                err = ew
            }
            if nr != nw {
                err = io.ErrShortWrite
            }
        }

        if er != nil {
            if er != io.EOF {
                err = er
            }
        }
    }
    
    return written, crc, err
}

func testPRead() {
	buf := make([]byte, 256 * 1024)

	backupendfile, err := os.Create("testseekfile")
	if err != nil {
		fmt.Printf("open error: %s\n", err.Error())
		return
	}
	defer backupendfile.Close()

	param := make(map[string]interface{})
	conf := make(map[string]interface{})

	conf["Pbd"] = "loop1"
	conf["Mode"] = "file"
	conf["Flags"] = "backup"
	conf["DBPath"] = "/data"
	conf["Cluster"] = "disk"
	conf["LogPath"] = "/tmp/test.log"

	content, _ := json.Marshal(&conf)

	param["conf"] = content
	ctx, err := PFSInit(nil, param)
	if err != nil {
		fmt.Printf("mount error: %s\n", err.Error())
		return
	}

	frontendParam := make(map[string]interface{})
	frontendParam["name"] = "base/16032/16395"
    frontendParam["flags"] = uint(00)
    frontendParam["mode"] = uint(0400)
    frontendFile, err := PFSOpen(ctx, frontendParam)
    if err != nil {
        fmt.Printf("[ERROR] pfs open %s failed: %s\n", frontendParam["name"], err.Error())
        return
	}
	fmt.Printf("open %s\n", frontendParam["name"])
    frontendReader, ok := frontendFile.(io.Reader)
    if !ok {
        frontendParam["file"] = frontendFile
        PFSClose(ctx, frontendParam)
        return
	}
	
	frontendParam["file"] = frontendFile
	frontendParam["offset"] = 7 * 8192
	offset, err := PFSSeek(ctx, frontendParam)
	if err != nil {
		fmt.Printf("[ERROR] pgpipeline seek %s failed: %s\n", frontendParam["name"], err.Error())
		return
	}
	if offset != frontendParam["offset"] {
		err = errors.New("offset return by seek not valid")
		fmt.Printf("[ERROR] pgpipeline seek %s failed: %s, offset set: %d, return: %d\n", frontendParam["name"], err.Error(), frontendParam["offset"], offset)
		return
	}

	n, _, err := CopyBufferN(backupendfile, frontendReader, buf, 8192)
	if err != nil {
		frontendParam["file"] = frontendFile
		PFSClose(ctx, frontendParam)
		fmt.Printf("[ERROR] pgpipeline archive %s offset %d failed: %s\n", frontendParam["name"], offset, err.Error())
		return
	}
	fmt.Printf("copy %d bytes\n", n)

	frontendParam["file"] = frontendFile
	frontendParam["offset"] = 7 * 8192
	offset, err = PFSSeek(ctx, frontendParam)
	if err != nil {
		fmt.Printf("[ERROR] pgpipeline seek %s failed: %s\n", frontendParam["name"], err.Error())
		return
	}
	if offset != frontendParam["offset"] {
		err = errors.New("offset return by seek not valid")
		fmt.Printf("[ERROR] pgpipeline seek %s failed: %s, offset set: %d, return: %d\n", frontendParam["name"], err.Error(), frontendParam["offset"], offset)
		return
	}

	n, _, err = CopyBufferN(backupendfile, frontendReader, buf, 8192)
	if err != nil {
		frontendParam["file"] = frontendFile
		PFSClose(ctx, frontendParam)
		fmt.Printf("[ERROR] pgpipeline archive %s offset %d failed: %s\n", frontendParam["name"], offset, err.Error())
		return
	}
	fmt.Printf("copy %d bytes\n", n)
}

func testDecompressFromEncryptionHttpFile(file string){
	compress := true
	enableEncryption := true
	encryptionPassword := "abc123456"

	conf := make(map[string]interface{})
	param := make(map[string]interface{})

	conf["InstanceID"] = "test-flex-ins"
	conf["BackupID"] = "increbk"
	conf["Endpoint"] = "47.118.41.173:8080"

	content, _ := json.Marshal(&conf)

	param["conf"] = content
	httpctx, err := HTTPInit(nil, param)
	if err != nil {
		fmt.Printf("init error: %s\n", err.Error())
		return
	}

	params := make(map[string]interface{})

	buf := make([]byte, 256 * 1024)
	params["name"] = file
	backendfile, err := HTTPOpen(httpctx, params)
	if err != nil {
		fmt.Printf("[ERROR] open %s failed: %s\n", file, err.Error())
		return
	}

	backendreader, ok := backendfile.(io.Reader)
	if !ok {
		fmt.Printf("[ERROR] invalid backendFile")
		return
	}

	frontendfile, err := os.Create("./restore/" + file)
	if err != nil {
		fmt.Printf("[ERROR] frontend create %s failed: %s\n", file, err.Error())
		return
	}
	frontendwriter := io.Writer(frontendfile)

	if compress {
		if enableEncryption {
			chachaReader := NewChaCha20Reader(encryptionPassword, backendreader)
			lz4Reader := lz4.NewReader(chachaReader)
			// lz4Reader.Apply(
			// 	lz4.ConcurrencyOption(-1),
			// 	// lz4.ChecksumOption(false),
			// )
			_, _, err = CopyBuffer(frontendwriter, io.Reader(lz4Reader), buf)
		} else {
			lz4Reader := lz4.NewReader(backendreader)
			_, _, err = CopyBuffer(frontendwriter, io.Reader(lz4Reader), buf)
		}
	} else {
		if enableEncryption {
			chachaReader := NewChaCha20Reader(encryptionPassword, backendreader)
			_, _, err = CopyBuffer(frontendwriter, chachaReader, buf)
		} else {
			_, _, err = CopyBuffer(frontendwriter, backendreader, buf)
		}
	}

	frontendfile.Close()
	
	params["file"] = backendfile
	HTTPClose(httpctx, params)

	if err != nil {
		fmt.Printf("[ERROR] recovery file %s failed: %s\n", file, err.Error())
		return
	}
}

func testCompressToEncryptionHttpFile(file string) {
	compress := true
	enableEncryption := true
	encryptionPassword := "abc123456"

	chunkBuf := make([]byte, 256 * 1024)

	fmt.Printf("[INFO] begin to backup [%s]\n", file)

	frontendFile, err := os.OpenFile("./" + file, os.O_RDWR, os.ModePerm)
	if err != nil {
		fmt.Printf("[ERROR] open frontend file [%s] failed, err[%s]\n", file, err.Error())
		return
	}

	frontendReader := io.Reader(frontendFile)

	conf := make(map[string]interface{})
	param := make(map[string]interface{})

	conf["InstanceID"] = "test-flex-ins"
	conf["BackupID"] = "increbk"
	conf["Endpoint"] = "47.118.41.173:8080"

	content, _ := json.Marshal(&conf)

	param["conf"] = content
	httpctx, err := HTTPInit(nil, param)
	if err != nil {
		fmt.Printf("init error: %s\n", err.Error())
		return
	}

	params := make(map[string]interface{})

	params["name"] = file
	backendFile, e := HTTPCreate(httpctx, params)
	if e != nil {
		fmt.Printf("[ERROR] create %s failed: %s\n", file, err.Error())
		return
	}

	backendWriter, ok := backendFile.(io.Writer)
	if !ok {
		fmt.Printf("[ERROR] invalid backendFile")
		return
	}

	if compress {
		if enableEncryption {
			chachaWriter := NewChaCha20Writer(encryptionPassword, backendWriter)
			lz4Writer := lz4.NewWriter(chachaWriter)
			// lz4Writer.Apply(
			// 	lz4.ConcurrencyOption(-1),
			// 	// lz4.ChecksumOption(false),
			// 	// lz4.BlockSizeOption(lz4.Block4Mb),
			// 	// lz4.CompressionLevelOption(0),
			// 	// lz4.LegacyOption(true),
			// )
			_, _, err = CopyBuffer(io.Writer(lz4Writer), io.Reader(frontendReader), chunkBuf)
			lz4Writer.Close()
		} else {
			lz4Writer := lz4.NewWriter(backendWriter)
			_, _, err = CopyBuffer(io.Writer(lz4Writer), io.Reader(frontendReader), chunkBuf)
			lz4Writer.Close()
		}
	} else {
		if enableEncryption {
			chachaWriter := NewChaCha20Writer(encryptionPassword, backendWriter)
			_, _, err = CopyBuffer(io.Writer(chachaWriter), io.Reader(frontendReader), chunkBuf)
		} else {
			_, _, err = CopyBuffer(io.Writer(backendWriter), io.Reader(frontendReader), chunkBuf)
		}
	}

	frontendFile.Close()

	params["file"] = backendFile
	HTTPClose(httpctx, params)


	if err != nil {
		fmt.Printf("[ERROR] copy failed: %s", err.Error())
	}
}

func main() {
	// test1()
	// testMultiMount()
	// testCreateFiles()
	// testReadDir()
	// testWriteFiles()
	// testPRead()
	testfile := "xxx06"
	testCompressToEncryptionHttpFile(testfile)
	testDecompressFromEncryptionHttpFile(testfile)
}
