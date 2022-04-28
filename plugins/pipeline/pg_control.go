package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"time"
)

/* src/include/catalog/pg_control.h */
type PgControlData struct {
	version       uint32 // ControlFileData.pg_control_version [8:12]
	checkpointloc uint64 // ControlFileData.checkPoint [32:40]
	startpoint    uint64 // ControlFileData.checkPointCopy.redo [40:48]
	starttime     uint64 // ControlFileData.checkPointCopy.time [96:104]
	starttli      uint32 // ControlFileData.checkPointCopy.ThisTimeLine [48:52]
	walsize       uint32 // ControlFileData.xlog_seg_size [220:224]
	checksum      uint32 // ControlFileData.crc  [280:284]

	xlogfilename string
}

const (
	PgControlDataSize     = 280
	PgControlDataFileSize = 284
	BackupLabelFile       = "backup.label"
)

func IsPgControlFileInvalid(buf []byte, checksum, version uint32) bool {
	crc32q := crc32.MakeTable(0x82F63B78)
	crc32c := crc32.Checksum(buf[:PgControlDataSize], crc32q)

	if crc32c != checksum {
		return false
	}

	if version%65536 == 0 && version/65536 != 0 {
		return false
	} else {
		return true
	}
}

func xlogByteToSeg(seq, walsize uint32) uint32 {
	return seq / walsize
}

func xlogsegmentsperxlogid(walsize uint32) uint64 {
	return uint64(0x100000000) / uint64(walsize)
}

func getxlogfilename(starttli uint32, seg uint64, walsize uint32) string {
	prefix := seg / xlogsegmentsperxlogid(walsize)
	suffix := seg % xlogsegmentsperxlogid(walsize)
	xlogfilename := fmt.Sprintf("%08X%08X%08X", starttli, uint32(prefix), uint32(suffix))
	return xlogfilename
}

func ParsePgControlFile(path string) (*PgControlData, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, PgControlDataFileSize)
	n, err := f.Read(buf)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if n != PgControlDataFileSize {
		return nil, errors.New("read failed")
	}

	return ParsePgControl(buf)
}

func ParsePgControl(buf []byte) (*PgControlData, error) {
	_checkpointloc := buf[32:40]
	_startpoint := buf[40:48]
	_starttli := buf[48:52]
	_walsize := buf[220:224]

	_version := buf[8:12]
	_checksum := buf[280:284]
	_starttime := buf[96:104]

	checksum := binary.LittleEndian.Uint32(_checksum)
	version := binary.LittleEndian.Uint32(_version)

	var checkpointloc, startpoint uint64
	var starttli, walsize uint32
	var starttime uint64
	if !IsPgControlFileInvalid(buf, checksum, version) {
		checksum = binary.BigEndian.Uint32(_checksum)
		version = binary.BigEndian.Uint32(_version)
		if !IsPgControlFileInvalid(buf, checksum, version) {
			return nil, errors.New("invalid pg_control file")
		}

		checkpointloc = binary.BigEndian.Uint64(_checkpointloc)
		startpoint = binary.BigEndian.Uint64(_startpoint)
		starttli = binary.BigEndian.Uint32(_starttli)
		walsize = binary.BigEndian.Uint32(_walsize)
		starttime = binary.BigEndian.Uint64(_starttime)
	} else {
		checkpointloc = binary.LittleEndian.Uint64(_checkpointloc)
		startpoint = binary.LittleEndian.Uint64(_startpoint)
		starttli = binary.LittleEndian.Uint32(_starttli)
		walsize = binary.LittleEndian.Uint32(_walsize)
		starttime = binary.LittleEndian.Uint64(_starttime)
	}

	segno := startpoint / uint64(walsize)

	pgControlData := &PgControlData{
		checkpointloc: checkpointloc,
		startpoint:    startpoint,
		starttli:      starttli,
		starttime:     starttime,
		version:       version,
		checksum:      checksum,
		walsize:       walsize,
		xlogfilename:  getxlogfilename(starttli, segno, walsize),
	}
	return pgControlData, nil
}

func SetBackupLabelInfo(pgctrl *PgControlData, node, label string) string {
	var now time.Time
	now = time.Unix(int64(pgctrl.starttime), 0)
	t := now.Format("2006-01-02 15:04:05")
	zone, _ := now.Zone()

	str := fmt.Sprintf("START WAL LOCATION: %X/%X (file %s)\n", uint32(pgctrl.startpoint>>32), uint32(pgctrl.startpoint), pgctrl.xlogfilename)
	str += fmt.Sprintf("CHECKPOINT LOCATION: %X/%X\n", uint32((pgctrl.checkpointloc >> 32)), uint32(pgctrl.checkpointloc))
	str += fmt.Sprintf("BACKUP METHOD: %s\n", "pg_start_backup")
	str += fmt.Sprintf("BACKUP FROM: %s\n", node)
	str += fmt.Sprintf("START TIME: %s %s\n", t, zone)
	str += fmt.Sprintf("LABEL: %s\n", label)
	str += fmt.Sprintf("START TIMELINE: %d\n", pgctrl.starttli)
	return str
}

func test_parse_pg_control() {
	ctrl, err := ParsePgControlFile("pg_control")
	if err != nil {
		panic(err)
	}
	str := SetBackupLabelInfo(ctrl, "master", "backup")
	fmt.Printf(str)
}
