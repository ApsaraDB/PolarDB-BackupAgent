package main

import ()

type BackupPageHeader struct {
	Block      int64
	Block_size int64
	Truncate   int64
}

type DataPageMap struct {
	bitmap     []byte
	bitmapsize int
	blockcount int
	curBlock   int
	maxBlock   int
}

func (page *DataPageMap) init(buf []byte) {
	page.bitmap = buf
	page.bitmapsize = len(page.bitmap)

	page.countBlockChange()
	page.curBlock = -1
	page.maxBlock = page.bitmapsize*8 - 1
}

func (page *DataPageMap) countBlockChange() {
	page.blockcount = 0

	for _, e := range page.bitmap {
		for i := 0; i < 8; i++ {
			if (e>>i)&0x01 == 0x01 {
				page.blockcount = page.blockcount + 1
			}
		}
	}
}

func (page *DataPageMap) datapagemap_next() (bool, int) {
	for i := page.curBlock + 1; i <= page.maxBlock; i++ {
		byte_o := i / 8
		bit_o := i % 8

		_byte := page.bitmap[byte_o]
		if (_byte>>bit_o)&0x01 == 0x01 {
			page.curBlock = i
			// fmt.Printf(" i:%d, byte_o:%d, bit_o:%d\n", i, byte_o, bit_o)
			return true, page.curBlock
		}
	}
	return false, 0
}
