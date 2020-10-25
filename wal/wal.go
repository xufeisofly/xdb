package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"os"
	"sync"
)

type WalWriter struct {
	bufWriter *bufio.Writer
	f         *os.File
	mu        sync.Mutex
	crc       hash.Hash32
	seq       int64 // 当前 log 的 seq
	size      int64 // 当前 log file 的 size

	recordChn chan rawRecord
	closeChn  chan struct{}
}

const (
	firstSeq        = int64(1)
	maxFileByteSize = int64(50)
)

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

// NewWalWriter 初始化一个新 WalWriter
func NewWalWriter() (*WalWriter, error) {
	ww := &WalWriter{
		crc: crc32.New(crcTable),
	}
	fn := logName(firstSeq)
	if err := ww.rollover(fn); err != nil {
		return nil, err
	}

	go ww.writeLoop()
	return ww, nil
}

// rollover 让 WalWriter 滚动到 filename 这个 file
func (ww *WalWriter) rollover(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	ww.f = f
	ww.bufWriter = bufio.NewWriter(f)
	ww.size = 0
	return nil
}

// logName 生成 log 文件名称
func logName(seq int64) string {
	return fmt.Sprintf("xdb/wal-%d.log", seq)
}

// RecordLog 日志基本结构
type RecordLog struct {
	Seq     int64
	Content string
}

type rawRecord struct {
	seq      int64
	data     []byte
	checkSum uint32
}

// Append 为什么要加锁？保证顺序吧
func (ww *WalWriter) Append(log *RecordLog) {
	ww.mu.Lock()
	defer ww.mu.Unlock()

	// 按照 WalWriter 填充这个 log 的 Seq
	log.Seq = ww.seq
	ww.seq++

	rawRecord, err := ww.serializeLog(log)
	if err != nil {
		panic(err)
	}

	ww.recordChn <- rawRecord
}

func (ww *WalWriter) serializeLog(log *RecordLog) (rawRecord, error) {
	ww.crc.Reset()
	data := []byte(log.Content)
	if _, err := ww.crc.Write(data); err != nil {
		return rawRecord{}, err
	}
	c := ww.crc.Sum32()
	return rawRecord{
		seq:      log.Seq,
		data:     data,
		checkSum: c,
	}, nil
}

func (ww *WalWriter) writeLoop() {
Main:
	for {
		select {
		case r := <-ww.recordChn:
			if err := ww.writeRawRecord(r); err != nil {
				panic(err)
			}
		case <-ww.closeChn:
			break Main
		}

		// flush buffer and fsync file
		if err := ww.flushAndSync(); err != nil {
			panic(err)
		}
	}

	// flush buffer and fsync file
	if err := ww.flushAndSync(); err != nil {
		panic(err)
	}
}

// writeRawRecord 把 rawRecord 写入 bufio buffer
func (ww *WalWriter) writeRawRecord(r rawRecord) error {
	if ww.size > maxFileByteSize {
		if err := ww.rollover(logName(r.seq)); err != nil {
			return err
		}
	}

	var scratch [8]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(r.data)))
	binary.LittleEndian.PutUint32(scratch[4:8], r.checkSum)

	if _, err := ww.bufWriter.Write(scratch[:]); err != nil {
		return err
	}
	if _, err := ww.bufWriter.Write(r.data); err != nil {
		return err
	}
	ww.size += int64(len(r.data) + len(scratch))
	return nil
}

// flushAndSync flush buffer to file system and fsync file to disk
func (ww *WalWriter) flushAndSync() error {
	if err := ww.bufWriter.Flush(); err != nil {
		return err
	}
	if err := ww.f.Sync(); err != nil {
		return err
	}
	return nil
}

// Close close the wal writer
func (ww *WalWriter) Close() {
	ww.closeChn <- struct{}{}
}
