package pager

import (
	"os"
	"path/filepath"
	"testing"
)

// Test opening an empty pager file.
func TestOpenPagerEmptyFile(t *testing.T) {
	tmp, err := os.CreateTemp("", "pager_test_empty_*.db")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	path := tmp.Name()
	tmp.Close()
	defer os.Remove(path)

	p, err := OpenPager(path)
	if err != nil {
		t.Fatalf("OpenPager: %v", err)
	}
	defer p.Close()

	if len(p.Pages) != 0 {
		t.Errorf("expected 0 pages, got %d", len(p.Pages))
	}

	size, err := p.FileSize()
	if err != nil {
		t.Fatalf("FileSize: %v", err)
	}
	if size != 0 {
		t.Errorf("expected file size 0, got %d", size)
	}
}

// Test that GetPage on an empty pager returns an error.
func TestGetPageOutOfBounds(t *testing.T) {
	tmp, err := os.CreateTemp("", "pager_test_oob_*.db")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	path := tmp.Name()
	tmp.Close()
	defer os.Remove(path)

	p, err := OpenPager(path)
	if err != nil {
		t.Fatalf("OpenPager: %v", err)
	}
	defer p.Close()

	if _, err := p.GetPage(0); err == nil {
		t.Errorf("expected error on GetPage(0) for empty pager")
	}
}

// Test AllocatePage, modifying, flushing, and verifying on-disk content.
func TestAllocateAndFlushPage(t *testing.T) {
	tmp, err := os.CreateTemp("", "pager_test_alloc_*.db")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	path := tmp.Name()
	tmp.Close()
	defer os.Remove(path)

	p, err := OpenPager(path)
	if err != nil {
		t.Fatalf("OpenPager: %v", err)
	}
	defer p.Close()

	// Allocate a new page
	pgNum, err := p.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	if pgNum != 0 {
		t.Errorf("expected pgNum=0, got %d", pgNum)
	}
	if len(p.Pages) != 1 {
		t.Errorf("expected len(p.Pages)=1, got %d", len(p.Pages))
	}
	pg := p.Pages[pgNum]
	if pg == nil {
		t.Fatalf("allocated page is nil")
	}
	if !pg.Dirty {
		t.Errorf("expected allocated page to be dirty")
	}

	// Write some content
	pg.Data[0] = 0xAB
	pg.Data[PageSize-1] = 0xCD
	pg.Dirty = true

	// Flush the page
	if err := p.FlushPage(pgNum); err != nil {
		t.Fatalf("FlushPage: %v", err)
	}

	size, err := p.FileSize()
	if err != nil {
		t.Fatalf("FileSize: %v", err)
	}
	if size != PageSize {
		t.Errorf("expected file size %d, got %d", PageSize, size)
	}

	// Read file content
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) != PageSize {
		t.Fatalf("expected read data length %d, got %d", PageSize, len(data))
	}
	if data[0] != 0xAB {
		t.Errorf("expected byte 0 = 0xAB, got 0x%X", data[0])
	}
	if data[PageSize-1] != 0xCD {
		t.Errorf("expected byte at %d = 0xCD, got 0x%X", PageSize-1, data[PageSize-1])
	}

	// After flushing, page should no longer be dirty
	if pg.Dirty {
		t.Errorf("expected page dirty=false after flush")
	}
}

// Test loading an existing full page from disk.
func TestLoadExistingPage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "exist.db")

	// Write one full page of 0x01 to disk
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	buf := make([]byte, PageSize)
	for i := range buf {
		buf[i] = 0x01
	}
	if _, err := f.Write(buf); err != nil {
		t.Fatalf("Write: %v", err)
	}
	f.Close()

	p, err := OpenPager(path)
	if err != nil {
		t.Fatalf("OpenPager: %v", err)
	}
	defer p.Close()

	if len(p.Pages) != 1 {
		t.Errorf("expected 1 page, got %d", len(p.Pages))
	}
	pg, err := p.GetPage(0)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	if pg.Dirty {
		t.Errorf("expected loaded page dirty=false")
	}
	if pg.Data[0] != 0x01 || pg.Data[PageSize-1] != 0x01 {
		t.Errorf("unexpected data in loaded page: first=0x%X last=0x%X", pg.Data[0], pg.Data[PageSize-1])
	}
}

// Test partial-page read at EOF.
func TestPartialPageRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "partial.db")

	// Write 100 bytes of 0xAA to disk
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	buf := make([]byte, 100)
	for i := range buf {
		buf[i] = 0xAA
	}
	if _, err := f.Write(buf); err != nil {
		t.Fatalf("Write: %v", err)
	}
	f.Close()

	p, err := OpenPager(path)
	if err != nil {
		t.Fatalf("OpenPager: %v", err)
	}
	defer p.Close()

	if len(p.Pages) != 1 {
		t.Errorf("expected 1 page, got %d", len(p.Pages))
	}
	pg, err := p.GetPage(0)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}

	// Verify first 100 bytes are 0xAA
	for i := 0; i < 100; i++ {
		if pg.Data[i] != 0xAA {
			t.Errorf("byte %d: expected 0xAA, got 0x%X", i, pg.Data[i])
			break
		}
	}
	// Verify byte 100 is zero
	if pg.Data[100] != 0 {
		t.Errorf("expected pg.Data[100]=0, got 0x%X", pg.Data[100])
	}
}

// Test that GetPage can retrieve an allocated page.
func TestGetPageAfterAllocate(t *testing.T) {
	tmp, err := os.CreateTemp("", "pager_test_afteralloc_*.db")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	path := tmp.Name()
	tmp.Close()
	defer os.Remove(path)

	p, err := OpenPager(path)
	if err != nil {
		t.Fatalf("OpenPager: %v", err)
	}
	defer p.Close()

	pgNum, err := p.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	first := p.Pages[pgNum]
	retrieved, err := p.GetPage(pgNum)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	if first != retrieved {
		t.Errorf("GetPage returned a different page instance")
	}
}
