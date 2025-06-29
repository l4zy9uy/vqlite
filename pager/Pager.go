package pager

import (
	"fmt"
	"io"
	"os"
)

const (
	TableMaxPages = 100
	PageSize      = 4096
)

type Page struct {
	Data        [PageSize]byte
	writeOffset uint32
}

type Pager struct {
	File       *os.File
	FileLength uint64
	pages      [TableMaxPages]*Page
}

func OpenPager(filename string) (*Pager, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("OpenPager: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("OpenPager Stat: %w", err)
	}
	return &Pager{
		File:       f,
		FileLength: uint64(fi.Size()),
		pages:      [100]*Page{},
	}, nil
}

func (p *Pager) GetPage(pageNum uint32) (*Page, error) {
	if pageNum >= TableMaxPages {
		return nil, fmt.Errorf("getPage: pageNum %d out of bounds (max %d)", pageNum, TableMaxPages-1)
	}

	if p.pages[pageNum] == nil {
		newPage := new(Page)
		numPagesOnDisk := uint32(p.FileLength / PageSize)

		if pageNum < numPagesOnDisk {
			offset := int64(pageNum) * PageSize
			if _, err := p.File.Seek(offset, io.SeekStart); err != nil {
				return nil, fmt.Errorf("getPage seek: %w", err)
			}

			if _, err := p.File.Read(newPage.Data[:]); err != nil {
				return nil, fmt.Errorf("getPage read: %w", err)
			}
		}

		p.pages[pageNum] = newPage
	}

	return p.pages[pageNum], nil
}

func (p *Pager) FlushPage(pageNum uint32, size uint32) error {
	if pageNum >= TableMaxPages {
		return fmt.Errorf("FlushPage: pageNum %d out of bounds (max %d)", pageNum, TableMaxPages-1)
	}

	pageBuf := p.pages[pageNum]
	if pageBuf == nil {
		return fmt.Errorf("FlushPage: tried to flush null page at index %d", pageNum)
	}

	offset := int64(pageNum) * PageSize
	if _, err := p.File.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("FlushPage: seek failed: %w", err)
	}

	if _, err := p.File.Write(pageBuf.Data[:size]); err != nil {
		return fmt.Errorf("FlushPage: write failed: %w", err)
	}

	return nil
}
