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
	numPages   int
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
	numPages := fi.Size() / PageSize
	if fi.Size()%PageSize != 0 {
		return nil, fmt.Errorf("OpenPager: file size %d is not a multiple of page size %d", fi.Size(), PageSize)
	}
	return &Pager{
		File:       f,
		FileLength: uint64(fi.Size()),
		pages:      [TableMaxPages]*Page{},
		numPages:   int(numPages),
	}, nil
}

func (p *Pager) GetPage(pageNum uint32) (*Page, error) {
	if pageNum >= TableMaxPages {
		return nil, fmt.Errorf("GetPage: pageNum %d out of bounds", pageNum)
	}
	if p.pages[pageNum] == nil {
		newPg := &Page{}
		// 1) Compute pages on disk, rounding up for any partial page:
		numPagesOnDisk := uint32(p.FileLength / PageSize)
		if p.FileLength%PageSize != 0 {
			numPagesOnDisk++
		}

		// 2) If this page exists on disk, load only its real bytes:
		if pageNum < numPagesOnDisk {
			offset := int64(pageNum) * PageSize
			if _, err := p.File.Seek(offset, io.SeekStart); err != nil {
				return nil, fmt.Errorf("GetPage seek: %w", err)
			}
			// Determine how many bytes to read:
			toRead := PageSize
			// Last (partial) page: read only the remainder
			if pageNum == numPagesOnDisk-1 && p.FileLength%PageSize != 0 {
				toRead = int(p.FileLength % PageSize)
			}
			// Read them, ignoring io.EOF
			n, err := io.ReadFull(p.File, newPg.Data[:toRead])
			if err != nil && err != io.ErrUnexpectedEOF {
				return nil, fmt.Errorf("GetPage read: %w", err)
			}
			newPg.writeOffset = uint32(n) // if you need to track it
		} else {
			p.numPages++
		}

		p.pages[pageNum] = newPg
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

	if _, err := p.File.Write(pageBuf.Data[:]); err != nil {
		return fmt.Errorf("FlushPage: write failed: %w", err)
	}

	return nil
}

func (p *Pager) AllocatePage() (uint32, error) {
	np := uint32(p.FileLength / PageSize)
	if np >= TableMaxPages {
		return 0, fmt.Errorf("AllocatePage: out of space")
	}
	p.FileLength += PageSize
	return np, nil
}
