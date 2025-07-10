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
	Pager       *Pager
	PageNum     uint32
	Dirty       bool
}

type Pager struct {
	File     *os.File
	Pages    []*Page
	NumPages int
}

func (p *Pager) FileSize() (int64, error) {
	fi, err := p.File.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// OpenPager opens the file, computes how many pages it currently has,
// and allocates the slice — _without_ reading every page.
func OpenPager(path string) (*Pager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fi.Size()
	numPages := int((fileSize + PageSize - 1) / PageSize)

	p := &Pager{
		File:     f,
		Pages:    make([]*Page, numPages),
		NumPages: numPages,
	}
	return p, nil
}

// preloadAll will eagerly load every page into memory.
// _Use with caution_ on very large files!
func (p *Pager) preloadAll() error {
	for i := 0; i < p.NumPages; i++ {
		pg, err := p.loadPageFromDisk(uint32(i))
		if err != nil {
			return err
		}
		pg.Pager = p
		pg.PageNum = uint32(i)
		p.Pages[i] = pg
	}
	return nil
}

// loadPageFromDisk handles the raw seek+read and returns a fresh Page.
func (p *Pager) loadPageFromDisk(pageNum uint32) (*Page, error) {
	off := int64(pageNum) * PageSize
	if _, err := p.File.Seek(off, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek page %d: %w", pageNum, err)
	}
	pg := &Page{
		Pager:   p,
		PageNum: pageNum,
	}
	n, err := io.ReadFull(p.File, pg.Data[:])
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("read page %d: %w", pageNum, err)
	}
	pg.writeOffset = uint32(n)
	return pg, nil
}

func (p *Pager) GetPage(pageNum uint32) (*Page, error) {
	if pageNum >= TableMaxPages {
		return nil, fmt.Errorf("GetPage: page %d out of bounds (max %d)", pageNum, TableMaxPages)
	}
	if pageNum >= uint32(p.NumPages) {
		return nil, fmt.Errorf("GetPage: page %d beyond EOF (%d pages)", pageNum, p.NumPages)
	}
	// if not yet in cache, pull it in
	if p.Pages[pageNum] == nil {
		pg, err := p.loadPageFromDisk(pageNum)
		if err != nil {
			return nil, err
		}
		p.Pages[pageNum] = pg
	}
	return p.Pages[pageNum], nil
}

func (p *Pager) FlushPage(pgNo uint32) error {
	pg := p.Pages[pgNo]
	if pg == nil || !pg.Dirty {
		return nil
	}
	off := int64(pgNo) * PageSize
	if _, err := p.File.Seek(off, io.SeekStart); err != nil {
		return err
	}
	if _, err := p.File.Write(pg.Data[:]); err != nil {
		return err
	}
	pg.Dirty = false
	return nil
}

func (p *Pager) AllocatePage() (uint32, error) {
	np := uint32(p.NumPages)
	if np >= TableMaxPages {
		return 0, fmt.Errorf("no more pages")
	}
	pg := &Page{
		Pager:   p,
		PageNum: np,
		Dirty:   true, // mark for writing
	}
	p.Pages = append(p.Pages, pg)
	p.NumPages++
	return np, nil
}

func (p *Pager) FlushAll() error {
	for i, pg := range p.Pages {
		if pg != nil && pg.Dirty {
			if err := p.FlushPage(uint32(i)); err != nil {
				return err
			}
			pg.Dirty = false
		}
	}
	return p.File.Sync()
}

func (p *Pager) Close() error {
	if err := p.FlushAll(); err != nil {
		return err
	}
	return p.File.Close()
}
