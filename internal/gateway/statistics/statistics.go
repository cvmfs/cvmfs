package statistics

import (
	"fmt"
	"os/exec"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Counters struct {
	ChunksAdded          int64  `json:"n_chunks_added,string"`
	ChunksDuplicated     int64  `json:"n_chunks_duplicated,string"`
	CatalogsAdded        int64  `json:"n_catalogs_added,string"`
	UploadedBytes        int64  `json:"sz_uploaded_bytes,string"`
	UploadedCatalogBytes int64  `json:"sz_uploaded_catalog_bytes,string"`
	StartTime            string `json:"start_time"`
}

// TODO: concurrent access
type StatisticsMgr struct {
	leaseCounters map[string]Counters
	readLock      sync.Mutex
}

func NewStatisticsMgr() *StatisticsMgr {
	return &StatisticsMgr{leaseCounters: make(map[string]Counters)}
}

func (m *StatisticsMgr) CreateLease(leasePath string) error {
	m.readLock.Lock()
	defer m.readLock.Unlock()
	if _, ex := m.leaseCounters[leasePath]; ex {
		return fmt.Errorf("Could not create statistics entry for lease %s, entry already exsts", leasePath)
	}
	m.leaseCounters[leasePath] = Counters{StartTime: time.Now().Format("2006-01-02 15:04:05")}
	return nil
}

func (m *StatisticsMgr) PopLease(leasePath string) (Counters, error) {
	m.readLock.Lock()
	defer m.readLock.Unlock()
	res, prs := m.leaseCounters[leasePath]
	if !prs {
		return Counters{}, fmt.Errorf("No statistics counters for lease %s", leasePath)
	}
	delete(m.leaseCounters, leasePath)
	return res, nil
}

func (m *StatisticsMgr) MergeIntoLeaseCounters(leasePath string, other *Counters) error {
	m.readLock.Lock()
	defer m.readLock.Unlock()
	c, prs := m.leaseCounters[leasePath]
	if !prs {
		return fmt.Errorf("Statistics counters not found for lease %s", leasePath)
	}
	c.ChunksAdded += other.ChunksAdded
	c.ChunksDuplicated += other.ChunksDuplicated
	c.CatalogsAdded += other.CatalogsAdded
	c.UploadedBytes += other.UploadedBytes
	c.UploadedCatalogBytes += other.UploadedCatalogBytes
	m.leaseCounters[leasePath] = c
	return nil
}

func (m *StatisticsMgr) UploadStatsPlots(repoName string) error {
	scriptPath := "/usr/share/cvmfs-server/upload_stats_plots.sh"
	cmd := exec.Command(scriptPath, repoName)
	err := cmd.Run()
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("statistics plots upload failed."))
	}
	return nil
}
