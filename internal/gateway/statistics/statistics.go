package statistics

import (
	"fmt"
	"os/exec"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type PublishCounters struct {
	ChunksAdded          int64 `json:"n_chunks_added"`
	ChunksDuplicated     int64 `json:"n_chunks_duplicated"`
	CatalogsAdded        int64 `json:"n_catalogs_added"`
	UploadedBytes        int64 `json:"sz_uploaded_bytes"`
	UploadedCatalogBytes int64 `json:"sz_uploaded_catalog_bytes"`
}

type Statistics struct {
	Publish   PublishCounters `json:"publish"`
	StartTime string          `json:"start_time"`
}

type StatisticsMgr struct {
	leaseStatistics map[string]Statistics
	readLock        sync.Mutex
}

func NewStatisticsMgr() *StatisticsMgr {
	return &StatisticsMgr{leaseStatistics: make(map[string]Statistics)}
}

func (m *StatisticsMgr) CreateLease(leasePath string) error {
	m.readLock.Lock()
	defer m.readLock.Unlock()
	if _, ex := m.leaseStatistics[leasePath]; ex {
		return fmt.Errorf("Could not create statistics entry for lease %s, entry already exists", leasePath)
	}
	m.leaseStatistics[leasePath] = Statistics{StartTime: time.Now().Format("2006-01-02 15:04:05")}
	return nil
}

func (m *StatisticsMgr) PopLease(leasePath string) (Statistics, error) {
	m.readLock.Lock()
	defer m.readLock.Unlock()
	res, prs := m.leaseStatistics[leasePath]
	if !prs {
		return Statistics{}, fmt.Errorf("No statistics counters for lease %s", leasePath)
	}
	delete(m.leaseStatistics, leasePath)
	return res, nil
}

func (m *StatisticsMgr) MergeIntoLeaseStatistics(leasePath string, other *Statistics) error {
	m.readLock.Lock()
	defer m.readLock.Unlock()
	c, prs := m.leaseStatistics[leasePath]
	if !prs {
		return fmt.Errorf("Statistics counters not found for lease %s", leasePath)
	}
	c.Publish.ChunksAdded += other.Publish.ChunksAdded
	c.Publish.ChunksDuplicated += other.Publish.ChunksDuplicated
	c.Publish.CatalogsAdded += other.Publish.CatalogsAdded
	c.Publish.UploadedBytes += other.Publish.UploadedBytes
	c.Publish.UploadedCatalogBytes += other.Publish.UploadedCatalogBytes
	m.leaseStatistics[leasePath] = c
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
