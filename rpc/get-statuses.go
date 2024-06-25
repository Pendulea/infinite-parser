package rpc

import (
	"os"
	engine "pendulev2/task-engine"
	"syscall"

	pcommon "github.com/pendulea/pendule-common"

	"github.com/shirou/gopsutil/v3/mem"
)

type GetStatusResponse struct {
	CountPendingTasks  int                 `json:"count_pending_tasks"`
	CountRunningTasks  int                 `json:"count_running_tasks"`
	CSVStatuses        []engine.CSVStatus  `json:"csv_statuses"`
	HTMLStatuses       []engine.StatusHTML `json:"html_statuses"`
	CPUCount           int                 `json:"cpu_count"`
	AvailableMemory    uint64              `json:"available_memory"`
	AvailableDiskSpace uint64              `json:"available_disk_space"`
}

func (s *RPCService) GetStatus(payload pcommon.RPCRequestPayload) (*GetStatusResponse, error) {
	// start := time.Now()
	status, err := engine.GetCSVList()
	if err != nil {
		return nil, err
	}
	v, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}

	var diskSize uint64 = 0
	if _, err := os.Stat("/mnt/ethereum"); err == nil {
		var stat syscall.Statfs_t
		syscall.Statfs("/mnt/ethereum", &stat)
		diskSize = stat.Bavail * uint64(stat.Bsize)
	}

	r := &GetStatusResponse{
		CountPendingTasks:  engine.Engine.CountQueued(),
		CountRunningTasks:  engine.Engine.CountRunning(),
		CSVStatuses:        status,
		HTMLStatuses:       engine.Engine.GetHTMLStatuses(),
		CPUCount:           12,
		AvailableMemory:    v.Available,
		AvailableDiskSpace: diskSize,
	}

	// fmt.Println("GetStatus took", time.Since(start))
	return r, nil
}
