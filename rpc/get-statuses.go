package rpc

import (
	"os"
	engine "pendulev2/task-engine"
	"syscall"

	pcommon "github.com/pendulea/pendule-common"

	"github.com/shirou/gopsutil/v3/mem"
)

func (s *RPCService) GetStatus(payload pcommon.RPCRequestPayload) (*pcommon.GetStatusResponse, error) {
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

	r := &pcommon.GetStatusResponse{
		CountPendingTasks:  engine.Engine.CountQueued(),
		CountRunningTasks:  engine.Engine.CountRunning(),
		CSVStatuses:        status,
		HTMLStatuses:       engine.Engine.GetHTMLStatuses(),
		CPUCount:           12,
		AvailableMemory:    v.Available,
		AvailableDiskSpace: diskSize,
		MinTimeframe:       pcommon.Env.MIN_TIME_FRAME.Milliseconds(),
	}

	return r, nil
}
