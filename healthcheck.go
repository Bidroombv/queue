package queue

import "sync/atomic"

type healthCheck int32

func (h *healthCheck) SetHealthy() {
	atomic.StoreInt32((*int32)(h), 1)
}

func (h *healthCheck) SetUnhealthy() {
	atomic.StoreInt32((*int32)(h), 0)
}

func (h *healthCheck) IsHealthy() bool {
	return atomic.LoadInt32((*int32)(h)) == 1
}
