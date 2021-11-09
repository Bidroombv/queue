package queue

import "sync/atomic"

type atomicBool int32

func (a *atomicBool) SetTrue() {
	atomic.StoreInt32((*int32)(a), 1)
}

func (a *atomicBool) SetFalse() {
	atomic.StoreInt32((*int32)(a), 0)
}

func (a *atomicBool) Value() bool {
	return atomic.LoadInt32((*int32)(a)) == 1
}
