package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	ck kvtest.IKVClerk

	OwnerID string
	l       string
}

func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	lk.l = l
	lk.OwnerID = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	for {
		v, ver, err := lk.ck.Get(lk.l)
		switch err {
		case rpc.ErrNoKey:
			re := lk.UpdateLock(lk.OwnerID, ver)
			if re == true {
				return
			}
		case rpc.OK:
			if v != "" {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			re := lk.UpdateLock(lk.OwnerID, ver)
			if re == true {
				return
			}
		}
	}
}

func (lk *Lock) Release() {
	for {
		v, ver, err := lk.ck.Get(lk.l)
		switch err {
		case rpc.OK:
			if v == lk.OwnerID {
				re := lk.UpdateLock("", ver)
				if re == true {
					return
				}
				continue
			}
			return
		default:
			return
		}
	}
}

func (lk *Lock) UpdateLock(value string, ver rpc.Tversion) bool {
	ret := lk.ck.Put(lk.l, value, ver)
	if ret == rpc.OK {
		return true
	}

	if ret == rpc.ErrMaybe {
		v2, _, err2 := lk.ck.Get(lk.l)
		if err2 == rpc.OK && v2 == value {
			return true
		}
	}

	return false
}
