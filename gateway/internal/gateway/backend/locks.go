package backend

import "sync"

// NamedLocks provides a thread-safe map of named locks, used for locking
// repositories during critical operations (commits, GC, etc.)
type NamedLocks struct {
	locks sync.Map
}

// WithLock runs the given task, locking the "name" mutex for the
// duration of the task
func (l *NamedLocks) WithLock(name string, task func() error) error {
	m, _ := l.locks.LoadOrStore(name, &sync.Mutex{})
	mtx := m.(*sync.Mutex)
	mtx.Lock()
	defer mtx.Unlock()

	return task()
}
