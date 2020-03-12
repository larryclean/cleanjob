package cleanjob

import (
	"errors"
	"sync"
)

type Memory struct {
	list  map[string]*Job
	lock  sync.Map
	watch WatchFunc
}

func NewMemory() *Memory {
	return &Memory{
		list: make(map[string]*Job),
		lock: sync.Map{},
	}
}
func (m *Memory) Test(job *Job) {
	m.list[job.Key] = job
}
func (m *Memory) Load() ([]*Job, error) {
	jobs := make([]*Job, 0)
	for _, v := range m.list {
		jobs = append(jobs, v)
	}
	return jobs, nil
}
func (m *Memory) Save(job *Job) error {
	m.list[job.Key] = job
	m.watch(AddEvent, job)
	return nil
}
func (m *Memory) Delete(job *Job) error {
	delete(m.list, job.Key)
	m.watch(DeleteEvent, job)
	return nil
}

func (m *Memory) Lock(ex *ExecuteContext) error {
	if _, ok := m.lock.Load(ex.Job.Key); ok {
		return errors.New("locked")
	}
	m.lock.Store(ex.Job.Key, ex.Job.Key)
	return nil
}

func (m *Memory) UnLock(ex *ExecuteContext) error {
	if _, ok := m.lock.Load(ex.Job.Key); ok {
		m.lock.Delete(ex.Job.Key)
	}
	return nil
}
func (m *Memory) Watch(f WatchFunc) {
	m.watch = f
}
