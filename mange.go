package cleanjob

import (

	"github.com/robfig/cron/v3"
)

//Manage 脚本管理
type Manage struct {
	Store  IStore
	cron   *cron.Cron
	jobs   map[string]cron.EntryID
	exList map[string]Execute
}

func NewManage(store IStore) *Manage {
	m := &Manage{
		Store:  store,
		jobs:   make(map[string]cron.EntryID),
		exList: make(map[string]Execute),
	}
	m.cron = cron.New(cron.WithSeconds())
	m.cron.Start()
	m.Store.Watch(m.watch)
	return m
}
func (m *Manage) Run() {
	data, _ := m.Store.Load()
	for _, v := range data {
		m.addScheduler(v)
	}
}
func (m *Manage) watch(event string, job *Job) {
	var (
		id  cron.EntryID
		has bool
	)
	if id, has = m.jobs[job.Key]; has {
		m.cron.Remove(id)
		delete(m.jobs, job.Key)
	}
	switch event {
	case AddEvent:
		m.addScheduler(job)
	case DeleteEvent, KillEvent:
		//m.cron.Remove(id)
	}
}
func (m *Manage) addScheduler(job *Job) {
	plan := &Scheduler{
		Job:   job,
		store: m.Store,
		ex:    m.exList[job.ExecuteKey],
	}
	plan.EntryID = m.cron.Schedule(plan, plan)
	m.jobs[job.Key] = plan.EntryID
}

//Load 获取所有脚本
func (m *Manage) Load() ([]*Job, error) {
	return m.Store.Load()
}

//Save 保存
func (m *Manage) Save(job *Job) error {
	return m.Store.Save(job)
}

//Delete  删除
func (m *Manage) Delete(job *Job) error {
	return m.Store.Delete(job)
}
func (m *Manage) On(key string, execute Execute) {
	m.exList[key] = execute
}
