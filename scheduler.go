package cleanjob

import (
	"time"

	"github.com/robfig/cron/v3"

	"github.com/gorhill/cronexpr"
)

//Scheduler 执行计划
type Scheduler struct {
	Job      *Job
	EntryID  cron.EntryID
	store    IStore
	ex       Execute
	planTime time.Time
}

//Next 计划的下一次执行时间
func (s *Scheduler) Next(t time.Time) time.Time {
	exx := cronexpr.MustParse(s.Job.CronExpr)
	s.planTime = exx.Next(t)
	return s.planTime
}
func (s *Scheduler) Run() {
	ex := &ExecuteContext{
		Job: s.Job,
	}
	if err := s.store.Lock(ex); err != nil {
		return
	} else {
		s.ex.Execute(ex)
		s.store.UnLock(ex)
	}
	if s.Next(time.Now()).IsZero() {
		s.store.Delete(s.Job)
	}
}
