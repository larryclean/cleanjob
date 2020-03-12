package cleanjob

import (
	"context"
	"encoding/json"
	"time"
)

//Job 脚本实体
type Job struct {
	ExecuteKey string `json:"execute_key"` //执行器的key
	Key        string `json:"key"`         //脚本主键
	Data       []byte `json:"data"`        //脚本实体
	CronExpr   string `json:"cron_expr"`   //触发表达式
}

//NewJob 创建脚本
func NewJob(execute, key string) *Job {
	return &Job{ExecuteKey: execute, Key: key}
}

func (j *Job) WithData(data []byte) {
	j.Data = data
}
func (j *Job) WithStructToData(data interface{}) {
	b, _ := json.Marshal(data)
	j.Data = b
}
func (j *Job) WithCronTime(t time.Time) {
	j.CronExpr = t.Format("05 04 15 02 1 ? 2006")
}
func (j *Job) String() string {
	return string(j.Data)
}
func (j *Job) Struct(bean interface{}) {
	_ = json.Unmarshal(j.Data, bean)
	return
}

type WatchFunc func(event string, job *Job)

var (
	AddEvent    = "add"
	DeleteEvent = "delete"
	KillEvent   = "kill"
)

//IStore 存储
type IStore interface {
	Save(job *Job) error
	Delete(job *Job) error
	Lock(job *ExecuteContext) error
	UnLock(job *ExecuteContext) error
	Load() ([]*Job, error)
	Watch(WatchFunc)
}

type Execute interface {
	Execute(ctx *ExecuteContext) error
}

type ExecuteContext struct {
	Job        *Job
	CancelCtx  context.Context
	CancelFunc context.CancelFunc
	IsLock     bool
}
