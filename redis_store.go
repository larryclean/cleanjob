package cleanjob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

type RedisStore struct {
	pool    *redis.Pool
	watch   WatchFunc
	message chan message
}
type message struct {
	Event string
	Key   string
}

var (
	channel           = []string{AddEvent, DeleteEvent, KillEvent}
	healthCheckPeriod = time.Minute
	addJobSource      = `
redis.call("SADD", KEYS[1], KEYS[2])
redis.call("SET", KEYS[2], ARGV[1])
	`
	deleteJobSource = `
redis.call("SREM", KEYS[1], KEYS[2])
redis.call("DEL", KEYS[2])
	`
	lockSource = `
		local ex = nil
		ex=redis.call("SETNX",KEYS[1],ARGV[1])
		redis.call("expire",KEYS[1],ARGV[2])
		return ex
	`
	redisSetKey  = "job:set"
	redisInfoKey = "job:hash:%s"
	redisLockKey = "job:lock:%s"
)

func NewRedisStore(address string, options ...redis.DialOption) (*RedisStore, error) {
	re := &RedisStore{
		message: make(chan message),
	}
	if err := re.init(address, options...); err != nil {
		return nil, err
	}
	go func() {
	start:
		if err := re.run(); err != nil {
		init:
			time.Sleep(5 * time.Second)
			if err := re.init(address, options...); err != nil {
				goto init
			}
			goto start
		}

	}()
	go re.getMessage()
	return re, nil
}
func (s *RedisStore) init(address string, options ...redis.DialOption) (err error) {
	pool := &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 300 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", address, options...)
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
	c := pool.Get()
	defer c.Close()
	_, err = c.Do("PING")
	if err != nil {
		return
	}
	s.pool = pool
	return
}
func (s *RedisStore) run() error {
	c := s.pool.Get()
	psc := redis.PubSubConn{Conn: c}
	if err := psc.Subscribe(redis.Args{}.AddFlat(channel)...); err != nil {
		return err
	}
	done := make(chan error, 1)
	// Start a goroutine to receive notifications from the server.
	go func() {
		defer c.Close()
		for {
			n := psc.Receive()

			switch n := n.(type) {
			case error:
				done <- n
				return
			case redis.Message:
				s.message <- message{
					Event: n.Channel,
					Key:   string(n.Data),
				}
			case redis.Subscription:
				//TODO
				if n.Count == 0 {
					if err := psc.Subscribe(redis.Args{}.AddFlat(channel)...); err != nil {
						return
					}
				}
			}
		}
	}()

	ticker := time.NewTicker(healthCheckPeriod)
	defer ticker.Stop()
loop:
	for {
		select {
		case err := <-done:
			return err
		case <-ticker.C:
			if err := psc.Ping(""); err != nil {
				break loop
			}
		}
	}
	return nil
}

func (s *RedisStore) Load() ([]*Job, error) {
	jobs := make([]*Job, 0)
	c := s.pool.Get()
	defer c.Close()
	keys, err := redis.Strings(c.Do("SMEMBERS", redisSetKey))
	if err != nil {
		return nil, err
	}
	for _, k := range keys {
		str, err := redis.String(c.Do("GET", k))
		if err != nil {
			return nil, err
		}
		var job Job
		_ = json.Unmarshal([]byte(str), &job)
		jobs = append(jobs, &job)
	}
	return jobs, nil
}
func (s *RedisStore) Save(job *Job) error {
	//s.list[job.Key] = job
	body, _ := json.Marshal(job)
	c := s.pool.Get()
	defer c.Close()
	addJob := redis.NewScript(2, addJobSource)
	addJob.Load(c)
	_, err := addJob.Do(c, redisSetKey, fmt.Sprintf(redisInfoKey, job.Key), body)
	if err != nil {
		return err
	}
	err = s.publish(AddEvent, job.Key)
	if err != nil {
		return err
	}
	return nil
}
func (s *RedisStore) Delete(job *Job) (err error) {
	c := s.pool.Get()
	defer c.Close()
	addJob := redis.NewScript(2, deleteJobSource)
	addJob.Load(c)
	_, err = addJob.Do(c, redisSetKey, fmt.Sprintf(redisInfoKey, job.Key))
	if err != nil {
		return err
	}
	err = s.publish(DeleteEvent, job.Key)
	if err != nil {
		return err
	}
	return nil
}

func (s *RedisStore) Lock(ex *ExecuteContext) error {
	ex.CancelCtx, ex.CancelFunc = context.WithCancel(context.TODO())
	c := s.pool.Get()
	defer c.Close()
	lockJob := redis.NewScript(1, lockSource)
	lockJob.Load(c)
	res, err := lockJob.Do(c, fmt.Sprintf(redisLockKey, ex.Job.Key), ex.Job.Key, 5)
	if err != nil {
		return err
	}
	if res.(int64) == 1 {
		go func() {
			ticker := time.NewTicker(4 * time.Second)
			defer ticker.Stop()
			conn := s.pool.Get()
			defer conn.Close()
			for {

				select {
				case <-ticker.C:
					conn.Do("EXPIRE", fmt.Sprintf(redisLockKey, ex.Job.Key), 5)
				case <-ex.CancelCtx.Done():
					goto END
				}
			}
		END:
		}()
		ex.IsLock = true
		return nil
	}
	return errors.New("已经被锁")
}

func (s *RedisStore) UnLock(ex *ExecuteContext) error {
	if ex.IsLock {
		ex.CancelFunc()
		conn := s.pool.Get()
		defer conn.Close()
		conn.Do("DEL", fmt.Sprintf(redisLockKey, ex.Job.Key))
	}
	return nil
}
func (s *RedisStore) Watch(f WatchFunc) {
	s.watch = f
}

func (s *RedisStore) getMessage() {
	for {
		msg := <-s.message
		if s.watch != nil {
			if job, err := s.getJob(fmt.Sprintf(redisInfoKey, msg.Key)); err == nil {
				s.watch(msg.Event, job)
			}

		}
	}
}
func (s *RedisStore) publish(ch, key string) error {
	c := s.pool.Get()
	defer c.Close()
	_, err := c.Do("PUBLISH", ch, key)
	return err
}
func (s *RedisStore) getJob(key string) (*Job, error) {
	c := s.pool.Get()
	defer c.Close()
	str, err := redis.String(c.Do("GET", key))
	if err != nil {
		return nil, err
	}
	var job Job
	_ = json.Unmarshal([]byte(str), &job)
	return &job, nil
}
