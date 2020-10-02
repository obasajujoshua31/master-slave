package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

type priority int

const (
	unassigned priority = 0
	high       priority = 2
	low        priority = 1
)

type work struct {
	job      job
	priority priority
	id       string
}

type queue struct {
	work []*work
}

func (q *queue) Enqueue(work *work) {
	q.work = append(q.work, work)
}

func (q *queue) Dequeue() *work {
	if len(q.work) == 0 {
		return nil
	}

	work := q.work[0]
	q.work = q.work[1:]
	return work
}

func (q *queue) IsEmpty() bool {
	return len(q.work) == 0
}

func NewWork(job job, priority priority) *work {
	return &work{
		job:      job,
		priority: priority,
		id:       fmt.Sprintf("job-%d", time.Now().UnixNano()),
	}
}

type Master struct {
	ID        string
	work      chan work
	slaves    map[string]*Slave
	ctx       context.Context
	cancel    context.CancelFunc
	tasks     chan []*work
	completed chan bool
	queue     *queue
}

func NewMaster(numOfSlaves int) *Master {
	ctx, cancel := context.WithCancel(context.Background())
	master := &Master{
		ID:        fmt.Sprintf("master-%d", time.Now().UnixNano()),
		work:      make(chan work),
		slaves:    make(map[string]*Slave),
		ctx:       ctx,
		cancel:    cancel,
		tasks:     make(chan []*work),
		completed: make(chan bool),
		queue:     &queue{},
	}

	log.Printf("Starting %d worker slaves ----", numOfSlaves)
	for i := 0; i < numOfSlaves; i++ {
		slave := NewSlave(master)
		master.AddSlave(slave)
	}

	go master.CoordinateTasks()

	return master
}

func (m *Master) AddSlave(s *Slave) error {
	_, ok := m.slaves[s.ID]
	if ok {
		return errors.New("slave already added")
	}

	m.slaves[s.ID] = s
	return nil
}

func (m *Master) notify() {

}

func (m *Master) assignTask(work work, s *Slave) error {
	slave, ok := m.slaves[s.ID]
	if ok {
		slave.AddJob(work)
		return nil
	}

	return errors.New("unable to assign task to slave")

}

func (m *Master) GetSlaves() []*Slave {
	slaves := []*Slave{}

	for _, slave := range m.slaves {
		slaves = append(slaves, slave)
	}

	return slaves
}

func (m *Master) GetAvailableSlaves() []*Slave {
	slaves := []*Slave{}

	for _, slave := range m.slaves {
		if slave.GetState() == Ready {
			slaves = append(slaves, slave)
		}
	}

	return slaves
}

func (m *Master) AddTask(tasks []*work) {
	for _, work := range tasks {
		m.queue.Enqueue(work)
	}
}

func (m *Master) CoordinateTasks() {
	for {
		select {
		case <-m.ctx.Done():
			return

		default:
			task := m.queue.Dequeue()
			if task != nil {
				for len(m.GetAvailableSlaves()) == 0 && task.priority != high {
					// Wait for slaves to be ready
					fmt.Println("<--- all slaves now busy === waiting for slaves to be ready --->")
					time.Sleep(time.Millisecond * 15)
				}

				slave := m.GetNextAvailableSlave()
				if slave == nil {
					fmt.Println("<---- Task priority is high is creating new slave --->")
					slave = m.NewSlave()
				}

				m.assignTask(*task, slave)
			}
		}
	}
}

func (m *Master) terminate() {
	for _, slave := range m.slaves {
		slave.terminate()
	}

	m.cancel()
}

func (m *Master) NewSlave() *Slave {
	slave := NewSlave(m)

	m.AddSlave(slave)
	return slave
}

func (m *Master) GetNextAvailableSlave() *Slave {
	for _, slave := range m.slaves {
		if slave.GetState() == Ready {
			return slave
		}
	}

	return nil
}

func (m *Master) Finish() {
	ticker := time.NewTicker(time.Millisecond * 5)

	for range ticker.C {
		if m.queue.IsEmpty() {
			return
		}
	}
}
