package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type job func() error

type SlaveCondition int

var slaveState = map[SlaveCondition]string{
	1: "Ready",
	2: "Running",
	3: "Terminated",
	0: "Starting",
}

func (s SlaveCondition) String() string {
	return slaveState[s]
}

const (
	Ready      SlaveCondition = 1
	Running    SlaveCondition = 2
	Terminated SlaveCondition = 3
)

type Slave struct {
	ID          string
	state       SlaveCondition
	onDone      func(string, error)
	ctx         context.Context
	cancelFunc  context.CancelFunc
	mu          *sync.RWMutex
	stateChange chan SlaveCondition
	job         chan work
	jobID       string
	m           *Master
}

func NewSlave(m *Master) *Slave {
	ctx, cancel := context.WithCancel(context.Background())
	slave := &Slave{
		ID:          fmt.Sprintf("slave-id-%d", time.Now().UnixNano()),
		ctx:         ctx,
		cancelFunc:  cancel,
		mu:          &sync.RWMutex{},
		stateChange: make(chan SlaveCondition),
		job:         make(chan work),
		m:           m,
	}

	log.Printf("slave with id %s is %s: ---", slave.ID, slave.state.String())
	go slave.logs()
	go slave.process()

	time.Sleep(time.Millisecond * 20)
	slave.mu.Lock()
	slave.state = Ready
	slave.mu.Unlock()

	slave.stateChange <- Ready

	return slave
}

func (s *Slave) logs() {

	for state := range s.stateChange {
		select {
		case <-s.ctx.Done():
			return

		default:
			log.Println("slave id:", s.ID)
			log.Printf("slave state: %s", state.String())
			if s.jobID != "" {
				log.Println("currently on job: ", s.jobID)
			}
		}
	}
}

func (s *Slave) terminate() {
	s.mu.Lock()
	s.state = Terminated
	s.mu.Unlock()

	s.stateChange <- Terminated
	time.Sleep(time.Millisecond * 10)

	s.cancelFunc()
}

func (s *Slave) process() {
	for {
		select {
		case <-s.ctx.Done():
			return

		case work := <-s.job:
			s.mu.Lock()
			s.state = Running
			s.jobID = work.id
			s.mu.Unlock()

			s.stateChange <- Running

			err := work.job()
			time.Sleep(time.Millisecond * 50)
			if s.onDone != nil {
				s.onDone(s.jobID, err)
			}

			s.mu.Lock()
			s.state = Ready
			s.jobID = ""
			s.mu.Unlock()
			s.stateChange <- Ready
		}
	}
}

func (s *Slave) AddJob(work work) {
	log.Printf("Slave :%s received job: %s", s.ID, work.id)
	s.job <- work
}

func (s *Slave) OnDone(fn func(jobID string, err error)) {
	s.onDone = fn
}

func (s *Slave) GetState() SlaveCondition {
	s.mu.RLock()
	state := s.state
	s.mu.RUnlock()

	return state
}
