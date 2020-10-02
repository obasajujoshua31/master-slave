package main

import (
	"fmt"
	"time"
)

func main() {
	start := time.Now()
	master := NewMaster(32)
	fmt.Println(master.ID)

	job := func() error {
		return nil
	}

	tasks := []*work{}
	for i := 0; i < 353; i++ {
		tasks = append(tasks, NewWork(job, low))
	}
	tasks2 := []*work{}
	for i := 0; i < 300; i++ {
		tasks2 = append(tasks2, NewWork(job, high))
	}

	master.AddTask(tasks)
	master.AddTask(tasks2)

	master.Finish()

	fmt.Println("Completed===++++++", time.Since(start).Seconds())

	master.terminate()
	time.Sleep(time.Millisecond * 10)

	fmt.Println("slaves ====", len(master.GetSlaves()))
}
