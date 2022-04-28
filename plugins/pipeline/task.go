package main

import (
	"hash/fnv"
	"sort"
	"sync"
)

type Task struct {
	name string
	mode int
}

type BackupQueue struct {
	tasks []*Task
	lock  sync.Mutex
	self  string
	ips   []string
	idx   int
}

func NewTask(filename string) *Task {
	task := Task{
		name: filename,
	}
	return &task
}

func NewTaskWithParam(filename string, mode int) *Task {
	return &Task{
		name: filename,
		mode: mode,
	}
}

func NewSingleNodeDispatchTask() *BackupQueue {
	queue := &BackupQueue{
		tasks: make([]*Task, 0, 1024),
		idx:   0,
	}
	return queue
}

func NewDispatchTask(self string, ips []string) *BackupQueue {
	sort.Strings(ips)
	var idx int
	for i, s := range ips {
		if s == self {
			idx = i
			break
		}
	}
	queue := &BackupQueue{
		tasks: make([]*Task, 0, 1024),
		self:  self,
		ips:   ips,
		idx:   idx,
	}
	return queue
}

func (queue *BackupQueue) Get() *Task {
	queue.lock.Lock()
	l := len(queue.tasks)
	if l == 0 {
		queue.lock.Unlock()
		return nil
	}
	task := queue.tasks[0]
	queue.tasks = queue.tasks[1:]
	queue.lock.Unlock()
	return task
}

func (queue *BackupQueue) Length() int {
	queue.lock.Lock()
	l := len(queue.tasks)
	queue.lock.Unlock()
	return l
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (queue *BackupQueue) AddTask(task *Task) {
	key := int(hash(task.name))
	l := len(queue.ips)
	if l > 1 {
		if key%len(queue.ips) != queue.idx {
			return
		}
	}
	queue.lock.Lock()
	queue.tasks = append(queue.tasks, task)
	queue.lock.Unlock()
}
