package controller

type TaskJob interface {
	Running() bool
	Success() bool
	Start() error
	Stop()
	Error() error
}

type TaskController struct {
	jobs map[string]TaskJob
}

func NewTaskController() *TaskController {
	return &TaskController{jobs: make(map[string]TaskJob)}
}

func (t *TaskController) ContainTask(name string) bool {
	return t.jobs[name] != nil
}

func (t *TaskController) StartTask(name string, job TaskJob) error {
	t.jobs[name] = job
	return job.Start()
}

func (t *TaskController) StopTask(name string) {
	job := t.jobs[name]
	if job != nil {
		job.Stop()
	}
}

func (t *TaskController) GetTask(name string) TaskJob {
	return t.jobs[name]
}

func (t *TaskController) DeleteTask(name string) {
	t.StopTask(name)
	delete(t.jobs, name)
}
