// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package taskrunner

import (
	"fmt"
	"strings"
	"sync"

	"github.com/gardener/gardener-extension-provider-gdc/pkg/errors"
)

// Reusable Task Runner Module for parallel task execution with ordered results and wait for all to complete.

// Result holds the outcome of a single task
type result struct {
	TaskName string
	Value    any
	Err      error
}

type task struct {
	name string
	fn   func() (any, error)
}

// Runner manages a collection of tasks to be run in parallel
type Runner struct {
	tasks []task
}

// NewRunner creates a new task runner
func NewRunner() *Runner {
	return &Runner{
		tasks: []task{},
	}
}

// AddTask registers a new task with the runner
func (r *Runner) AddTask(name string, fn func() (any, error)) {
	r.tasks = append(r.tasks, task{name: name, fn: fn})
}

// Run executes all registered tasks in parallel, waits for them to complete,
// and processes the results. It returns a map of successful results (by task name)
// and a single, combined error if any tasks failed.
func (r *Runner) Run() (map[string]any, error) {
	numTasks := len(r.tasks)
	if numTasks == 0 {
		return nil, nil
	}

	var wg sync.WaitGroup
	wg.Add(numTasks)

	resultsCh := make(chan result, numTasks)

	for _, task := range r.tasks {
		go func() {
			defer wg.Done()

			// Run the function and capture both value and error
			val, err := task.fn()
			resultsCh <- result{
				TaskName: task.name,
				Value:    val,
				Err:      err,
			}
		}()
	}

	// Wait for all goroutines to finish
	wg.Wait()
	close(resultsCh)

	var allErrors []string
	successResults := make(map[string]any)

	for res := range resultsCh {
		if res.Err != nil {
			allErrors = append(allErrors, res.Err.Error())
			continue
		}
		successResults[res.TaskName] = res.Value
	}

	if len(allErrors) > 0 {
		err := fmt.Errorf("%s", strings.Join(allErrors, ", "))
		return successResults, errors.DetermineError(err)
	}

	return successResults, nil
}
