/*
Copyright 2017 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Job E2E Test", func() {
	It("Schedule Job", func() {
		context := initTestContext()
		defer cleanupTestContext(context)
		rep := clusterSize(context, oneCPU)

		job := createJob(context, &jobSpec{
			name: "qj-1",
			tasks: []taskSpec{
				{
					img: defaultBusyBoxImage,
					req: oneCPU,
					min: rep,
					rep: rep,
				},
			},
			queue: defaultQueue1,
		})

		err := waitJobReady(context, job)
		Expect(err).NotTo(HaveOccurred())

		job2 := createJob(context, &jobSpec{
			name: "qj-2",
			tasks: []taskSpec{
				{
					img: defaultBusyBoxImage,
					req: oneCPU,
					min: rep-1,
					rep: rep,
				},
			},
			queue: defaultQueue2,
		})

		err = waitJobReady(context, job2)
		Expect(err).NotTo(HaveOccurred())
	})
})
