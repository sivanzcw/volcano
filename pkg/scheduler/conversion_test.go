/*
Copyright 2019 The Kubernetes Authors.

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

package scheduler

import (
	"testing"
)

func TestCovertSchedulerConfToLatestSchedulerConf(t *testing.T) {
	cases := []struct {
		inputConf    string
		ExpectedConf string
	}{
		{
			inputConf: `actions: "allocate, backfill"
tiers:
- plugins:
  - name: priority
  - name: gang
  - name: conformance
- plugins:
  - name: drf
  - name: predicates
  - name: proportion
  - name: nodeorder
`,
			ExpectedConf: `version: v2
actions:
- name: allocate
  arguments: {}
- name: backfill
  arguments: {}
tiers:
- plugins:
  - name: priority
    enableJobOrder: null
    enableNamespaceOrder: null
    enableJobReady: null
    enableJobPipelined: null
    enableTaskOrder: null
    enablePreemptable: null
    enableReclaimable: null
    enableQueueOrder: null
    enablePredicate: null
    enableNodeOrder: null
    arguments: {}
  - name: gang
    enableJobOrder: null
    enableNamespaceOrder: null
    enableJobReady: null
    enableJobPipelined: null
    enableTaskOrder: null
    enablePreemptable: null
    enableReclaimable: null
    enableQueueOrder: null
    enablePredicate: null
    enableNodeOrder: null
    arguments: {}
  - name: conformance
    enableJobOrder: null
    enableNamespaceOrder: null
    enableJobReady: null
    enableJobPipelined: null
    enableTaskOrder: null
    enablePreemptable: null
    enableReclaimable: null
    enableQueueOrder: null
    enablePredicate: null
    enableNodeOrder: null
    arguments: {}
- plugins:
  - name: drf
    enableJobOrder: null
    enableNamespaceOrder: null
    enableJobReady: null
    enableJobPipelined: null
    enableTaskOrder: null
    enablePreemptable: null
    enableReclaimable: null
    enableQueueOrder: null
    enablePredicate: null
    enableNodeOrder: null
    arguments: {}
  - name: predicates
    enableJobOrder: null
    enableNamespaceOrder: null
    enableJobReady: null
    enableJobPipelined: null
    enableTaskOrder: null
    enablePreemptable: null
    enableReclaimable: null
    enableQueueOrder: null
    enablePredicate: null
    enableNodeOrder: null
    arguments: {}
  - name: proportion
    enableJobOrder: null
    enableNamespaceOrder: null
    enableJobReady: null
    enableJobPipelined: null
    enableTaskOrder: null
    enablePreemptable: null
    enableReclaimable: null
    enableQueueOrder: null
    enablePredicate: null
    enableNodeOrder: null
    arguments: {}
  - name: nodeorder
    enableJobOrder: null
    enableNamespaceOrder: null
    enableJobReady: null
    enableJobPipelined: null
    enableTaskOrder: null
    enablePreemptable: null
    enableReclaimable: null
    enableQueueOrder: null
    enablePredicate: null
    enableNodeOrder: null
    arguments: {}
`,
		},
		{
			inputConf: `actions:
- name: allocate
- name: backfill
tiers:
- plugins:
  - name: priority
  - name: gang
  - name: conformance
- plugins:
  - name: drf
  - name: predicates
  - name: proportion
  - name: nodeorder
`,
			ExpectedConf: `actions:
- name: allocate
- name: backfill
tiers:
- plugins:
  - name: priority
  - name: gang
  - name: conformance
- plugins:
  - name: drf
  - name: predicates
  - name: proportion
  - name: nodeorder
`,
		},
	}

	for i, test := range cases {
		out, err := CovertSchedulerConfToLatestSchedulerConf(test.inputConf)
		if err != nil {
			t.Errorf("Failed test case #%d, function return err %v", i, err)
		}
		if out != test.ExpectedConf {
			t.Errorf("Failed test case #%d, expected: %#v, got %#v", i, test.ExpectedConf, out)
		}
	}
}
