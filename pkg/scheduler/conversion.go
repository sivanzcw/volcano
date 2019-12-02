package scheduler

import (
	"fmt"
	"strings"

	yaml "gopkg.in/yaml.v2"

	"volcano.sh/volcano/pkg/scheduler/conf"
)

// CovertSchedulerConfToLatestSchedulerConf covert scheduler configuration from default to latest
func CovertSchedulerConfToLatestSchedulerConf(c string) (string, error) {
	buf := make([]byte, len(c))
	copy(buf, c)

	schedulerConf := &conf.SchedulerConfiguration{}
	if err := yaml.Unmarshal(buf, schedulerConf); err == nil {
		return c, nil
	}

	v1SchedulerConf := &conf.V1SchedulerConfiguration{}
	if err := yaml.Unmarshal(buf, v1SchedulerConf); err == nil {
		CovertV1SchedulerConfToV2SchedulerConf(v1SchedulerConf, schedulerConf)
		return marshalSchedulerConf(schedulerConf)
	}

	return "", fmt.Errorf("Unknown scheduler config %s", c)
}

// CovertV1SchedulerConfToV2SchedulerConf covert scheduler configuration from v1 version to v2 version
func CovertV1SchedulerConfToV2SchedulerConf(in *conf.V1SchedulerConfiguration, out *conf.SchedulerConfiguration) {
	out.Version = conf.SchedulerConfigVersionV2

	out.Tiers = in.Tiers

	actions := strings.Split(in.Actions, ",")
	for _, action := range actions {
		out.Actions = append(out.Actions, conf.Action{
			Name: strings.TrimSpace(action),
		})
	}
}
