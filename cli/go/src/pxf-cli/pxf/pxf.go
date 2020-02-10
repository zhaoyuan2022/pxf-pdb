package pxf

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
)

type EnvVar string

const (
	Gphome  EnvVar = "GPHOME"
	PxfConf EnvVar = "PXF_CONF"
)

type MessageType int

const (
	Success MessageType = iota
	Status
	Error
	Warning
)

type Command struct {
	commandName string
	messages    map[MessageType]string
	whereToRun  int
	envVars     []EnvVar
	warn        bool // whether the command requires a warning/prompt
}

func (c *Command) Name() string {
	return c.commandName
}

func (c *Command) requiredEnvVars() []EnvVar {
	return c.envVars
}

func (c *Command) WhereToRun() int {
	return c.whereToRun
}

func (c *Command) Messages(messageType MessageType) string {
	return c.messages[messageType]
}

func (c *Command) Warn(input io.Reader) error {
	if c.warn == false || promptUser(input, c.Messages(Warning)) {
		return nil
	}
	return fmt.Errorf("pxf %s cancelled", c.commandName)
}

func (c *Command) GetFunctionToExecute() (func(string) string, error) {
	inputs, err := makeValidCliInputs(c)
	if err != nil {
		return nil, err
	}

	switch c.commandName {
	case "sync":
		return func(hostname string) string {
			return fmt.Sprintf(
				"rsync -az -e 'ssh -o StrictHostKeyChecking=no' '%s/conf' '%s/lib' '%s/servers' '%s:%s'",
				inputs[PxfConf],
				inputs[PxfConf],
				inputs[PxfConf],
				hostname,
				inputs[PxfConf])
		}, nil
	default:
		pxfCommand := ""
		if inputs[PxfConf] != "" {
			pxfCommand += "PXF_CONF=" + inputs[PxfConf] + " "
		}
		pxfCommand += inputs[Gphome] + "/pxf/bin/pxf" + " " + c.commandName
		if c.commandName == "reset" {
			pxfCommand += " --force" // there is a prompt for local reset as well
		}
		return func(_ string) string { return pxfCommand }, nil
	}
}

func promptUser(input io.Reader, prompt string) bool {
	reader := bufio.NewReader(input)
	fmt.Printf(prompt)
	text, _ := reader.ReadString('\n')
	text = strings.TrimRight(text, "\r\n")
	return strings.ToLower(text) == "y"
}

type CommandName string

const (
	Init       = "init"
	Start      = "start"
	Stop       = "stop"
	Sync       = "sync"
	Statuses   = "status"
	Reset      = "reset"
	Restart    = "restart"
)

var (
	InitCommand = Command{
		commandName: Init,
		messages: map[MessageType]string{
			Success: "PXF initialized successfully on %d out of %d hosts\n",
			Status:  "Initializing PXF on master and %d other hosts...\n",
			Error:   "PXF failed to initialize on %d out of %d hosts\n",
		},
		warn:       false,
		envVars:    []EnvVar{Gphome, PxfConf},
		whereToRun: cluster.ON_HOSTS_AND_MASTER,
	}
	StartCommand = Command{
		commandName: Start,
		messages: map[MessageType]string{
			Success: "PXF started successfully on %d out of %d hosts\n",
			Status:  "Starting PXF on %d segment hosts...\n",
			Error:   "PXF failed to start on %d out of %d hosts\n",
		},
		warn:       false,
		envVars:    []EnvVar{Gphome},
		whereToRun: cluster.ON_HOSTS,
	}
	StopCommand = Command{
		commandName: Stop,
		messages: map[MessageType]string{
			Success: "PXF stopped successfully on %d out of %d hosts\n",
			Status:  "Stopping PXF on %d segment hosts...\n",
			Error:   "PXF failed to stop on %d out of %d hosts\n",
		},
		warn:       false,
		envVars:    []EnvVar{Gphome},
		whereToRun: cluster.ON_HOSTS,
	}
	SyncCommand = Command{
		commandName: Sync,
		messages: map[MessageType]string{
			Success: "PXF configs synced successfully on %d out of %d hosts\n",
			Status:  "Syncing PXF configuration files to %d hosts...\n",
			Error:   "PXF configs failed to sync on %d out of %d hosts\n",
		},
		warn:       false,
		envVars:    []EnvVar{PxfConf},
		whereToRun: cluster.ON_MASTER_TO_HOSTS,
	}
	StatusCommand = Command{
		commandName: Statuses,
		messages: map[MessageType]string{
			Success: "PXF is running on %d out of %d hosts\n",
			Status:  "Checking status of PXF servers on %d hosts...\n",
			Error:   "PXF is not running on %d out of %d hosts\n",
		},
		warn:       false,
		envVars:    []EnvVar{Gphome},
		whereToRun: cluster.ON_HOSTS,
	}
	ResetCommand = Command{
		commandName: Reset,
		messages: map[MessageType]string{
			Success: "PXF has been reset on %d out of %d hosts\n",
			Status:  "Resetting PXF on master and %d other hosts...\n",
			Error:   "Failed to reset PXF on %d out of %d hosts\n",
			Warning: "Ensure your PXF cluster is stopped before continuing. " +
				"This is a destructive action. Press y to continue:\n",
		},
		warn:       true,
		envVars:    []EnvVar{Gphome},
		whereToRun: cluster.ON_HOSTS_AND_MASTER,
	}
	RestartCommand = Command{
		commandName: Restart,
		messages: map[MessageType]string{
			Success: "PXF has restarted successfully on %d out of %d hosts\n",
			Status:  "Restarting PXF on %d segment hosts...\n",
			Error:   "Failed to restart PXF on %d out of %d hosts\n",
		},
		warn:       false,
		envVars:    []EnvVar{Gphome},
		whereToRun: cluster.ON_HOSTS,
	}
)

func makeValidCliInputs(c *Command) (map[EnvVar]string, error) {
	envVars := make(map[EnvVar]string)
	for _, e := range c.requiredEnvVars() {
		val, err := validateEnvVar(e)
		if err != nil {
			return nil, err
		}
		envVars[e] = val
	}
	return envVars, nil
}

func validateEnvVar(envVar EnvVar) (string, error) {
	envVarValue, isEnvVarSet := os.LookupEnv(string(envVar))
	if !isEnvVarSet {
		return "", errors.New(string(envVar) + " must be set")
	}
	if envVarValue == "" {
		return "", errors.New(string(envVar) + " cannot be blank")
	}
	return envVarValue, nil
}
