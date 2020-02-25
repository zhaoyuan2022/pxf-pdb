package cmd

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/spf13/cobra"
)

// ClusterData is exported for testing
type ClusterData struct {
	Cluster    *cluster.Cluster
	Output     *cluster.RemoteOutput
	NumHosts   int
	connection *dbconn.DBConn
}

func createCobraCommand(use string, short string, cmd *command) *cobra.Command {
	if cmd == nil {
		return &cobra.Command{Use: use, Short: short}
	}
	return &cobra.Command{
		Use:   use,
		Short: short,
		Run: func(cobraCmd *cobra.Command, args []string) {
			clusterData, err := doSetup(cmd)
			if err == nil {
				err = clusterRun(cmd, clusterData)
			}
			exitWithReturnCode(err)
		},
	}
}

var (
	clusterCmd = createCobraCommand("cluster", "Perform <command> on each segment host in the cluster", nil)
	initCmd    = createCobraCommand("init", "Initialize the PXF server instances on master, standby master, and all segment hosts", &InitCommand)
	startCmd   = createCobraCommand("start", "Start the PXF server instances on all segment hosts", &StartCommand)
	stopCmd    = createCobraCommand("stop", "Stop the PXF server instances on all segment hosts", &StopCommand)
	statusCmd  = createCobraCommand("status", "Get status of PXF servers on all segment hosts", &StatusCommand)
	syncCmd    = createCobraCommand("sync", "Sync PXF configs from master to standby master and all segment hosts. Use --delete to delete extraneous remote files", &SyncCommand)
	resetCmd   = createCobraCommand("reset", "Reset PXF (undo initialization) on all segment hosts", &ResetCommand)
	restartCmd = createCobraCommand("restart", "Restart the PXF server on all segment hosts", &RestartCommand)
	// DeleteOnSync is a boolean for determining whether to use rsync with --delete, exported for tests
	DeleteOnSync bool
)

func init() {
	rootCmd.AddCommand(clusterCmd)
	clusterCmd.AddCommand(initCmd)
	clusterCmd.AddCommand(startCmd)
	clusterCmd.AddCommand(stopCmd)
	clusterCmd.AddCommand(statusCmd)
	syncCmd.Flags().BoolVarP(&DeleteOnSync, "delete", "d", false, "delete extraneous files on remote host")
	clusterCmd.AddCommand(syncCmd)
	clusterCmd.AddCommand(resetCmd)
	clusterCmd.AddCommand(restartCmd)
}

func exitWithReturnCode(err error) {
	if err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

// CountHostsExcludingMaster is exported for testing
func (c *ClusterData) CountHostsExcludingMaster() error {
	hostSegMap := make(map[string]int, 0)
	for contentID, seg := range c.Cluster.Segments {
		if contentID == -1 {
			continue
		}
		hostSegMap[seg.Hostname]++
	}
	c.NumHosts = len(hostSegMap)
	return nil
}

// GenerateStatusReport is exported for testing
func GenerateStatusReport(cmd *command, clusterData *ClusterData) string {
	cmdMsg := fmt.Sprintf(cmd.messages[status], clusterData.NumHosts)
	gplog.Info(cmdMsg)
	return cmdMsg
}

// GenerateOutput is exported for testing
func GenerateOutput(cmd *command, clusterData *ClusterData) error {
	numHosts := len(clusterData.Output.Stdouts)
	numErrors := clusterData.Output.NumErrors
	if numErrors == 0 {
		gplog.Info(cmd.messages[success], numHosts-numErrors, numHosts)
		return nil
	}
	response := ""
	for index, stderr := range clusterData.Output.Stderrs {
		if clusterData.Output.Errors[index] == nil {
			continue
		}
		host := clusterData.Cluster.Segments[index].Hostname
		errorMessage := stderr
		if len(errorMessage) == 0 {
			errorMessage = clusterData.Output.Stdouts[index]
		}
		lines := strings.Split(errorMessage, "\n")
		errorMessage = lines[0]
		if len(lines) > 1 {
			errorMessage += "\n" + lines[1]
		}
		if len(lines) > 2 {
			errorMessage += "..."
		}
		response += fmt.Sprintf("%s ==> %s\n", host, errorMessage)
	}
	gplog.Info("ERROR: "+cmd.messages[err], numErrors, numHosts)
	gplog.Error("%s", response)
	return errors.New(response)
}

func doSetup(cmd *command) (*ClusterData, error) {
	connection := dbconn.NewDBConnFromEnvironment("postgres")
	err := connection.Connect(1)
	if err != nil {
		gplog.Error(fmt.Sprintf("ERROR: Could not connect to GPDB.\n%s\n"+
			"Please make sure that your Greenplum database is running and you are on the master node.", err.Error()))
		return nil, err
	}
	segConfigs := cluster.MustGetSegmentConfiguration(connection)
	clusterData := &ClusterData{Cluster: cluster.NewCluster(segConfigs), connection: connection}
	if cmd.name == sync || cmd.name == pxfInit || cmd.name == reset {
		err = clusterData.appendMasterStandby()
		if err != nil {
			return nil, err
		}
	}
	err = clusterData.CountHostsExcludingMaster()
	if err != nil {
		gplog.Error(err.Error())
		return nil, err
	}
	return clusterData, nil
}

func adaptContentIDToHostname(cluster *cluster.Cluster, f func(string) string) func(int) string {
	return func(contentId int) string {
		return f(cluster.GetHostForContent(contentId))
	}
}

func clusterRun(cmd *command, clusterData *ClusterData) error {
	defer clusterData.connection.Close()

	err := cmd.Warn(os.Stdin)
	if err != nil {
		gplog.Info(fmt.Sprintf("%s", err))
		return err
	}

	f, err := cmd.GetFunctionToExecute()
	if err != nil {
		gplog.Error(fmt.Sprintf("Error: %s", err))
		return err
	}

	cmdMsg := GenerateStatusReport(cmd, clusterData)
	clusterData.Output = clusterData.Cluster.GenerateAndExecuteCommand(
		cmdMsg,
		adaptContentIDToHostname(clusterData.Cluster, f),
		cmd.whereToRun,
	)
	return GenerateOutput(cmd, clusterData)
}

func (c *ClusterData) appendMasterStandby() error {
	query := ""
	if c.connection.Version.Before("6") {
		query = `
SELECT
        s.dbid,
        s.content as contentid,
        s.port,
        s.hostname,
        e.fselocation as datadir
FROM gp_segment_configuration s
JOIN pg_filespace_entry e ON s.dbid = e.fsedbid
JOIN pg_filespace f ON e.fsefsoid = f.oid
WHERE s.role = 'm' AND f.fsname = 'pg_system' AND s.content = '-1'
ORDER BY s.content;`
	} else {
		query = `
SELECT
        dbid,
        content as contentid,
        port,
        hostname,
        datadir
FROM gp_segment_configuration
WHERE role = 'm' AND content = '-1'
ORDER BY content;`
	}

	results := make([]cluster.SegConfig, 0)
	err := c.connection.Select(&results, query)
	if err != nil {
		return err
	}
	standbyMasterContentID := len(c.Cluster.Segments)
	if len(results) > 0 {
		c.Cluster.Segments[standbyMasterContentID] = results[0]
	}
	return nil
}
