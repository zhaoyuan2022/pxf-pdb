#!/usr/bin/env bash

dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" && pwd )
[[ -e ${dir}/common.sh ]] || exit 1
source "${dir}/common.sh"

get_status() {
	for i in {s,}mdw sdw{1,2}; do
		echo $i:
		ssh $i "
			[[ -d ${PXF_CONF}/servers/foo ]] && ls ${PXF_CONF}/servers/foo
			[[ -e ${PXF_CONF}/conf/foo.jar ]] && ls ${PXF_CONF}/conf/foo.jar
		"
	done
}

expected_cluster_output="Syncing PXF configuration files from master host to standby master host and 2 segment hosts...
PXF configs synced successfully on 3 out of 3 hosts"

mkdir -p ${PXF_CONF}/servers/foo
touch ${PXF_CONF}/servers/foo/{1..3} ${PXF_CONF}/conf/foo.jar
compare "${expected_cluster_output}" "$(pxf cluster sync)" "pxf cluster sync should succeed"

expected="smdw:
1
2
3
${PXF_CONF}/conf/foo.jar
mdw:
1
2
3
${PXF_CONF}/conf/foo.jar
sdw1:
1
2
3
${PXF_CONF}/conf/foo.jar
sdw2:
1
2
3
${PXF_CONF}/conf/foo.jar"
compare "${expected}" "$(get_status)" "all files should be present on all servers"

rm ${PXF_CONF}/conf/foo.jar
compare "${expected_cluster_output}" "$(pxf cluster sync)" "pxf cluster sync should succeed"
expected="smdw:
1
2
3
${PXF_CONF}/conf/foo.jar
mdw:
1
2
3
sdw1:
1
2
3
${PXF_CONF}/conf/foo.jar
sdw2:
1
2
3
${PXF_CONF}/conf/foo.jar"
compare "${expected}" "$(get_status)" "${PXF_CONF}/conf/foo.jar should be missing from mdw"

pxf sync --delete sdw1
pxf sync sdw2
expected="smdw:
1
2
3
${PXF_CONF}/conf/foo.jar
mdw:
1
2
3
sdw1:
1
2
3
sdw2:
1
2
3
${PXF_CONF}/conf/foo.jar"
compare "${expected}" "$(get_status)" "sdw1 should now also be missing ${PXF_CONF}/conf/foo.jar"

rm ${PXF_CONF}/servers/foo/1
compare "${expected_cluster_output}" "$(pxf cluster sync)" "pxf cluster sync should succeed"
expected="smdw:
1
2
3
${PXF_CONF}/conf/foo.jar
mdw:
2
3
sdw1:
1
2
3
sdw2:
1
2
3
${PXF_CONF}/conf/foo.jar"
compare "${expected}" "$(get_status)" "only mdw should be missing ${PXF_CONF}/servers/foo/1"

compare "${expected_cluster_output}" "$(pxf cluster sync --delete)" "pxf cluster sync should succeed"
expected="smdw:
2
3
mdw:
2
3
sdw1:
2
3
sdw2:
2
3"
compare "${expected}" "$(get_status)" "all nodes should now have just ${PXF_CONF}/servers/foo/{2,3}"

rm -rf ${PXF_CONF}/servers/foo
compare "${expected_cluster_output}" "$(pxf cluster sync --delete)" "pxf cluster sync should succeed"
expected="smdw:
mdw:
sdw1:
sdw2:"
compare "${expected}" "$(get_status)" "all nodes should be cleaned out completely"

# put standby master on sdw1
source "${GPHOME}/greenplum_path.sh"
gpinitstandby -ar >/dev/null
ssh sdw1 'mkdir /data/gpdata/master'
gpinitstandby -as sdw1 >/dev/null

# files should not sync to smdw anymore
expected_cluster_output="Syncing PXF configuration files from master host to 2 segment hosts...
PXF configs synced successfully on 2 out of 2 hosts"
touch ${PXF_CONF}/conf/foo.jar
compare "${expected_cluster_output}" "$(pxf cluster sync)" "pxf cluster sync should succeed"
expected="smdw:
mdw:
${PXF_CONF}/conf/foo.jar
sdw1:
${PXF_CONF}/conf/foo.jar
sdw2:
${PXF_CONF}/conf/foo.jar"
compare "${expected}" "$(get_status)" "all nodes but smdw should have ${PXF_CONF}/conf/foo.jar"

# put standby master back on smdw
gpinitstandby -ar >/dev/null
gpinitstandby -as smdw >/dev/null

exit_with_err "${BASH_SOURCE[0]}"
