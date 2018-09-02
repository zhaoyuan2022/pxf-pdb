#!/bin/bash -l

set -exo pipefail

GPHOME="/usr/local/greenplum-db-devel"
SSH_OPTS="-i cluster_env_files/private_key.pem"

function install_hadoop_client {

    local segment=${1}
    scp -r ${SSH_OPTS} hdp.repo centos@${segment}:~
    ssh ${SSH_OPTS} centos@${segment} "
        sudo yum install -y -d 1 java-1.8.0-openjdk-devel &&
	    echo 'export JAVA_HOME=/usr/lib/jvm/jre' | sudo tee -a ~gpadmin/.bash_profile &&
	    echo 'export JAVA_HOME=/usr/lib/jvm/jre' | sudo tee -a ~centos/.bash_profile &&
	    sudo mv /home/centos/hdp.repo /etc/yum.repos.d &&
	    sudo yum install -y -d 1 hadoop-client hive hbase"
}

function setup_pxf {

    local segment=${1}
    local hadoop_ip=${2}
    scp -r ${SSH_OPTS} pxf_tarball centos@"${segment}":
    scp ${SSH_OPTS} pxf_src/concourse/setup_pxf_on_segment.sh centos@${segment}:
    scp ${SSH_OPTS} /singlecluster/hadoop/etc/hadoop/{core,hdfs,mapred}-site.xml centos@${segment}:
    scp ${SSH_OPTS} /singlecluster/hive/conf/hive-site.xml centos@${segment}:
    scp ${SSH_OPTS} /singlecluster/hbase/conf/hbase-site.xml centos@${segment}:
    scp ${SSH_OPTS} cluster_env_files/etc_hostfile centos@${segment}:
    ssh ${SSH_OPTS} centos@${segment} "sudo bash -c \"\
        cd /home/centos && IMPERSONATION=${IMPERSONATION} PXF_JVM_OPTS='${PXF_JVM_OPTS}' ./setup_pxf_on_segment.sh ${hadoop_ip}
        \""
}

function install_hadoop_single_cluster() {

    local hadoop_ip=${1}
    scp ${SSH_OPTS} cluster_env_files/etc_hostfile centos@edw0:
    scp ${SSH_OPTS} -rq /singlecluster centos@edw0:
    scp ${SSH_OPTS} pxf_src/concourse/setup_hadoop_single_cluster.sh centos@edw0:

    ssh ${SSH_OPTS} centos@edw0 "sudo bash -c \"\
        cd /home/centos && IMPERSONATION=${IMPERSONATION} ./setup_hadoop_single_cluster.sh ${hadoop_ip}
    \""
}

function _main() {

	cp -R cluster_env_files/.ssh /root/.ssh
    gpdb_segments=$( < cluster_env_files/etc_hostfile grep -e "sdw\|mdw" | awk '{print $1}')
    hadoop_ip=$( < cluster_env_files/etc_hostfile grep "edw0" | awk '{print $1}')
    cat > hdp.repo <<-EOF
		#VERSION_NUMBER=2.6.5.0-292
		[HDP-2.6.5.0]
		name=HDP Version - HDP-2.6.5.0
		baseurl=http://public-repo-1.hortonworks.com/HDP/centos7/2.x/updates/2.6.5.0
		gpgcheck=1
		gpgkey=http://public-repo-1.hortonworks.com/HDP/centos7/2.x/updates/2.6.5.0/RPM-GPG-KEY/RPM-GPG-KEY-Jenkins
		enabled=1
		priority=1
	EOF
    for segment in ${gpdb_segments}; do
        install_hadoop_client ${segment} &
    done
    install_hadoop_single_cluster ${hadoop_ip} &
    wait
    for segment in ${gpdb_segments}; do
        setup_pxf ${segment} ${hadoop_ip} &
    done
    wait
}

_main "$@"
