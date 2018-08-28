#!/bin/bash -l

set -exo pipefail

function install_hadoop_client {

    local segment=${1}
    SSH_OPTS="-i cluster_env_files/private_key.pem"
    scp -r ${SSH_OPTS} hdp.repo centos@${segment}:~
    ssh ${SSH_OPTS} centos@${segment} "set -x &&
        sudo yum install -y -d 1 java-1.8.0-openjdk-devel &&
	    echo 'export JAVA_HOME=/usr/lib/jvm/jre' | sudo tee -a ~gpadmin/.bash_profile &&
	    echo 'export JAVA_HOME=/usr/lib/jvm/jre' | sudo tee -a ~centos/.bash_profile &&
	    sudo mv /home/centos/hdp.repo /etc/yum.repos.d &&
	    sudo yum install -y -d 1 hadoop-client hive hbase"
}

function _main() {

	cp -R cluster_env_files/.ssh /root/.ssh
    gpdb_segments=$( < cluster_env_files/etc_hostfile grep -e "[sdw|mdw]" | awk '{print $1}')
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
    wait
}

_main "$@"
