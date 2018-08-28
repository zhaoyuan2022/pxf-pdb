#!/bin/bash -l

set -exuo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

setup_ssh_for_user() {
  local user=${1}
  local home_dir
  home_dir=$(eval echo "~${user}")

  mkdir -p ${home_dir}/.ssh
  touch ${home_dir}/.ssh/authorized_keys ${home_dir}/.ssh/known_hosts ${home_dir}/.ssh/config
  #ssh-keygen -t rsa -N "" -f "${home_dir}/.ssh/id_rsa"
  cat ${home_dir}/.ssh/id_rsa.pub >> ${home_dir}/.ssh/authorized_keys
  chmod 0600 ${home_dir}/.ssh/authorized_keys
  chown -R ${user} ${home_dir}/.ssh
}

ssh_keyscan_for_user() {
  local user=${1}
  local home_dir
  home_dir=$(eval echo "~${user}")

  {
    ssh-keyscan localhost
    ssh-keyscan 0.0.0.0
    ssh-keyscan github.com
  } >> ${home_dir}/.ssh/known_hosts
}

setup_gpadmin_user() {
  echo -e "password\npassword" | passwd gpadmin
  groupadd supergroup
  usermod -a -G supergroup gpadmin
  setup_ssh_for_user gpadmin
}

setup_sshd() {
  test -e /etc/ssh/ssh_host_key || ssh-keygen -f /etc/ssh/ssh_host_key -N '' -t rsa1
  test -e /etc/ssh/ssh_host_rsa_key || ssh-keygen -f /etc/ssh/ssh_host_rsa_key -N '' -t rsa
  test -e /etc/ssh/ssh_host_dsa_key || ssh-keygen -f /etc/ssh/ssh_host_dsa_key -N '' -t dsa

  # See https://gist.github.com/gasi/5691565
  sed -ri 's/UsePAM yes/UsePAM no/g' /etc/ssh/sshd_config
  # Disable password authentication so builds never hang given bad keys
  sed -ri 's/PasswordAuthentication yes/PasswordAuthentication no/g' /etc/ssh/sshd_config

  setup_ssh_for_user root

  # Test that sshd can start
  /etc/init.d/sshd start

  ssh_keyscan_for_user root
  ssh_keyscan_for_user gpadmin
}

symlink_build_dir() {
  local target_base_dir="${1}"
  local cwd="${2}"

  # FIXME: total hack to deal with compiled code having the abs path from the
  # previous container hardcoded throughout files.
  rm -f ${target_base_dir}
  ln -sfv ${cwd} ${target_base_dir}
}

unpack_tarball() {
  echo "Unpacking tarball: $(ls ./*.tar.gz)"
  tar xfp ./*.tar.gz --strip-components=1
}

install_deps() {
  if [ "${distro_name}" == "redhat" ]; then
	  echo "Start to install thirdparty dependencies for HAWQ:"
	  pushd ..
	  unpack_tarball

	  cat > /etc/yum.repos.d/hawq_deps.repo <<-EOF
			[Thirdparty-deps]
			name=HAWQ Thirdparty dependencies
			baseurl=file://${PWD}/hawq
			gpgcheck=0
			enabled=1
			priority=1
		EOF

	  yum install -y -d 1 thrift boost protobuf apache-maven rpm-build perl-JSON lcov
	  ldconfig
	  gcc --version
	  tar xf curl/curl*.tgz -C /usr/local
	  popd
  fi
}

configure_environment() {
  local bashfile=/home/gpadmin/hdfsenv
  echo "GPHD_ROOT=/home/build/hdfsrepo/" >> ${bashfile}
  echo "HADOOP_ROOT=\$GPHD_ROOT/hadoop" >> ${bashfile}
  echo "HBASE_ROOT=\$GPHD_ROOT/hbase" >>  ${bashfile}
  echo "HIVE_ROOT=\$GPHD_ROOT/hive" >> ${bashfile}
  echo "ZOOKEEPER_ROOT=\$GPHD_ROOT/zookeeper" >> ${bashfile}
  echo "PATH=$PATH:\$GPHD_ROOT/bin:\$HADOOP_ROOT/bin:\$HBASE_ROOT/bin:\$HIVE_ROOT/bin:\$ZOOKEEPER_ROOT/bin" >> ${bashfile}
  echo "GPHOME=/usr/local/hawq" >> ${bashfile}
  source ${bashfile}
}

install_hadoop() {
  local hdfsrepo=${1}
  pushd ${hdfsrepo}

  echo "======================================================================"
  echo "                            Install HDFS"
  echo "======================================================================"

  unpack_tarball
  cd bin
  ./init-gphd.sh
  ./start-hdfs.sh
  ./start-pxf.sh
  hdfs dfs -ls /
  hdfs dfs -chown gpadmin:supergroup /

  echo "----------------------------------------------------------------------"
  echo "                         HADOOP VERSION INFO"
  echo "----------------------------------------------------------------------"
  ./hadoop version

  echo "======================================================================"
  echo "                         Install HDFS Finish"
  echo "======================================================================"

  popd
}

start_hive_hdfs() {
  local hdfsrepo=${1}
  pushd ${hdfsrepo}/bin

  echo "======================================================================"
  echo "                            Start Hive/HBase"
  echo "======================================================================"

  ./start-hive.sh
  ./hbase version
  ./hive --version 2> /dev/null

  popd
}

init_hawq_cluster() {
  local rpm_src_dir=${1}
  setup_gpadmin_user
  setup_sshd

  pushd ${rpm_src_dir}
    rpm -iv ./*.rpm
    pushd /usr/local
    ln -s  `ls -d hawq*` hawq
    popd
  popd

  pushd /usr/local/hawq
    /usr/bin/sudo -u gpadmin bash -c 'source greenplum_path.sh && hawq init cluster --prompt' || \
      (cat /home/gpadmin/hawqAdminLogs/*.log && exit 1)
  popd
}

set_yarn() {
  /home/build/hdfsrepo/bin/start-yarn.sh
  /usr/bin/sudo -u gpadmin bash -c 'source /usr/local/hawq/greenplum_path.sh && hawq config -c hawq_global_rm_type -v "yarn";hawq restart cluster -a'
} 

start_hadoop() {
  local hdfsrepo=${1}
  pushd "${hdfsrepo}"

  echo "======================================================================"
  echo "                            Install HDFS"
  echo "======================================================================"

  unpack_tarball
  chown -R gpadmin:gpadmin .
  cd bin
  /usr/bin/sudo -u gpadmin bash -c 'source /home/gpadmin/gphd_vars && ./init-gphd.sh && ./start-gphd.sh'
  /usr/bin/sudo -u gpadmin bash -c 'source /home/gpadmin/gphd_vars && hdfs dfs -ls /'
  /usr/bin/sudo -u gpadmin bash -c 'source /home/gpadmin/gphd_vars && hdfs dfs -chown gpadmin:supergroup /'

  echo "----------------------------------------------------------------------"
  echo "                         HADOOP VERSION INFO"
  echo "----------------------------------------------------------------------"
  ./hadoop version
  echo "----------------------------------------------------------------------"
  ./hbase version
  echo "----------------------------------------------------------------------"
  ./hive --version 2> /dev/null
  echo "----------------------------------------------------------------------"

  echo "======================================================================"
  echo "                         Install HDFS Finish"
  echo "======================================================================"
  
  popd
}

configure_variables() {
  local bashfile=/home/gpadmin/gphd_vars
  echo "export JAVA_HOME=/etc/alternatives/java_sdk" >> ${bashfile}
  echo "export GPHD_ROOT=/home/build/hdfsrepo/" >> ${bashfile}
  echo "export HADOOP_ROOT=\$GPHD_ROOT/hadoop" >> ${bashfile}
  echo "export HBASE_ROOT=\$GPHD_ROOT/hbase" >>  ${bashfile}
  echo "export HIVE_ROOT=\$GPHD_ROOT/hive" >> ${bashfile}
  echo "export ZOOKEEPER_ROOT=\$GPHD_ROOT/zookeeper"  >> ${bashfile}
  echo "export PATH=$PATH:\$GPHD_ROOT/bin:\$HADOOP_ROOT/bin:\$HBASE_ROOT/bin:\$HIVE_ROOT/bin:\$ZOOKEEPER_ROOT/bin"  >> ${bashfile}
  echo "export GPHOME=/usr/local/hawq"  >> ${bashfile}
  echo "export PGPORT=5432" >> ${bashfile}
  echo "export CATALINA_OPTS=\"-XX:MaxPermSize=1024m\"" >> ${bashfile}
  source ${bashfile}
}

init_hawq() {
  local rpm_src_dir=${1}

  pushd ${rpm_src_dir}
    rpm -iv ./*.rpm
    pushd /usr/local
    ln -s  `ls -d hawq*` hawq
    popd
  popd

  pushd /usr/local/hawq
    /usr/bin/sudo -u gpadmin bash -c 'source greenplum_path.sh && hawq init cluster --prompt' || \
      (cat /home/gpadmin/hawqAdminLogs/*.log && exit 1)
  popd
}
  
featuretest_hawq() {
  base_dir=$(pwd)

  chown -R gpadmin:gpadmin ${TEST_RUNPATH}
  # chown -R gpadmin:gpadmin "${HDFS_REPO}"

  configure_variables

  setup_gpadmin_user
  setup_sshd
  start_hadoop ${HDFS_REPO}
  init_hawq ${HAWQ_RPM}
  
  pushd ${TEST_RUNPATH}
  echo "${MAVEN_PW}"
  sed -i.bak 's/<password>.*<\/password>/<password>'"${MAVEN_PW}"'<\/password>/' settings.xml
  cd scripts
  /usr/bin/sudo -u gpadmin bash -c 'source /home/gpadmin/gphd_vars && ./run-automation-local-machine.sh '"${TEST_TYPE}"
  popd
  
}

_main() {
  install_deps
  configure_environment
  featuretest_hawq
}

_main "$@"
