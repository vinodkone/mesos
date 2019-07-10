#!/usr/bin/env bash

# This script should be used to publish a snapshot JAR to Apache Maven
# repo from the current HEAD.

set -e

# Use 'atexit' for cleanup.
. $(dirname ${0})/atexit.sh

# Use colors for errors.
. $(dirname ${0})/colors.sh

test ${#} -eq 0 || \
  { echo "Usage: `basename ${0}`"; exit 1; }


echo "${GREEN}Deploying SNAPSHOT JAR${NORMAL}"

WORK_DIR=`mktemp -d /tmp/mesos-tag-XXXX`
atexit "rm -rf ${WORK_DIR}"

# Get the absolute path of the local git clone.
MESOS_GIT_LOCAL=$(cd "$(dirname $0)"/..; pwd)

pushd ${WORK_DIR}

# Make a shallow clone from the local git repository.
git clone --shared ${MESOS_GIT_LOCAL} mesos

pushd mesos

VERSION=`grep AC_INIT configure.ac | grep mesos | cut -d'[' -f3 | cut -d']' -f1`
TAG="${VERSION}-SNAPSHOT"

echo "${GREEN}Updating version in configure.ac to ${TAG}.${NORMAL}"

if [[ "$OSTYPE" == "darwin"* ]]; then
  sed -i '' "s/\[mesos\], \[.*\]/[mesos], [${TAG}]/" configure.ac
else
  sed -i "s/\[mesos\], \[.*\]/[mesos], [${TAG}]/" configure.ac
fi

# Build mesos.
./bootstrap
mkdir build
pushd build
../configure --disable-optimize

# First build the protobuf compiler.
# TODO(vinod): This is short term fix for MESOS-959.
pushd 3rdparty
make -j3
popd

# Build and deploy the jar.
make -j3 maven-install
mvn deploy -Dgpg.skip -f src/java/mesos.pom

echo "${GREEN}Successfully deployed the jar to maven snapshot repository ...${NORMAL}"

popd # build

popd # mesos
popd # ${WORK_DIR}

echo "${GREEN}Success!${NORMAL}"

exit 0
