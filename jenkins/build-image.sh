#!/bin/bash

function fail_with_error_msg()
{
    echo Command failed: "$1"
    exit 1
}

function exec_cmd()
{
    echo Running command: "$1"
    eval "$1" || fail_with_error_msg "$1"
}

DOCKERIMAGENAME=$1
DOCKERFILE=$2
GIT_DESCRIBE=`git describe --tags --dirty --always`
exec_cmd "docker build --no-cache -t ${DOCKERIMAGENAME}:${GIT_DESCRIBE} -f ${DOCKERFILE} ."

exit 0
