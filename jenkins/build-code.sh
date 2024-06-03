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

exec_cmd "git config --global --add safe.directory $PWD"

exit 0
