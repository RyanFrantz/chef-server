#!/bin/bash -xe
if [ "$(cat /opt/opscode/.branch)" == "master" ]
then
  sudo rm -rf /opt/opscode/*
fi
if [ -d /opt/opscode-master ]
then
  sudo rm -rf /opt/opscode-master/*
fi
rm -rf /var/cache/omnibus/master/*
