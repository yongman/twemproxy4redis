#! /bin/sh
#
# entrypoint.sh
# Copyright (C) 2017 yanming02 <yanming02@baidu.com>
#
# Distributed under terms of the MIT license.
#


#!/bin/bash
listen_port=$1
seed_ip=$2
seed_port=$3

sed -i "s/LISTEN_PORT/$listen_port/g" /opt/proxy/conf/nutcracker.yml
sed -i "s/SEEDIP/$seed_ip/g" /opt/proxy/conf/nutcracker.yml
sed -i "s/SEEDPORT/$seed_port/g" /opt/proxy/conf/nutcracker.yml

/opt/proxy/bin/nutcracker -v 6 -s 5380  -m 16384 -c /opt/proxy/conf/nutcracker.yml -l /opt/proxy/bin/lua -o /opt/proxy/log/nutcracker.log
