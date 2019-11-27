#!/bin/sh
/usr/bin/curl http://0.0.0.0:13800/dict
/usr/bin/crontab periodicGossipInvoker
service cron start
