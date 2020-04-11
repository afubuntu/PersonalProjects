#!/bin/bash
#create_ctennisdb.sh

sql_cmd=""

if sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw ${1}; then
   echo "The database <$1> already exists."
else
   echo "The database <$1> does not exist. It will be created."
   sql_cmd="create database $1"
   sudo -u postgres psql -c "${sql_cmd}"
fi
exit