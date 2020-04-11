#!/bin/bash
#drop_ctennisdb.sh

sql_cmd=""

if sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw ${1}; then
   echo "The database <$1> exists. It will be dropped."
   sql_cmd="drop database $1"
   sudo -u postgres psql -c "${sql_cmd}"   
else
   echo "The database <$1> does not exist."
fi
exit