#! /usr/bin/env bash

# Helper script for run_tests.sh to send all the requests listed in a file to a particular port

testfile=$1
port=$2 
outfile=$3
server_pid=$4
NC_FLAGS="-N"
retcode=0

if [ "$(uname -s)" == "Darwin" ]; then 
  NC_FLAGS=""
fi 

# reset outfile
rm -f ${outfile}
touch ${outfile}

while : ; do 
  nc -z localhost $port >/dev/null 2>&1
  if [ $? -eq 0 ]; then break ; fi 
done

cat ${testfile} | nc ${NC_FLAGS} localhost $port >> ${outfile} 2>&1

retcode=$?

exit $retcode