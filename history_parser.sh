#!/bin/bash
SPARK_JOB_FOLDER="./pyspark_jobs"
PATH_TO_PYTHON_INTERPRETER=.venv/bin/python3.12
RED='\033[0;31m'
NC='\033[0m' # No Color

 function run_write_to_delta {
   if [[ $1 == 0 ]]
   then
      cd $SPARK_JOB_FOLDER || exit 1
      ../$PATH_TO_PYTHON_INTERPRETER create_delta_tables.py || exit 1
      ../$PATH_TO_PYTHON_INTERPRETER read_users.py || exit 1
   else
      echo "${RED} script execution failed with the code $1 ${NC}"
  fi
 }

if [[ $1 == "" ]]
then
  echo "There are no search terms provided. Please add a string argument, to search among Telegram conversations. Starting test script!"
  $PATH_TO_PYTHON_INTERPRETER ingestion/parse_concrete_channel_history.py || exit 1
  run_write_to_delta $?
else
   echo "Provided search term: $1"
   $PATH_TO_PYTHON_INTERPRETER ingestion/parse_history.py "$1" || exit 1
   run_write_to_delta $?
fi
