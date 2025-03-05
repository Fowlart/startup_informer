#!/bin/bash
SPARK_JOB_FOLDER="./pyspark_jobs"
PATH_TO_PYTHON_INTERPRETER=.venv/bin/python3.12

 function run_write_to_delta {
   if [[ $1 == 0 ]]
   then
      cd $SPARK_JOB_FOLDER || exit 1
      ../$PATH_TO_PYTHON_INTERPRETER create_delta_tables.py || exit 1
   else
      echo "Script execution failed with the code $1"
  fi
 }

 function run_key_words_extraction {
   if [[ $1 == 0 ]]
   then
      cd $SPARK_JOB_FOLDER || exit 1
      ../$PATH_TO_PYTHON_INTERPRETER create_delta_tables.py || exit 1
      ../$PATH_TO_PYTHON_INTERPRETER retrieve_keywords_tf_idf.py || exit 1
   else
      echo "Script execution failed with the code $1"
  fi
 }

if [[ $1 == "" ]]
then
  echo "There are no search terms provided. Please add a string argument, to search among Telegram conversations!"
  export SPECIFIC_DIALOG=Olena
  echo "Telegram channel with the name $SPECIFIC_DIALOG used as a primary messages source"
  $PATH_TO_PYTHON_INTERPRETER ingestion/parse_concrete_channel_history.py || exit 1
  run_key_words_extraction $?
else
   echo "Provided search term: $1"
   $PATH_TO_PYTHON_INTERPRETER ingestion/parse_history.py "$1" || exit 1
   run_write_to_delta $?
fi
