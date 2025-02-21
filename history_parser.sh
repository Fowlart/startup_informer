 if [[ $1 == "" ]]
then
  echo "There are no search terms provided. Please add a string argument, to search among Telegram conversations. Starting test script!"
  .venv/bin/python3.12 parsers/concrate_channel_parser_test.py || 1
  cd pyspark_interaction || exit 1
   ../.venv/bin/python3.12 create_delta_tables.py || exit 1
else
   echo "Provided search term: $1"
   .venv/bin/python3.12 parsers/full_history_parser.py "$1" || exit 1
   cd pyspark_interaction || exit 1
   ../.venv/bin/python3.12 create_delta_tables.py || exit 1
fi