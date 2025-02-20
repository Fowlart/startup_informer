 if [[ $1 == "" ]]
then
  echo "There are no search terms provided. Please add a string argument, to search among Telegram conversations."
else
   echo "Provided search term: $1"
   .venv/bin/python3.12 full_history_parser.py "$1" || exit 1
   cd pyspark_interaction || exit 1
   ../.venv/bin/python3.12 create_delta_tables.py || exit 1
fi