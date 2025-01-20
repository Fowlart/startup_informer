 if [[ $1 == "" ]]
then
  echo "There are no search terms provided. Please add a string argument, to search among Telegram conversations."
else
   echo "Provided search term: $1"
   .venv/bin/python3.12 full_history_parser.py  > ./startup_info 2>&1 || exit 1
fi