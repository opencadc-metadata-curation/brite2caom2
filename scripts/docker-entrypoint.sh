#!/bin/bash

if [[ ! -e ${PWD}/config.yml ]]
then
  cp /usr/local/.config/config.yml ${PWD}
fi

if [[ ! -e ${PWD}/state.yml ]]; then
  if [[ "${@}" == "brite_run_state" ]]; then
    yesterday=$(date -d yesterday "+%d-%b-%Y %H:%M")
    echo "bookmarks:
    brite_timestamp:
      last_record: $yesterday
" > ${PWD}/state.yml
  else
    cp /usr/local/.config/state.yml ${PWD}
  fi
fi

exec "${@}"
