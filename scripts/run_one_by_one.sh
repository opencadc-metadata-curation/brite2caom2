#!/bin/bash

if [[ ${#} -lt 1 ]]; then
    echo "usage: ${0} <fully-qualified directory name>"
    exit 1
fi

COLLECTION="brite"
IMAGE="opencadc/${COLLECTION}/2caom2"
dir_name=$1

for ii in $( ls ${dir_name} ); do
	fqn=$"${dir_name}/${ii}"
	echo "$fqn"
	docker run --rm --name ${COLLECTION}_todo  --user $(id -u):$(id -g) -e HOME=/usr/src/app -v ${PWD}:/usr/src/app/ -v ${fqn}:/data ${IMAGE} ${COLLECTION}_run
	break
done

echo "ok"
exit 0

