#!/bin/bash

echo Migrating index in $1
pushd $1
for name in *
do
  keep=`echo $name | cut -d ':' -f 1`
  if [ ${#keep} == 1 ] 
  then
    keep="0$keep"
  fi
  echo $name to $keep
  mv $name $keep
done

popd
