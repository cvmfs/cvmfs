#!/bin/bash
# Created: Vipin Bhatnagar 28-Mar-2006
# replaces the @DATE@ strings in the doc files with mtime & @CVS_TAG@
# with the committed tag in CMS TagCOllector
# Now uses arrays for SubSystem/Package Tag version: 11-Jun-2006 VB 
ARG=$1
declare -a SubsPackNm=(`CmsTCPackageList.pl --rel $ARG |cut -d' ' -f1`)
declare -a SubsPackTg=(`CmsTCPackageList.pl --rel $ARG |cut -d' ' -f2`)
for i in src/*/*/doc/*.dox; do
  DATUM=`/usr/bin/stat --format="%y" "$i" | cut -d " " -f1`
  SUBPACK=`echo $i | cut -d "/" -f2,3`
  for (( j = 0 ; j < ${#SubsPackNm[@]} ; j++ ))
  do
    if [ $SUBPACK = ${SubsPackNm[$j]} ]
    then
      CVSTAG=${SubsPackTg[$j]}
      sed -e "s/@DATE@/$DATUM/g" \
          -e "s/@CVS_TAG@/$CVSTAG/g" "$i" > "${i/.dox}".doy
    fi
  done
done
