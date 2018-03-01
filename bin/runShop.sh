#!/bin/sh 

set -eux
#work_dir=$(readlink -f $(dirname $0))
work_dir=$(readlink -f $(dirname $0))/..

date="2014-06-13 08:00:00"
for ((i=0; i<1; i++))
do
	date=`date -d "$date 5 minutes" "+%Y-%m-%d %H:%M:%S"`
	echo $date
	#sh $work_dir/bin/shopMessage.sh "$date" "min"
	#sh $work_dir/bin/mergeMessage.sh "$date" "hour" 
	#sh $work_dir/bin/mergeMessage.sh "$date" "day" 
	sh $work_dir/bin/shopFlow.sh "$date" 
done
