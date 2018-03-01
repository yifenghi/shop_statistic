#!/bin/sh 

set -ux
#work_dir=$(readlink -f $(dirname $0))
work_dir=$(readlink -f $(dirname $0))/..
source $work_dir/conf/hadoop.conf

date="2014-08-06 10:25:00"
#date="2014-08-04 17:30:00"
log_date=`date -d "$date" +%Y%m%d`
log_path="$work_dir/log/bi_store.$log_date.log"
for ((i=0; i<1; i++))
do
	date=`date -d "$date 5 minutes" "+%Y-%m-%d %H:%M:%S"`
	#sh $work_dir/bin/shopMessage.sh "$date" "min" 2>&1 | tee -a "$log_path"
    	#sh $work_dir/bin/mergeMessage.sh "$date" "hour" 2>&1 | tee -a "$log_path"
    	#sh $work_dir/bin/mergeMessage.sh "$date" "day" 2>&1 | tee -a "$log_path"
    	#sh $work_dir/bin/shopFlow.sh "$date" 2>&1 | tee -a "$log_path"
    	sh $work_dir/bin/customerDwell.sh "$date" 2>&1 | tee -a "$log_path"
done	
