#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class="com.doodod.market.statistic.ShopLauncher"
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml


#date="2014-07-29 06:00:00"
if [ $# -eq 1 ]
then
  date=$1
else
  echo "Usgae: $0 date"  
  exit 1
fi

###step1 
time_tag="night"
date_6hours_ago=`date -d "$date 6 hours ago" "+%Y-%m-%d %H:%M:%S"`
date_today=`date -d "$date" "+%Y%m%d"`
model_path="$work_dir/data/model.min"
brand_path="$work_dir/data/mac_brand"

output="$statistic/$time_tag/$date_today"
hrmr $output 

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-files $model_path,$brand_path \
-libjars $jama \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D store.businesstime.start="$date_6hours_ago" \
-D store.businesstime.now="$date" \
-D train.model.path="`basename $model_path`" \
-D brand.map.path="`basename $brand_path`" \
-D train.passenger.featurelist="$minute_feature" \
-D rssi.indoor.scope="$rssi_indoor"

#input=$output
exit 0;
