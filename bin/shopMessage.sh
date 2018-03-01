#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.market.statistic.ShopLauncher
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

#date=`date "+%Y-%m-%d %H:%M:%S"`
if [ $# -eq 2 ]
then
  date=$1
  time_tag=$2
else
  echo "Usgae: $0 date time_tag"  
  exit 1
fi

date_5mins_ago=`date -d "$date 5 minutes ago" "+%Y-%m-%d %H:%M:%S"`
date_today=`date -d "$date_5mins_ago" "+%Y%m%d"`
dir_name=`date -d "$date" "+%Y%m%d/%H/%M"`
model_path="$work_dir/data/model.min"
brand_path="$work_dir/data/mac_brand"
feature="$minute_feature"

output="$statistic/$time_tag/$dir_name"
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
-D store.businesstime.start="$date_5mins_ago" \
-D store.businesstime.now="$date" \
-D train.model.path="`basename $model_path`" \
-D brand.map.path="`basename $brand_path`" \
-D train.passenger.featurelist="$feature" \
-D rssi.indoor.scope="$rssi_indoor"

exit 0;
