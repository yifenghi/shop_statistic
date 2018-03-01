#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

if [ $# -eq 1 ]
then
  date=$1
else
  echo "Usgae: $0 date"  
  exit 1
fi

###step1 
time_tag="day"
date_today=`date -d "$date" "+%Y%m%d"`
brand_path="$work_dir/data/machine_brand"
jama="/var/lib/hadoop-hdfs/code/deploy/local_lib/Jama-1.0.3.jar"

input="$statistic/$time_tag/$date_today/00/00"
for((i=1;i<$machine_input_num;i++))
do
  date_num=`date -d "$date_today $i days ago" "+%Y%m%d"`
  input="$input,$statistic/$time_tag/$date_num/00/00"
done
output="$statistic/employee/$date_today"
class="com.doodod.market.analyse.EmloyeeLanuncher"
hrmr $output

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-libjars $jama \
-files $brand_path \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D mapreduce.input.fileinputformat.inputdir="$input" \
-D brand.map.path="`basename $brand_path`" \
-D store.businesstime.now="$date" \
-D rssi.machine.var="$rssi_machine_var"

exit 0;
