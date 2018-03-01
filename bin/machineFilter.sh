#!/bin/bash

set -ux
work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

if [ $# -eq 1 ]
then
  date=$1
else
  echo "Usgae: $0 date"  
  exit 1
fi

time_tag="night"
date_today=`date -d "$date" "+%Y%m%d"`
input="$statistic/$time_tag/$date_today"
for((i=1;i<$machine_input_num;i++))
do
  date_num=`date -d "$date_today $i days ago" "+%Y%m%d"`
  input="$input,$statistic/$time_tag/$date_num"
done
output="$statistic/machine/$date_today"

class="com.doodod.market.analyse.MachineLauncher"
job_name=$(basename ${0%.sh})
job_conf="$work_dir/conf/machineMessage.xml"
hrmr $output

HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-libjars $jama \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D mapreduce.input.fileinputformat.inputdir="$input" \
-D store.businesstime.now="$date" \
-D rssi.machine.var="$rssi_machine_var"

exit 0;
