# !/bin/bash

set -x

TWO_DAYS_AGO_FLAG=`date +%Y-%m-%d -d "-2 days"`
DATE_FLAG=`date +%Y-%m-%d -d "-1 days"`
TODAY_FLAG=`date +%Y-%m-%d`
NOW_TIME_FLAG=`date +'%Y-%m-%d %H:%M'`
TWO_DAYS_AGO_NOW_TIME_FLAG=`date +'%Y-%m-%d %H:%M' -d "-2 days"`
LOG_CLEANUP_DAY=30
DATA_CLEANUP_DAY=8
CHECK_STAGE=10000
STATISTIC_DAYS=1
LOG_CLEANUP_DATE=`date +%Y-%m-%d -d "${DATE_FLAG} -${LOG_CLEANUP_DAY} days"`
DATA_CLEANUP_DATE=`date +%Y-%m-%d -d "${DATE_FLAG} -${DATA_CLEANUP_DAY} days"`

LOCAL_WORK_PATH="/data/zhaomin/stage_topdoc"
LOCAL_DATA_PATH=${LOCAL_WORK_PATH}/data
LOCAL_LOG_PATH=${LOCAL_WORK_PATH}/log
LOCAL_BIN_PATH=${LOCAL_WORK_PATH}/bin
HADOOP_ROOT_PATH="s3a://pm-hdfs2/user/zhaomin"

HADOOP_BIN=hadoop
HDFS_BIN=hdfs
HIVE_BIN="beeline -u jdbc:hive2://receng.emr.nb.com:10000/default -n hadoop"
DEFAULT_JOB_QUEUE="default"
JOB_NAME_PREFIX="zhaomin_stage_topdoc"

mkdir $LOCAL_BIN_PATH
mkdir $LOCAL_DATA_PATH
function copy_to_local()
{
    local hdfs_path=$1
    local local_path=$2

    rm -rf ${local_path} &>/dev/null
    ${HADOOP_BIN} dfs -getmerge ${hdfs_path}/* ${local_path}
    return $?
}

function copy_hive_to_local()
{
    local COPY_TO_LOCAL="${LOCAL_DATA_PATH}/${DATE_FLAG}"
    local output_path="${HADOOP_ROOT_PATH}/stage_topdoc/${DATE_FLAG}"
    copy_to_local ${output_path}/docid ${COPY_TO_LOCAL}/docid
    local copy_ret=$?
    if [ ${copy_ret} -ne 0 ]; then
        return ${copy_ret}
    fi
    return ${copy_ret}
}

function dump_docid_from_hive() {
    local hdfs_cjv_path=${HADOOP_ROOT_PATH}/stage_topdoc/${DATE_FLAG}/docid
    local hive_sql="SELECT   \
  a.doc_id, \
  checked,\
  combine_ctr \
FROM (   \
  SELECT\
    doc_id,\
    sum(cjv.checked) as checked,\
    (sum(cjv.clicked) + 2 * sum(cjv.thumbed_up) +  5 * sum(cjv.shared) + 5 * sum(cjv.comments_thumbed_up) + 10 * sum(cjv.comments_posted)) * 1.00000 / sum(checked) as combine_ctr \
  FROM warehouse.online_cjv_parquet_hourly AS cjv   \
  WHERE (cjv.pdate >= '${TWO_DAYS_AGO_FLAG}' and cjv.pdate <= '${TODAY_FLAG}')\
  AND (from_unixtime(unix_timestamp(ts), 'yyyy-MM-dd HH:mm') >= '${TWO_DAYS_AGO_NOW_TIME_FLAG}') \
  AND (from_unixtime(unix_timestamp(ts), 'yyyy-MM-dd HH:mm') <= '${NOW_TIME_FLAG}')    \
  AND cjv.joined = 1    \
  AND cjv.checked = 1   \
  AND cjv.channel_name = 'foryou'    \
  AND cjv.nr_condition not in ('deeplink', 'topheadline', 'dma_sports', 'opcard')   \
  AND cjv.nr_condition not in ('topheadline', 'local','localbriefing','localcurpos','localheadline','localpick','local_video','local_video_card', 'failover_local')   \
  GROUP BY cjv.doc_id     \
  HAVING (sum(cjv.clicked) + 2 * sum(cjv.thumbed_up) +  5 * sum(cjv.shared) + 5 * sum(cjv.comments_thumbed_up) + 10 * sum(cjv.comments_posted)) * 1.00000 /  sum(checked) >= 0.18\
  AND sum(cjv.clicked) / sum(cjv.checked) >= 0.07\
  AND sum(cjv.clicked) / sum(cjv.checked) <= 0.16\
  AND sum(cjv.checked) > 1000) a \
  JOIN (   \
    SELECT     \
      DISTINCT(doc_id)   \
    FROM dim.document_parquet dim  \
    WHERE (dim.pdate >= '${TWO_DAYS_AGO_FLAG}' and dim.pdate <= '${TODAY_FLAG}')\
    AND (from_unixtime(unix_timestamp(dim.insert_time), 'yyyy-MM-dd HH:mm') >= '${TWO_DAYS_AGO_NOW_TIME_FLAG}') \
    AND (from_unixtime(unix_timestamp(dim.insert_time), 'yyyy-MM-dd HH:mm') <= '${NOW_TIME_FLAG}')) b \
    ON a.doc_id = b.doc_id ORDER BY checked DESC \
    LIMIT 200"
    local sql_file=${LOCAL_BIN_PATH}/hive.sql.docid
    local hive_cmd="insert overwrite directory '${hdfs_cjv_path}' row format delimited fields terminated by ',' ${hive_sql};"
    echo "${hive_cmd}" >${sql_file}
    ${HDFS_BIN} dfs -rmr -skipTrash ${hdfs_cjv_path} &>/dev/null
    ${HIVE_BIN} --hiveconf mapreduce.job.name=${JOB_NAME_PREFIX}_query_cjv \
        --hiveconf mapreduce.job.queuename=${DEFAULT_JOB_QUEUE} \
        --hiveconf yarn.app.mapreduce.am.resource.mb=8192 \
        --hiveconf tez.am.resource.memory.mb=8192 \
        --hiveconf mapreduce.map.memory.mb=2048 \
        --hiveconf mapreduce.reduce.memory.mb=2048 \
        -f ${sql_file}
    return $?
}

function write_redis() {
    python ${LOCAL_BIN_PATH}/write_redis.py \
        --input ${LOCAL_DATA_PATH}/${DATE_FLAG}/docid \
        --prefix "stage_topdoc" \
        --stage $CHECK_STAGE \
        --days $STATISTIC_DAYS \
        --ttl 7200
    return $?
}

function process() {
    local ret=0
    local timestamp=`date +"%Y%m%d%H%M%S"`

    dump_docid_from_hive
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    copy_hive_to_local
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    write_redis
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi
}

function cleanup() {
    find ${LOCAL_LOG_PATH}/ -type f -mtime +${LOG_CLEANUP_DAY} -exec rm -f {} \; &>/dev/null
    find ${LOCAL_DATA_PATH}/ -maxdepth 1 -type d -mtime +${DATA_CLEANUP_DAY} -exec rm -rf {} \; &>/dev/null

    ${HDFS_BIN} dfs -rmr -skipTrash ${HADOOP_ROOT_PATH}/nearline/${LOG_CLEANUP_DATE} &>/dev/null
}

process
ret=$?
if [ ${ret} -ne 0 ]; then
    echo "process failed. ret[${ret}]" 1>&2
    exit ${ret}
fi

cleanup
exit ${ret}
