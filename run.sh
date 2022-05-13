# !/bin/bash

set -x

TWO_DAYS_AGO_FLAG=`date +%Y-%m-%d -d "-2 days"`
DATE_FLAG=`date +%Y-%m-%d -d "-1 days"`
TODAY_FLAG=`date +%Y-%m-%d`
LOG_CLEANUP_DAY=30
DATA_CLEANUP_DAY=8
LOG_CLEANUP_DATE=`date +%Y-%m-%d -d "${DATE_FLAG} -${LOG_CLEANUP_DAY} days"`
DATA_CLEANUP_DATE=`date +%Y-%m-%d -d "${DATE_FLAG} -${DATA_CLEANUP_DAY} days"`

LOCAL_WORK_PATH="/data/dengjiahao/nearline"
LOCAL_DATA_PATH=${LOCAL_WORK_PATH}/data
LOCAL_LOG_PATH=${LOCAL_WORK_PATH}/log
LOCAL_BIN_PATH=${LOCAL_WORK_PATH}/bin
HADOOP_ROOT_PATH="s3a://pm-hdfs2/user/dengjiahao"

HADOOP_BIN=hadoop
HDFS_BIN=hdfs
HIVE_BIN="beeline -u jdbc:hive2://receng.emr.nb.com:10000/default -n hadoop"
DEFAULT_JOB_QUEUE="default"
JOB_NAME_PREFIX="jiahao_nearline"

function copy_to_local()
{
    local hdfs_path=$1
    local local_path=$2

    rm -rf ${local_path} &>/dev/null
    # mkdir -p ${local_path}
    ${HADOOP_BIN} dfs -getmerge ${hdfs_path}/* ${local_path}
    return $?
}

function copy_hive_to_local()
{
    local COPY_TO_LOCAL="${LOCAL_DATA_PATH}/${DATE_FLAG}"
    local output_path="${HADOOP_ROOT_PATH}/nearline/${DATE_FLAG}"
    copy_to_local ${output_path}/docid ${COPY_TO_LOCAL}/docid
    local copy_ret=$?
    if [ ${copy_ret} -ne 0 ]; then
        return ${copy_ret}
    fi

    copy_to_local ${output_path}/uid ${COPY_TO_LOCAL}/uid
    local copy_ret=$?
    return ${copy_ret}
}

function dump_docid_from_hive() {
    local hdfs_cjv_path=${HADOOP_ROOT_PATH}/nearline/${DATE_FLAG}/docid
    local hive_sql="SELECT \
doc_id \
FROM warehouse.online_cjv_parquet_hourly AS cjv \
WHERE \
((cjv.pdate = '${TWO_DAYS_AGO_FLAG}' and cjv.phour >= '05') or cjv.pdate = '${DATE_FLAG}' or (cjv.pdate = '${TODAY_FLAG}' and cjv.phour < '05')) \
AND cjv.joined = 1 \
AND cjv.checked = 1 \
AND cjv.channel_name = 'foryou' \
AND cjv.nr_condition not in ('deeplink', 'topheadline', 'dma_sports', 'opcard') \
AND cjv.nr_condition not like '%local%' \
AND cjv.ctype = 'news' \
GROUP BY cjv.doc_id \
HAVING (sum(cjv.clicked) + 2 * sum(cjv.thumbed_up) + 5 * sum(cjv.shared)) / sum(cjv.checked) >= 0.035"
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

function dump_uid_from_hive() {
    local hdfs_cjv_path=${HADOOP_ROOT_PATH}/nearline/${DATE_FLAG}/uid
    local hive_sql="with user_stat as (\
        SELECT \
            user_id, \
            sum(cjv.checked) as checks, \
            sum(cjv.clicked) as clicks, \
            concat_ws('|', collect_set(cjv.os)) \
        FROM warehouse.online_cjv_parquet_hourly AS cjv \
        WHERE \
            ((cjv.pdate = '${TWO_DAYS_AGO_FLAG}' and cjv.phour >= '05') or (cjv.pdate = '${TODAY_FLAG}' and cjv.phour < '05')) \
        AND cjv.joined = 1 \
        AND cjv.checked = 1 \
        AND cjv.channel_name = 'foryou' \
        GROUP BY user_id \
        ) \
        select user_stat.user_id from user_stat \
        where user_stat.checks >= 350"
    local sql_file=${LOCAL_BIN_PATH}/hive.sql.uid
    local hive_cmd="insert overwrite directory '${hdfs_cjv_path}' row format delimited fields terminated by ',' ${hive_sql};"
    echo ${hive_cmd} >${sql_file}
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

function select_target_user() {
    # cat ${LOCAL_DATA_PATH}/${DATE_FLAG}/uid | awk -F ',' '$2 >= 8 && $2 <= 200' | sort -k2nr -t ',' >${LOCAL_DATA_PATH}/${DATE_FLAG}/uid.target
    cat ${LOCAL_DATA_PATH}/${DATE_FLAG}/uid | sort -k2nr -t ',' >${LOCAL_DATA_PATH}/${DATE_FLAG}/uid.target
    cat ${LOCAL_BIN_PATH}/uid.whitelist >> ${LOCAL_DATA_PATH}/${DATE_FLAG}/uid.target
    return $?
}

function select_valid_docid() {
    cat ${LOCAL_DATA_PATH}/${DATE_FLAG}/docid | python ${LOCAL_BIN_PATH}/select_valid_docid.py --time_window 60 >${LOCAL_DATA_PATH}/${DATE_FLAG}/docid.target
    return $?
}

function query_sim_score() {
    python ${LOCAL_BIN_PATH}/query_sim_score.py \
        --uid ${LOCAL_DATA_PATH}/${DATE_FLAG}/uid.target \
        --docid ${LOCAL_DATA_PATH}/${DATE_FLAG}/docid.target \
        --batch 500 \
        >${LOCAL_DATA_PATH}/${DATE_FLAG}/sim_scores.out
    return $?
}

function split_scores() {
    local split_res_dir=${LOCAL_DATA_PATH}/${DATE_FLAG}/score_splits
    rm -rf ${split_res_dir}
    mkdir -p ${split_res_dir}
    python ${LOCAL_BIN_PATH}/split_score_results.py \
        --scores ${LOCAL_DATA_PATH}/${DATE_FLAG}/sim_scores.out \
        --part 100 \
        --score_splits ${split_res_dir}
    return $?
}

function select_top_scores() {
    local split_res_dir=${LOCAL_DATA_PATH}/${DATE_FLAG}/score_splits
    mkdir -p ${split_res_dir}

    rm -rf ${LOCAL_DATA_PATH}/${DATE_FLAG}/top_scores.txt

    for score_file in $(ls ${split_res_dir}); do
        python ${LOCAL_BIN_PATH}/select_top_scores.py \
            --score_file ${split_res_dir}/${score_file} \
            --topn 300 >> ${LOCAL_DATA_PATH}/${DATE_FLAG}/top_scores.txt
    done
    return $?
}

function write_redis() {
    python ${LOCAL_BIN_PATH}/write_redis.py \
        --input ${LOCAL_DATA_PATH}/${DATE_FLAG}/top_scores.txt \
        --prefix "nearline" \
        --ttl 172800
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

    dump_uid_from_hive
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    copy_hive_to_local
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    select_target_user
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    select_valid_docid
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    query_sim_score
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    split_scores
    ret=$?
    if [ ${ret} -ne 0 ]; then
        return ${ret}
    fi

    select_top_scores
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
    rm -rf ${LOCAL_DATA_PATH}/${DATE_FLAG}/score_splits

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
