#!/bin/bash

ALARM_EMAILS="jiahao.deng@newsbreak.com"

function send_mail_msg() {
    local subject="$1"
    local msg="$2"
    local emails=${ALARM_EMAILS//,/ }

    for email in ${emails}; do
        echo "${msg}" | mail -s "${subject}" ${email}
    done
    return 0
}

ROOT="/data/dengjiahao/nearline"
time_flag=$(date +"%Y%m%d%H%M")
log="${ROOT}/log/log.${time_flag}"

bash -x $ROOT/bin/run.sh &> $log

ret=$?
if [ ${ret} -ne 0 ];then
    subject="${time_flag} update nearline Failure Alarm"
    msg="${time_flag}"
    send_mail_msg "${subject}" "${msg}"
fi
