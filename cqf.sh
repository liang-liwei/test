#!/bin/bash
i=0;
cat /dev/null >log/c.txt
cat /dev/null >log/dzd.txt
echo "请在b3.txt 输入机构号 客户代码"
echo "开始时间：`date '+%F %T'`"
while read splitOrgCode senderNo 
do

if [[ "$splitOrgCode" == NULL || "$splitOrgCode" == '' ]];then
continue;
fi

if [[ "$senderNo" == NULL || "$senderNo" == '' ]];then
senderNo=""
fi

i=`expr $i + 1`

echo "------------------------序号${i}、查询${splitOrgCode}机构${senderNo}客户库里剩余欠费信息-------------------------------">>log/c.txt
echo "------------------------序号${i}、处理${splitOrgCode}机构${senderNo}客户库对账单信息-------------------------------">>log/dzd.txt
HOME=/home/yunwei/yunwei mysql -N -e "select if(is_follow_concession='2',if(follow_concession_state='1','未完成事后优惠','已完成事后优惠'),'-'),'邮件',sum(ifnull(postage_total,0)-ifnull(bill_write_off_amount,0)),DATE_FORMAT(split_date,'%Y-%m'),case when payment_mode='1' then '寄' when payment_mode='2' then '到' when payment_mode='4' then '集中整' else '-' end fff,if(bill_create_state=1,'未','已'),ifnull(bls_business_bill_id,'6'),if(ledger_account_flag=1,'邮政','速递'),sum(ifnull(postage_total,0)) zje FROM bls_waybill where split_org_code='${splitOrgCode}' and sender_no='${senderNo}' and ifnull(postage_total,0)-ifnull(bill_write_off_amount,0)>0 and (business_type='3' or payment_mode='2') and write_off_state<>'1' group by split_org_code,DATE_FORMAT(split_date,'%Y-%m'),payment_mode,bill_create_state,bls_business_bill_id,ledger_account_flag,is_follow_concession,follow_concession_state  union all select if(apply_status=2,'未审批补录修改申请', '-'),'补录',sum(ifnull(add_amount,0)-ifnull(write_off_amount,0)),DATE_FORMAT(split_date,'%Y-%m'),'记欠',if(bill_create_state=0,'未','已'),ifnull(bls_business_bill_id,'8'),if(ledger_account_flag=1,'邮政','速递'),sum(ifnull(add_amount,0)) from bls_customer_arrearage_add where split_org_code='${splitOrgCode}' and customer_no='${senderNo}' and ifnull(add_amount,0)-ifnull(write_off_amount,0)>0 and write_off_state<>'1' and business_type='2' group by split_org_code,date_format(split_date,'%Y-%m'),bill_create_state,bls_business_bill_id,ledger_account_flag,apply_status;"|while read bbb
do 

if [[ "$bbb" == NULL || "$bbb" == "" ]]; then
echo "${splitOrgCode}机构${sendeNo}客户没有欠费">>log/c.txt
continue;
fi

shzt=`echo $bbb |cut -d" " -f 1`
qflx=`echo $bbb |cut -d" " -f 2`
qfye=`echo $bbb |cut -d" " -f 3`
qfsj=`echo $bbb |cut -d" " -f 4`
fff=`echo $bbb |cut -d" " -f 5`
zdzt=`echo $bbb |cut -d" " -f 6`
zdh=`echo $bbb |cut -d" " -f 7`
fzbs=`echo $bbb |cut -d" " -f 8`
qfzje=`echo $bbb |cut -d" " -f 9`

if [ "${zdzt}" == "未" ];then
echo "${shzt}${qflx}欠费余额:${qfye},欠费时间:${qfsj},${fff}付,账单状态:未生成,邮件标识:${fzbs},总金额:${qfzje}" >>log/c.txt
continue;
elif [ "$zdzt" == "已" ];then
if [[ "$zdh" == "6" && "$qflx" == "邮件" ]];then
HOME=/home/yunwei/yunwei mysql -N -e "update bls_waybill set gmt_modified=now(),bill_create_state='1' where split_org_code='${splitOrgCode}' and sender_no='${senderNo}' and bls_business_bill_id is null and bill_create_state='2' and split_date>='${qfsj}-01';"
echo "${splitOrgCode}机构,${senderNo}客户，${qfsj}月份${qflx}账单${zdh}生成失败，已处理，重新生成账单试下" >>log/dzd.txt
echo "${shzt}邮件欠费余额:${qfye},欠费时间:${qfsj},${fff}付,账单状态:未生成,邮件标识:${fzbs},总金额:${qfzje}" >>log/c.txt
continue;
elif [[ "$zdh" == "8" && "$qflx" == "补录" ]];then
HOME=/home/yunwei/yunwei mysql -N -e "update bls_customer_arrearage_add set gmt_modified=now(),bill_create_state='0' where split_org_code='${splitOrgCode}' and customer_no='${senderNo}' and bls_business_bill_id is null and bill_create_state='1'  and split_date>='${qfsj}-01' ;"
echo "${splitOrgCode}机构,${senderNo}客户，${qfsj}月份${qflx}账单${zdh}生成失败，已处理，重新生成账单试下" >>log/dzd.txt
echo "${shzt}补录欠费余额:${qfye},欠费时间:${qfsj},账单状态:未生成,补录标识:${fzbs},总金额:${qfzje}" >>log/c.txt
continue;
else
aa=`sqlplus -s sc_sts/SC_STS_caiwu2020@jdscdb <<EOF
set heading off
set feedback off
set pagesize 0
set verify off
set echo off
set line 3000
select '普通',decode(a.bill_state,'1','未确认','2','已确认','8','撤回中','9','已撤回',a.bill_state) zdzt,NVL(a.WRITE_OFF_AMOUNT,0) hxje,NVL(a.UNWRITE_OFF_AMOUNT,0) qfje,decode(a.WRITE_OFF_STATUS,'0','未核销','2','已核销','部分核销') hxzt,decode(a.LEDGER_ACCOUNT_FLAG,'1','邮政','2','速递','无')zwbs,to_char(a.GEN_DATE,'yyyymmdd')scrq  from STS_BILL a where a.BILL_NO = '${zdh}'  union all select '统结',decode(c.bill_state,'1','未确认','2','已确认','8','撤回中','9','已撤回',c.bill_state) zdzt  ,NVL(c.WRITE_OFF_AMOUNT,0) hxje,NVL(c.UNWRITE_OFF_AMOUNT,0)  qfje ,decode(c.WRITE_OFF_STATUS,'0','核销','2','已核销','部分核销') hxzt ,decode(c.LEDGER_ACCOUNT_FLAG,'1','邮政','2','速递','无') zwbs,to_char(c.GEN_DATE,'yyyymmdd')  scrq from sts_unified_bill c where c.uni_bill_no = '${zdh}';
exit;
EOF
`;
if [[ "${aa}" == NULL || "${aa}" ==  "" ]];then  
bb=`sqlplus -s sc_sts/SC_STS_caiwu2020@jdscdb <<EOF
set heading off
set feedback off
set pagesize 0
set verify off
set echo off
set line 3000
select decode(b.job_mode,'1',decode(b.state,'0','待执行','1','执行中','2','成功','3','异常','4','无数据','5','取消>执行',b.state) ,'2',decode(b.state,'0','待','1','生成中','2','撤回成功','3','撤回异常','4','无数据','5','取消执行',b.state) ,b.job_mode) zxzt,decode(b.job_type,'1','普通','2','统签',b.job_type) zdlx from  sts_bill_job b  where b.BILL_NO = '${zdh}';
exit;
EOF
`;
if [[ "${bb}" == NULL || "${bb}" ==  "" ]];then 
dd=`sqlplus -s sc_sts/SC_STS_caiwu2020@jdscdb <<EOF
select count(1) from sts_bill_product where bill_no='${zdh}' or uni_bill_no='${zdh}' ;
exit;
EOF
`;
if [[ "$dd" == NULL || "${dd}" == "" ]];then
if [ "${qflx}" == "邮件" ];then
HOME=/home/yunwei/yunwei mysql -N -e "update bls_waybill set bls_business_bill_id=null,gmt_modified=now(),bill_create_state='1' where split_org_code='${splitOrgCode}' and sender_no='${senderNo}' and bls_business_bill_id='${zdh}' and bill_create_state='2'  and split_date>='${qfsj}-01';"
echo "${splitOrgCode}机构,${senderNo}客户，${qfsj}月份${qflx}账单${zdh}生成失败，已处理，重新生成账单试下" >>log/dzd.txt
echo "${shzt}邮件欠费余额:${qfye},欠费时间:${qfsj},${fff}付,账单状态:未生成,邮件标识:${fzbs},总金额:${qfzje}" >>log/c.txt
continue;
fi
if [ "${qflx}" == "补录" ];then
HOME=/home/yunwei/yunwei mysql -N -e "update bls_customer_arrearage_add set bls_business_bill_id=null,gmt_modified=now(),bill_create_state='0'where split_org_code='${splitOrgCode}' and customer_no='${senderNo}' and bls_business_bill_id='${zdh}' and bill_create_state='1' and split_date>='${qfsj}-01' ;"
echo "${splitOrgCode}机构,${senderNo}客户，${qfsj}月份${qflx}账单${zdh}生成失败，已处理，重新生成账单试下" >>log/dzd.txt
echo "${shzt}补录欠费余额:${qfye},欠费时间:${qfsj},账单状态:未生成,补录标识:${fzbs},总金额:${qfzje}" >>log/c.txt
continue;
fi
fi
else
state=`echo $aa |cut -d" " -f 1`
zdlx=`echo $aa |cut -d" " -f 2`
echo "${shzt}${qflx}欠费余额:${qfye},欠费时间:${qfsj},${fff}付,账单:${zdh},状态:${state},账单类型:${zdlx}" >>log/c.txt
continue;
fi
else
zdlx=`echo $aa |cut -d" " -f 1` 
billState=`echo $aa |cut -d" " -f 2`
hxje=`echo $aa |cut -d" " -f 3`
whxje=`echo $aa |cut -d" " -f 4`
hxzt=`echo $aa |cut -d" " -f 5`
zwbs=`echo $aa |cut -d" " -f 6`
scrq=`echo $aa |cut -d" " -f 7`
if [ "$billState" == "已撤回" ] ;then
if [ "${qflx}" == "邮件" ];then
HOME=/home/yunwei/yunwei mysql -N -e "update bls_waybill set bls_business_bill_id=null,gmt_modified=now(),bill_create_state='1' where split_org_code='${splitOrgCode}' and sender_no='${senderNo}' and bls_business_bill_id='${zdh}' and bill_create_state='2'  and split_date>='${qfsj}-01';"
echo "${splitOrgCode}机构,${senderNo}客户，${qfsj}月份${qflx}账单${zdh}已撤回，重新生成账单试下" >>log/dzd.txt
echo "${shzt}邮件欠费余额:${qfye},欠费时间:${qfsj},${fff}付,账单状态:未生成,邮件标识:${fzbs},总金额:${qfzje}" >>log/c.txt
fi
if [ "${qflx}" == "补录" ];then
HOME=/home/yunwei/yunwei mysql -N -e "update bls_customer_arrearage_add set bls_business_bill_id=null,gmt_modified=now(),bill_create_state='0' where split_org_code='${splitOrgCode}' and customer_no='${senderNo}' and bls_business_bill_id='${zdh}' and bill_create_state='1'  and split_date>='${qfsj}-01' ;"
echo "${splitOrgCode}机构,${senderNo}客户，${qfsj}月份${qflx}账单${zdh}已撤回，重新生成账单试下" >>log/dzd.txt
echo "${shzt}补录欠费余额:${qfye},欠费时间:${qfsj},账单状态:未生成,补录标识:${fzbs},总金额:${qfzje}" >>log/c.txt
fi
continue;
fi
if [ "${hxzt}" != "已核销" ];then
echo "${shzt}${qflx}欠费余额:${whxje},欠费时间:${qfsj},${fff}付,账单状态:已生成${billState}${hxzt},生成日期:${scrq},账单:${zdh},邮件标识:${fzbs},账单标识:${zwbs},已核销金额:${hxje},总金额:${qfzje}" >>log/c.txt
continue;
fi
fi
fi
fi
done
sleep 1;
done < b3.txt
echo "结束时间：`date '+%F %T'`"
cat log/c.txt
cat log/dzd.txt
cat /dev/null >b3.txt
