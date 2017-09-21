#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Author: limen
import os
import time
import sys
from datetime import datetime,timedelta
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')


def calcul_brand_competition(cid3,cid3_name):
    #### 选择商品及其品牌商（单个属性和组合属性）
    brand = hc.sql('select * from gdm.gdm_m03_self_item_sku_da where dt = "%s"'% (max_dtt))
    brand = brand.filter((brand.item_third_cate_cd == cid3) & (brand.sku_valid_flag == 1))
    brand = brand.groupby('item_sku_id','barndname_full').agg(max('brand_code').alias('brand_code')).cache()
    # 获取sku之间的替代性
    switch = hc.sql('select * from app.app_cis_ai_slct_switching_prob where cid3 = "%s" ' % (cid3))
    max_dt = hc.sql('select max(dt) from app.app_cis_ai_slct_switching_prob where cid3 = "%s" ' % (cid3)).collect()[0][0]
    switch = switch.filter(switch.dt == max_dt).cache()
    # 计算商品过去两年(一年)的gmv
    begin_dt = datetime.strptime(max_dt, '%Y-%m-%d') - timedelta(days=365)
    begin_dt = str(begin_dt.date())
    ord_data = hc.sql(''' select item_sku_id, after_prefr_amount from  dev.all_sku_order_det  
                           where dt >= "%s" and dt <= "%s" and item_third_cate_cd in ("%s") ''' % (begin_dt, max_dt, cid3)).coalesce(1000)
    ord_data = ord_data.filter(ord_data.after_prefr_amount > 0)
    sku_gmv = ord_data.groupby(['item_sku_id']).agg(sum('after_prefr_amount').alias('gmv')).cache()
    # 关联被替代商品和替代商品的品牌商标签
    brandd = brand[['item_sku_id', 'brand_code']].distinct().withColumnRenamed('item_sku_id', 'src_item_id').withColumnRenamed('brand_code', 'src_brand_code')
    tmp_brand = brandd.withColumnRenamed('src_item_id', 'dst_item_id').withColumnRenamed('src_brand_code','dst_brand_code')
    # 关联商品gmv
    sku_gmv = sku_gmv.withColumnRenamed('gmv', 'src_gmv').withColumnRenamed('item_sku_id', 'src_item_id')
    brand_switch = switch.join(brandd, 'src_item_id', 'inner').join(tmp_brand, 'dst_item_id', 'inner').join(sku_gmv,'src_item_id','inner').cache()
    ##### 计算品牌商之间的替代性金额(A->B)(gvm 加权)
    brand_switch = brand_switch.withColumn('spend_switch', brand_switch.switching_prob * brand_switch.src_gmv)
    brand_switch = brand_switch.groupby(['cid3', 'src_brand_code', 'dst_brand_code']).agg(sum('spend_switch').alias('spend_switch'))
    # 汇总品牌商的被替代金额
    all_switch_out = brand_switch.groupby(['src_brand_code']).agg(sum('spend_switch').alias('all_switch_out'))
    # 计算品牌商的忠诚度
    switch_out = brand_switch.join(all_switch_out, 'src_brand_code', 'inner')
    switch_out = switch_out.withColumn('switch_prob', switch_out.spend_switch / switch_out.all_switch_out)
    switch_out = switch_out[switch_out.src_brand_code == switch_out.dst_brand_code]
    switch_out = switch_out.withColumnRenamed('switch_prob', 'loyalty')
    # 计算品牌商的转出度/被替代度
    switch_out = switch_out.withColumn('switch_out_prob', 1 - switch_out.loyalty).cache()
    #####  计算品牌商的转入程度（A<-B）(gmv 加权)（将替代性按品牌金额加权）
    # 品牌商对非自身品牌的总替代金额
    tmp_switch = brand_switch.filter(brand_switch.src_brand_code != brand_switch.dst_brand_code)
    switch_in = tmp_switch.groupby(['cid3', 'dst_brand_code']).agg(sum('spend_switch').alias('switch_in_spend')).cache()
    #  计算各个品牌商的金额
    market_gmv = brand_switch.groupby('src_brand_code').agg(sum('spend_switch').alias('gmv'))
    all_gmv = market_gmv.groupBy().sum('gmv').collect()[0][0]
    market_gmv = market_gmv.withColumn('all_gmv', lit(all_gmv))
    # 品牌商除去自身金额的整个外部市场金额
    market_gmv = market_gmv.withColumn('out_gmv', market_gmv.all_gmv - market_gmv.gmv)
    market_gmv = market_gmv.withColumnRenamed('src_brand_code', 'dst_brand_code').cache()
    #  计算品牌商转入度
    switch_in = switch_in.join(market_gmv, 'dst_brand_code', 'inner')
    switch_in = switch_in.withColumn('switch_in_prob', switch_in.switch_in_spend / switch_in.out_gmv).cache()
    ####  品牌商对整个市场的净蚕食比例
    # 品牌商净蚕食市场金额
    attack = brand_switch.groupby(['cid3', 'dst_brand_code']).agg(sum('spend_switch').alias('attack_brand')).cache()
    hurt = brand_switch.groupby(['cid3', 'src_brand_code']).agg(sum('spend_switch').alias('hurt_brand')).cache()
    attack = attack.withColumnRenamed('dst_brand_code', 'brand_code')
    hurt = hurt.withColumnRenamed('src_brand_code', 'brand_code')
    erode = attack.join(hurt, ['cid3', 'brand_code'], 'inner')
    erode = erode.withColumn('erode_amount', erode.attack_brand - erode.hurt_brand).cache()
    # 品牌商净蚕食市场比例
    erode = erode.withColumn('erode_percentile', erode.erode_amount / all_gmv)
    ####  保存结果
    switch_in = switch_in[['dst_brand_code', 'cid3', 'switch_in_prob']].withColumnRenamed('dst_brand_code','brand_code')
    switch_out = switch_out[['src_brand_code', 'cid3', 'switch_out_prob', 'loyalty']].withColumnRenamed('src_brand_code', 'brand_code')
    result = switch_out.join(switch_in, ['cid3', 'brand_code'], 'inner')
    brand = brand[['brand_code', 'barndname_full']].distinct()
    result = result.join(brand, 'brand_code', 'left').join(erode,['cid3','brand_code'],'left')
    result = result.withColumn('dt', lit(dt)).withColumn('cid3_name', lit(cid3_name)).withColumn('domain', lit('brand'))
    result = result[['cid3_name','domain','brand_code','barndname_full','switch_in_prob','switch_out_prob','loyalty','erode_percentile','dt','cid3']]
    hc.sql("set hive.exec.dynamic.partition=true")
    hc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    result.write.insertInto('dev.dev_brand_compete_profile', overwrite=True)
    return 1

sc = SparkContext(appName='calculate brand competence')
hc = HiveContext(sc)
dt = str(datetime.now().date())
max_dtt = hc.sql('select max(dt) from gdm.gdm_m03_self_item_sku_da').collect()[0][0]
market_brand = hc.sql("select item_third_cate_cd as cid3,item_third_cate_name as cid3_name from gdm.gdm_m03_self_item_sku_da where dt='%s' and item_third_cate_cd='11922'" % (max_dtt))\
                .select("cid3","cid3_name").distinct().collect()
cid3 = market_brand[0][0]
cid3_name = market_brand[0][1]
a = calcul_brand_competition(cid3,cid3_name)
print 'calcul brand competition of %s succuss'%cid3_name

'''
create table dev.dev_brand_compete_profile(
    cid3_name string comment '三级分类名称',
    domain string comment '领域，对应品牌',
    brand_code string comment '细分品牌商',
    barndname_full string comment '细分品牌商名字',
    switch_in_prob double comment '品牌商转入度',
    switch_out_prob double comment '品牌商转出度，被替代性',
    loyalty double comment '品牌商忠诚度',
    erode_percentile double comment '品牌商蚕食度，蚕食市场比例'
) 
PARTITIONED BY ( 
  `dt` string,
  cid3 string)
ROW FORMAT DELIMITED  
  FIELDS TERMINATED BY '\t'  
stored as orc;
'''
sc = SparkContext(appName='calculate brand competence')
hc = HiveContext(sc)
dt = str(datetime.now().date())
from_tab_brand_sku='gdm.gdm_m03_self_item_sku_da'
market_brand = hc.sql("select item_third_cate_cd as cid3,item_third_cate_name as cid3_name from %s where dt='%s' and item_third_cate_cd in ('11922')" % (from_tab_brand_sku,max_dtt))\
                .select("cid3","cid3_name").distinct().collect()
for i in range(len(market_brand)):
    cid3 = market_brand[i][0]
    cid3_name = market_brand[i][1]
    a = calcul_brand_competition(cid3,cid3_name)
    print 'calcul brand competition of %s succuss' % cid3_name