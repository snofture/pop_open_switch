# -*- coding: utf-8 -*-
"""
Created on Sun Aug 20 13:33:41 2017

@author: limen
"""

import os
import time
import sys
from datetime import datetime,timedelta
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')




def calcul_market_competition(cid3,cid3_name,key_attr,attr_value):
    #### 切分市场（单个属性和组合属性）
    attr = hc.sql('select * from app.app_ai_slct_attributes')
    attr = attr.filter((attr.item_third_cate_cd == cid3) & (attr.web_id == 0))
    attr = attr[attr.attr_name == key_attr]
    attr = attr.join(attr_value, on=['item_third_cate_cd', 'attr_name', 'attr_value'], how='inner')
    attr = attr.groupby('sku_id').agg(max('attr_value').alias('attr_value'))
    ####  计算替代性
    # 获取sku之间的替代性
    switch = hc.sql('select * from app.app_cis_ai_slct_switching_prob where cid3 = "%s" ' % (cid3))
    max_dt = hc.sql('select max(dt) from app.app_cis_ai_slct_switching_prob where cid3 = "%s" ' % (cid3)).collect()[0][0]
    switch = switch.filter(switch.dt == max_dt)
    # 计算商品过去两年(一年)的gmv
    begin_dt = datetime.strptime(max_dt, '%Y-%m-%d') - timedelta(days=365)
    begin_dt = str(begin_dt.date())
    ord_data = hc.sql(''' select item_sku_id, after_prefr_amount from  dev.all_sku_order_det  
                           where dt >= "%s" and dt <= "%s" and item_third_cate_cd in ("%s") ''' %(begin_dt, max_dt, cid3)).coalesce(1000)
    ord_data = ord_data.filter(ord_data.after_prefr_amount > 0)
    sku_gmv = ord_data.groupby(['item_sku_id']).agg(sum('after_prefr_amount').alias('gmv'))
    # 关联商品的市场标签
    attr = attr[['sku_id', 'attr_value']].distinct().withColumnRenamed('sku_id', 'src_item_id').withColumnRenamed('attr_value', 'src_attr_value')
    tmp_attr = attr.withColumnRenamed('src_item_id', 'dst_item_id').withColumnRenamed('src_attr_value','dst_attr_value')
    # 关联 gmv
    sku_gmv = sku_gmv.withColumnRenamed('gmv', 'src_gmv').withColumnRenamed('item_sku_id', 'src_item_id')
    attr_switch = switch.join(attr, 'src_item_id', 'inner').join(tmp_attr, 'dst_item_id', 'inner').join(sku_gmv,'src_item_id','inner')
    ##### 计算细分市场之间的替代性(A->B)(gvm 加权)
    attr_switch = attr_switch.withColumn('spend_switch', attr_switch.switching_prob * attr_switch.src_gmv)
    attr_switch = attr_switch.groupby(['cid3', 'src_attr_value', 'dst_attr_value']).agg(sum('spend_switch').alias('spend_switch'))
    # 汇总细分市场的被替代性
    all_switch_out = attr_switch.groupby(['src_attr_value']).agg(sum('spend_switch').alias('all_switch_out'))
    # 计算细分市场忠诚度
    switch_out = attr_switch.join(all_switch_out, 'src_attr_value', 'inner')
    switch_out = switch_out.withColumn('switch_prob', switch_out.spend_switch / switch_out.all_switch_out)
    switch_out = switch_out[switch_out.src_attr_value == switch_out.dst_attr_value]
    switch_out = switch_out.withColumnRenamed('switch_prob', 'loyalty')
    switch_out = switch_out.withColumn('switch_out_prob', 1 - switch_out.loyalty)
    #####  计算市场的转入程度（A<-B）(gmv 加权)（将替代性按市场份额加权）
    tmp_switch = attr_switch.filter(attr_switch.src_attr_value != attr_switch.dst_attr_value)
    switch_in = tmp_switch.groupby(['cid3','dst_attr_value']).agg(sum('spend_switch').alias('switch_in_spend'))
    #  计算各个市场的 gmv
    market_gmv = attr_switch.groupby('src_attr_value').agg(sum('spend_switch').alias('gmv'))
    all_gmv = market_gmv.groupBy().sum('gmv').collect()[0][0]
    market_gmv = market_gmv.withColumn('all_gmv',lit(all_gmv))
    market_gmv = market_gmv.withColumn('out_gmv',market_gmv.all_gmv - market_gmv.gmv )
    market_gmv = market_gmv.withColumnRenamed('src_attr_value', 'dst_attr_value')
    #  计算转入度
    switch_in = switch_in.join(market_gmv,'dst_attr_value','inner')
    switch_in = switch_in.withColumn('switch_in_prob',switch_in.switch_in_spend/switch_in.out_gmv)
    ####  保存结果
    switch_in = switch_in[['dst_attr_value','cid3','switch_in_prob']].withColumnRenamed('dst_attr_value', 'seg_market')
    switch_out = switch_out[['src_attr_value', 'cid3', 'switch_out_prob']].withColumnRenamed('src_attr_value', 'seg_market')
    result = switch_out.join(switch_in,['cid3','seg_market'], 'inner')
    result = result.withColumn('dt', lit(dt)).withColumn('cid3_name', lit(cid3_name)).withColumn('market', lit(key_attr))
    result = result[['cid3_name', 'market', 'seg_market', 'switch_in_prob', 'switch_out_prob', 'dt', 'cid3']]
    hc.sql("set hive.exec.dynamic.partition=true")
    hc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    result.write.insertInto('dev.dev_market_compete_profile', overwrite=True)
    return 1




#if __name__ == '__main__':
sc = SparkContext(appName='calculate market competence')
hc = HiveContext(sc)
dt = str(datetime.now().date())
day='2017-07-17'
from_tab_market_sku='dev.op_pop_sku_market_da'
market_attr = hc.sql("select sku_id,cid3,cid3_name,attr,attr_value from %s where dt='%s'" % (from_tab_market_sku,day))\
                .select("cid3","cid3_name","attr").distinct().collect() #.toPandas()
attr_value = hc.sql("select cid3 as item_third_cate_cd,attr as attr_name,attr_value from %s where dt='%s'" % (from_tab_market_sku,day)).distinct()


for i in range(len(market_attr)):
    cid3 = market_attr[i][0]
    cid3_name = market_attr[i][1]
    key_attr = market_attr[i][2]
    a = calcul_market_competition(cid3, cid3_name, key_attr, attr_value)
    print 'calcul market competition of %s succuss'%cid3_name  
    

'''
create table dev.dev_market_compete_profile(
    cid3_name string comment '三级分类名称',
    market string comment '市场，对应属性',
    seg_market string comment '细分市场，对应属性值或者属性值组合',
    switch_in_prob double comment '市场转入度',
    switch_out_prob double comment '被替代性'
) 
PARTITIONED BY ( 
  `dt` string,
  cid3 string)
ROW FORMAT DELIMITED  
  FIELDS TERMINATED BY '\t'  
stored as orc;
'''