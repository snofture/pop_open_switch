# -*- coding: utf-8 -*-
"""
Created on Wed Aug 16 14:26:51 2017

@author: limen
"""

import os
import time
import sys
from datetime import datetime,timedelta
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark import SparkContext, SparkConf

# 每天计算昨天的
dt = str(datetime.now().date()-timedelta(days=1))
sc = SparkContext(appName='caculate self brand sku overlap')
hc = HiveContext(sc)

last_dt = hc.sql('select max(dt) from  dev.dev_self_vender_sku_match ').collect()[0][0]


brand_mainsku_match = hc.sql('''select main_sku_id1,brand_code1,brand_name1,main_sku_id2,brand_code2,brand_name2
                             from dev.dev_self_vender_sku_match 
                             where item_third_cate_cd1 in (11924,11925,6739,11922,13550,11923)
                             and item_third_cate_cd2 in (11924,11925,6739,11922,13550,11923)
                             and dt == "%s" '''%(last_dt)).coalesce(100).cache()

tmp_mainsku_match = hc.sql('''select main_sku_id2 as main_sku_id1,brand_code2 as brand_code1,brand_name2 as brand_name1,
                           main_sku_id1 as main_sku_id2,brand_code1 as brand_code2,brand_name1 as brand_name2  
                            from dev.dev_self_vender_sku_match 
                            where item_third_cate_cd1 in (11924,11925,6739,11922,13550,11923)
                            and item_third_cate_cd2 in (11924,11925,6739,11922,13550,11923)
                            and dt == "%s" '''%(last_dt)).coalesce(100).cache()

all_sku_match = brand_mainsku_match.union(tmp_mainsku_match)
all_sku_match = all_sku_match.distinct()


# 汇总sku匹配数目
sku_match = all_sku_match.groupby('brand_code1','brand_name1','brand_code2','brand_name2').agg(countDistinct(all_sku_match.main_sku_id1).alias('match_sku_num')).cache()
sku_match = sku_match.sort('brand_code1','match_sku_num')
# 计算每个店铺有匹配关系的spu数目
shop_sku_num = all_sku_match.groupby('brand_code1').agg(countDistinct(all_sku_match.main_sku_id1).alias('sku_num'))

# 计算重合度
sku_match = sku_match.join(shop_sku_num,'brand_code1','inner')
sku_match = sku_match.withColumn('overlap_ratio',sku_match.match_sku_num/sku_match.sku_num)



sku_match= sku_match[['brand_code1','brand_name1','brand_code2','brand_name2',
                'match_sku_num','sku_num','overlap_ratio']]
# 保存到hive表
hc.registerDataFrameAsTable(sku_match, "table1")
insert_sql = '''insert overwrite table dev.dev_open_brand_similarity_spu_overlap_da partition(dt="%s") 
                select * from table1'''%(dt)
hc.sql(insert_sql)



CREATE TABLE IF NOT EXISTS dev.dev_open_brand_similarity_spu_overlap_da(
brand_code1 STRING,
brand_name1 STRING,
brand_code2 STRING,
brand_name2 STRING,
match_sku_num INT,
sku_num INT,
overlap_ratio FLOAT) 
PARTITIONED BY ( 
  `dt` string)
ROW FORMAT DELIMITED  
  FIELDS TERMINATED BY '\t'  
stored as orc;


















