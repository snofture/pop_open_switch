# -*- coding: utf-8 -*-
"""
Created on Fri Aug 18 11:26:42 2017

@author: limen
"""

import os
import time
import sys
from pyspark.sql.types import StringType,IntegerType
from pyspark.sql.functions import udf
from pyspark.sql import Row
from datetime import datetime,timedelta
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark import SparkContext, SparkConf
reload(sys)
sys.setdefaultencoding('utf-8')



sc = SparkContext(appName='pop_brand_similarity_summary ')
hc = HiveContext(sc)

dt = str(datetime.now().date()-timedelta(days=1))

# 读取 替代性相似商家默认取最新的分区的数
max_switch_dt = hc.sql('select max(dt) from dev.dev_open_brand_similarity_replacement ').collect()[0][0]

switch_query = ''' select cid3,from_brand_code as brand_code_origin,to_brand_code as brand_code_similar,dt,switch_prob
                    from dev.dev_open_brand_similarity_replacement where dt == "%s" '''%(max_switch_dt)
switch = hc.sql(switch_query)
switch_rank = Window.partitionBy('cid3','brand_code_origin').orderBy(switch.switch_prob.desc())
switch = switch.withColumn('switch_rank',rank().over(switch_rank))


switch_prob = switch.groupby(switch.cid3).agg(avg(switch.switch_prob).alias('cid3_avg_switch_prob'))
switch = switch.join(switch_prob,'cid3','inner')


switch = switch.filter((switch.switch_prob >= switch.cid3_avg_switch_prob) & (switch.switch_rank <= 10))
switch = switch.withColumn('similarity_type',lit('1'))
switch = switch.sort('cid3','brand_code_origin')
switch = switch[['cid3','brand_code_origin','brand_code_similar','similarity_type']]


# 读取 规模相似商家
max_volumn_dt = hc.sql('select max(dt) from dev.dev_open_brand_similarity_volumn_da ').collect()[0][0]

volumn_query = ''' select cid3,brand_code as brand_code_origin, similar_brand_code as brand_code_similar,dt
                    from dev.dev_open_brand_similarity_volumn_da where dt == "%s" '''%(max_volumn_dt)
volumn = hc.sql(volumn_query)
volumn = volumn.withColumn('similarity_type',lit('3'))
volumn = volumn.sort('cid3','brand_code_origin')
volumn = volumn[['cid3','brand_code_origin','brand_code_similar','similarity_type']]



# 读取 重合相似商家
max_overlap_dt = hc.sql('select max(dt) from dev.dev_open_brand_similarity_spu_overlap_da ').collect()[0][0]

overlap_query = ''' select cid3,brand_code1 as brand_code_origin, brand_code2 as brand_code_similar,dt,overlap_ratio 
                    from dev.dev_open_brand_similarity_spu_overlap_da where dt=="%s" '''%(max_overlap_dt)
overlap = hc.sql(overlap_query)
overlap = overlap.withColumn('similarity_type',lit('2'))
overlap_rank = Window.partitionBy('cid3','brand_code_origin').orderBy(overlap.overlap_ratio.desc())
overlap = overlap.withColumn('overlap_rank',rank().over(overlap_rank))

sku_overlap_ratio = overlap.groupby(overlap.cid3).agg(avg(overlap.overlap_ratio).alias('cid3_avg_overlap_ratio'))
overlap = overlap.join(sku_overlap_ratio,'cid3','inner')


overlap = overlap.filter((overlap.overlap_ratio > overlap.cid3_avg_overlap_ratio) & (overlap.overlap_rank <= 50))
overlap = overlap.sort('cid3','brand_code_origin')
overlap = overlap[['cid3','brand_code_origin','brand_code_similar','similarity_type']]


result = volumn.union(switch).union(overlap)
result = result[result.brand_code_origin != result.brand_code_similar]
hc.registerDataFrameAsTable(result, "table1")
insert_sql = '''insert overwrite table dev.dev_open_brand_similarity_da 
                partition(dt='%s') select * from table1'''%(dt)
hc.sql(insert_sql)


CREATE TABLE IF NOT EXISTS dev.dev_open_brand_similarity_da(
cid3 STRING COMMENT "三级分类",
brand_code_origin STRING COMMENT "被替代的自营品牌商id",
brand_code_similar STRING COMMENT "替代from_brand_code的品牌商id",
similarity_type STRING COMMENT "算法类型出处")
COMMENT "三级分类层级下，三种相似性算法合并出的相似品牌商" 
PARTITIONED BY ( 
  `dt` string)
ROW FORMAT DELIMITED  
  FIELDS TERMINATED BY '\t'  
stored as orc;




























































