# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 18:38:20 2017

@author: limen
"""

import os
import time
import sys
from datetime import datetime,timedelta
from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql.functions import desc


sc = SparkContext(appName='caculate brand vendor switch')
hc = HiveContext(sc)

dt = str(datetime.now().date())
begin_dt = datetime.strptime(dt, '%Y-%m-%d') - timedelta(days=91)
begin_dt = str(begin_dt.date())

# 获取品牌商信息
top_pop_query = ''' select brand_code from gdm.gdm_m03_self_item_sku_da '''
top_pop_id = hc.sql(top_pop_query).distinct().coalesce(50).cache()
top_pop_id = F.broadcast(top_pop_id)

# 获取会话点击量信息
log_query = ''' select  sku_id as item_sku_id, date(request_tm) as request_dt, 
                        concat(session_id,bs) as session_id,brand_code,item_third_cate_id
                from gdm.gdm_m14_online_log_item_d 
                where  dt >= "%s" and dt < "%s" 
                and session_id is not null and bs in ("1","13","8","311210")
                and item_third_cate_id in (11924,11925,6739,11922,13550,11923)'''%(begin_dt,dt)
                                
data = hc.sql(log_query).coalesce(1200)

# 获取品牌商会话点击里信息
data = top_pop_id.join(data,'brand_code','inner')
data = data.withColumnRenamed('brand_code', 'from_brand_code').cache()
tmp = data.withColumnRenamed('from_brand_code', 'to_brand_code').withColumnRenamed('item_sku_id','to_sku_id').withColumnRenamed('item_third_cate_id', 'to_cid3').cache()
#data = data.withColumnRenamed('brand_code', 'from_brand_code').withColumnRenamed('barndname_full','from_brand_name').cache()
#tmp = data.withColumnRenamed('from_brand_code', 'to_brand_code').withColumnRenamed('item_sku_id','to_sku_id').withColumnRenamed('from_brand_name','to_brand_name').withColumnRenamed('item_third_cate_id', 'to_cid3').cache()

# 过滤同一天的session 才计算替代性
switch = data.join(tmp,['session_id','request_dt'],'inner')
switch = switch.filter(switch.item_third_cate_id == switch.to_cid3)
switch = switch.filter(switch.item_sku_id != switch.to_sku_id)
switch = switch.filter(switch.from_brand_code != switch.to_brand_code)

# 生成新的点击量列，默认值1
switch = switch.withColumn('page_views', F.lit(1)).cache()

# 从会话获取从品牌商到品牌商点击量
switch = switch.groupby('item_third_cate_id','from_brand_code','to_brand_code').agg(F.sum('page_views').alias('page_views_switch'))
new_column = switch.from_brand_code.cast("string")
switch = switch.withColumn('from_brand_code',new_column)

# 从会话获取品牌商总点击量
from_brand_total_switch = switch.groupby('item_third_cate_id','from_brand_code').agg(F.sum('page_views_switch').alias('from_brand_total_switch'))
new_column = from_brand_total_switch.from_brand_code.cast("string")
from_brand_total_switch = from_brand_total_switch.withColumn('from_brand_code',new_column)

# 生成基于点击量的替代性结果
result = switch.join(from_brand_total_switch,['item_third_cate_id','from_brand_code'],'inner')
result = result.withColumn('switch_prob', result.page_views_switch/result.from_brand_total_switch)



#result = result.sort(desc('switch_prob'))
result = result.withColumnRenamed('item_third_cate_id','cid3')
result = result[['cid3','from_brand_code','to_brand_code','page_views_switch','from_brand_total_switch','switch_prob']]

#switch_rank = Window.partitionBy('from_brand_code').orderBy(switch.switch_prob.desc())
#switch = switch.withColumn('switch_rank',rank().over(switch_rank))
#
#switch = switch[['from_brand_code','to_brand_code',
#                 'page_views_switch','from_shop_total_switch','switch_prob','switch_rank']]
# 保存结果
hc.registerDataFrameAsTable(result, "table1")
insert_sql = '''insert overwrite table dev.dev_open_brand_similarity_replacement partition(dt='%s')
                select * from table1'''%(dt)
hc.sql(insert_sql)



CREATE TABLE IF NOT EXISTS dev.dev_open_brand_similarity_replacement(
cid3 STRING COMMENT "三级分类",
from_brand_code STRING COMMENT "被替代的自营品牌商id",
to_brand_code STRING COMMENT "替代from_brand_code的品牌商id",
page_views_switch INT COMMENT "从from_brand_code到to_brand_code的跳转次数",
from_brand_total_switch INT COMMENT "从from_brand_code到其他品牌商总的跳转次数",
switch_prob FLOAT COMMENT "替代概率")
COMMENT "基于会话点击跳转的品牌商替代性" 
PARTITIONED BY ( 
  `dt` string)
ROW FORMAT DELIMITED  
  FIELDS TERMINATED BY '\t'  
stored as orc;

























