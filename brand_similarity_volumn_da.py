# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 14:46:52 2017

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

spark.conf.set("spark.sql.crossJoin.enabled", "true")
sc = SparkContext(appName='caculate brand vender volumn similarity')
hc = HiveContext(sc)


dt = str(datetime.now().date()-timedelta(days=1))
begin_dt = datetime.strptime(dt, '%Y-%m-%d') - timedelta(days=31)
begin_dt = begin_dt.strftime('%Y-%m-%d')


# 获取品牌商信息
#top_brand_query = ''' select item_third_cate_cd, brand_code, item_sku_id from gdm.gdm_m03_self_item_sku_da
#where item_third_cate_cd in (11924,11925,6739,11922,13550,11923) '''
#
#top_brand_id = hc.sql(top_brand_query).coalesce(50).distinct().cache()

# 获取sku gmv信息
#sku_gmv_query = '''select item_sku_id, (after_prefr_amount-pop_shop_jq_pay_amount-pop_shop_dq_pay_amount-
#pop_shop_lim_sku_jq_pay_amount-pop_shop_lim_sku_dq_pay_amount+sku_freight_amount+delv_ser_fee_amount+
#worldwide_tax_amount)as sku_GMV 
#from gdm.gdm_m04_ord_det_sum as ord 
#join app.app_ai_slct_attributes as att
#on ord.item_sku_id = att.sku_id 
#where att.web_id = 0 and 
#ord.item_third_cate_cd in (11924,11925,6739,11922,13550,11923) and
#ord.dt >= '%s' and 
#ord.dt <= '%s' and
#att.dt >= '%s' and 
#att.dt <= '%s'
#'''% (begin_dt,dt,begin_dt,dt)


#sku_gmv = hc.sql(sku_gmv_query)
#single_sku_gmv = sku_gmv.groupby(['item_sku_id']).agg(sum('sku_GMV').alias('item_sku_gmv'))


# 合并品牌商及其sku gmv
#brand_gmv = top_brand_id.join(single_sku_gmv,'item_sku_id','inner')
# 获取品牌商gmv信息
brand_gmv_query = ''' select cid3, brand_code, gmv from dev.self_sku_pv
where cid3 in (11924,11925,6739,11922,13550,11923) and dt >= '%s' and dt <= '%s'
''' % (begin_dt,dt)

brand_gmv = hc.sql(brand_gmv_query).coalesce(50).distinct().cache()
# 获取品牌商gmv
top_brand_gmv = brand_gmv.groupby(['cid3','brand_code']).agg(sum('gmv').alias('brand_gmv')).cache()


# 品牌商gmv排名
gmv_rank = Window.partitionBy('cid3').orderBy(top_brand_gmv.brand_gmv.desc())
top_brand_gmv = top_brand_gmv.filter(top_brand_gmv.brand_gmv > 0).withColumn('gmv_rank',rank().over(gmv_rank))


similar_top_brand_gmv=top_brand_gmv.withColumnRenamed('cid3','item_third_cate_cd').withColumnRenamed('brand_code','similar_brand_code').withColumnRenamed("gmv_rank", "similar_gmv_rank").withColumnRenamed("brand_gmv","similar_brand_gmv")
top_brand_gmv = top_brand_gmv.crossJoin(similar_top_brand_gmv).filter("(similar_gmv_rank>gmv_rank and similar_gmv_rank<=gmv_rank+10) or (similar_gmv_rank>=gmv_rank-10 and similar_gmv_rank<gmv_rank)")
top_brand_gmv = top_brand_gmv.filter("cid3 == item_third_cate_cd")
top_brand_gmv = top_brand_gmv[['cid3','brand_code','brand_gmv','gmv_rank','item_third_cate_cd','similar_brand_code','similar_brand_gmv','similar_gmv_rank']]
top_brand_gmv = top_brand_gmv.select('cid3','brand_code','brand_gmv','gmv_rank','similar_brand_code','similar_brand_gmv','similar_gmv_rank')


# 保存结果
top_brand_gmv = top_brand_gmv[top_brand_gmv.brand_code != top_brand_gmv.similar_brand_code]
hc.registerDataFrameAsTable(top_brand_gmv, "table1")
insert_sql = '''insert overwrite table dev.dev_open_brand_similarity_volumn_da  
                partition(dt='%s') select * from table1'''%(dt)
hc.sql(insert_sql)



CREATE TABLE IF NOT EXISTS dev.dev_open_brand_similarity_volumn_da(
cid3 STRING COMMENT "三级分类码",
brand_code STRING COMMENT "品牌商代码",
brand_gmv FLOAT COMMENT "三级分类下品牌商GMV",
gmv_rank INT COMMENT "三级分类下品牌商GMV排名",
similar_brand_code STRING COMMENT "三级分类下相似品牌商代码",
similar_brand_gmv FLOAT COMMENT "三级分类下相似品牌商GMV",
similar_gmv_rank INT COMMENT "三级分类下相似品牌商GMV排名") 
COMMENT "品牌商规模相似性"
PARTITIONED BY ( 
  `dt` string)
ROW FORMAT DELIMITED  
  FIELDS TERMINATED BY '\t'  
stored as orc;





'''
#top_brand_gmv = top_brand_gmv[['brand_code','gmv_rank','item_third_cate_cd']]

brand_top_gmv = top_brand_gmv[['brand_code','gmv_rank','item_third_cate_cd']].withColumnRenamed('brand_code','similar_brand_code').withColumndRenames('gmv_rank','similar_gmv_rank')

result = top_brand_gmv.join(brand_top_gmv,'item_third_cate_cd','inner')
result = result.filter((result.gmv_rank >= result.similar_gmv_rank-10) & (result.gmv_rank <= result.similar_gmv_rank+10))

result = result.withColumnRenamed('cid3','group_id').withColumn('mode',lit('cid3'))
result = result[['vender_id','gmv_rank','similar_vender_id','similar_vender_gmv_rank','group_id','mode']]

top_brand = top_brand_gmv.select('brand_code','gmv_rank')


similar_brand = top_brand[['brand_code','gmv_rank']].withColumnRenamed('brand_code','similar_brand_code')\
                        .withColumnRenamed('gmv_rank','similar_brand_gmv_rank')

'''





























