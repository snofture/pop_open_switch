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

sc = SparkContext(appName='calculate brand competence')
hc = HiveContext(sc)
dt = str(datetime.now().date())
max_dtt = hc.sql('select max(dt) from gdm.gdm_m03_self_item_sku_da').collect()[0][0]
market_brand = hc.sql("select item_third_cate_cd as cid3,item_third_cate_name as cid3_name from gdm.gdm_m03_self_item_sku_da where dt='%s' and item_third_cate_cd='11922'" % (max_dtt))\
                .select("cid3","cid3_name").distinct().collect()
cid3 = market_brand[0][0]
cid3_name = market_brand[0][1]


#### 切分市场（单个属性和组合属性）
brand = hc.sql('select * from gdm.gdm_m03_self_item_sku_da where dt = "%s"'% (max_dtt))
brand = brand.filter((brand.item_third_cate_cd == cid3) & (brand.sku_valid_flag == 1))
brand = brand.groupby('item_sku_id','barndname_full').agg(max('brand_code').alias('brand_code'))
# 获取sku之间的替代性
switch = hc.sql('select * from app.app_cis_ai_slct_switching_prob where cid3 = "%s" ' % (cid3))
max_dt = hc.sql('select max(dt) from app.app_cis_ai_slct_switching_prob where cid3 = "%s" ' % (cid3)).collect()[0][0]
switch = switch.filter(switch.dt == max_dt).cache()
# 计算商品过去两年(一年)的gmv
begin_dt = datetime.strptime(max_dt, '%Y-%m-%d') - timedelta(days=15)
begin_dt = str(begin_dt.date())
ord_data = hc.sql(''' select item_sku_id, after_prefr_amount from  dev.all_sku_order_det  
                       where dt >= "%s" and dt <= "%s" and item_third_cate_cd in ("%s") ''' % (begin_dt, max_dt, cid3)).coalesce(1000)
ord_data = ord_data.filter(ord_data.after_prefr_amount > 0)
sku_gmv = ord_data.groupby(['item_sku_id']).agg(sum('after_prefr_amount').alias('gmv')).cache()
# 关联商品的市场标签
brandd = brand[['item_sku_id','brand_code']].distinct().withColumnRenamed('item_sku_id', 'src_item_id').withColumnRenamed('brand_code', 'src_brand_code')
tmp_brand = brandd.withColumnRenamed('src_item_id', 'dst_item_id').withColumnRenamed('src_brand_code','dst_brand_code')
# 关联 gmv
sku_gmv = sku_gmv.withColumnRenamed('gmv', 'src_gmv').withColumnRenamed('item_sku_id', 'src_item_id')
brand_switch = switch.join(brandd, 'src_item_id', 'inner').join(tmp_brand, 'dst_item_id', 'inner').join(sku_gmv,'src_item_id','inner').cache()
##### 计算细分市场之间的替代性(A->B)(gvm 加权)
brand_switch = brand_switch.withColumn('spend_switch', brand_switch.switching_prob * brand_switch.src_gmv)
brand_switch = brand_switch.groupby(['cid3', 'src_brand_code','dst_brand_code']).agg(sum('spend_switch').alias('spend_switch'))
brand_switch = brand_switch.withColumn('combine', concat(brand_switch.src_brand_code,brand_switch.dst_brand_code))


tmp_brand_switch = brand_switch
tmp_brand_switch = tmp_brand_switch.withColumn('tmp_combine',concat(tmp_brand_switch.dst_brand_code,tmp_brand_switch.src_brand_code))
tmp_brand_switch = tmp_brand_switch.withColumnRenamed('spend_switch','tmp_spend_switch')
tmp_brand_switch = tmp_brand_switch[['cid3','tmp_combine','tmp_spend_switch']]
tmp_brand_switch = tmp_brand_switch.withColumnRenamed('tmp_combine','combine')
tmp_brand_switch = tmp_brand_switch[['cid3','combine','tmp_spend_switch']]

all_switch = brand_switch.join(tmp_brand_switch,['cid3','combine'],'inner')
all_switch = all_switch[all_switch.src_brand_code != all_switch.dst_brand_code]
all_switch = all_switch.withColumn('brand_to_brand_erode_amount',all_switch.spend_switch-all_switch.tmp_spend_switch)
all_switch = all_switch.orderBy(all_switch.dst_brand_code)
all_switch = all_switch.withColumn('dt', lit(dt))
all_switch = all_switch[['cid3','src_brand_code','dst_brand_code','spend_switch','tmp_spend_switch','brand_to_brand_erode_amount','dt']]


hc.sql("set hive.exec.dynamic.partition=true")
hc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
all_switch.write.insertInto('dev.dev_brand_erode_profile', overwrite=True)

'''
create table dev.dev_brand_erode_profile(
    cid3 string comment '三级分类',
    src_brand_code string comment '被替代品牌商',
    dst_brand_code string comment '替代品牌商',
    spend_switch double comment 'dst对src蚕食金额',
    tmp_spend_switch double comment 'src对dst蚕食金额',
    brand_to_brand_erode_amount double comment 'dst对src净蚕食金额'
) 
PARTITIONED BY ( 
  `dt` string)
ROW FORMAT DELIMITED  
  FIELDS TERMINATED BY '\t'  
stored as orc;
'''