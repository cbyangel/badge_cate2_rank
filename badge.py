# Authorized : cho.by@gsshop.com
# Description :

import pyspark
from pyspark.sql import SparkSession, SQLContext, HiveContext, Row
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import date, timedelta

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sqlContext = HiveContext(sc)


yesterday = (date.today() - timedelta(days=1)).strftime('%Y%m%d')

uv_df = sqlContext.sql("select dt, siteid, if(userid != '', userid, pcid) as userid, parse_url(currenturl, 'PATH') as path, parse_url(currenturl,'QUERY','sectSeq') as sectSeq from wcs_onload ").filter(col('path').like('/section/groupSectM%')).filter(col('siteid') =='gsshopMobile').filter(col('dt') == yesterday).filter((col('userid') != '') & col('sectSeq').isNotNull())
split_col = split(uv_df['sectSeq'], '-')
uv_df = uv_df.withColumn('lsect', trim(split_col.getItem(0)))
uv_df = uv_df.withColumn('letter', trim(split_col.getItem(1)))
uv_df = uv_df.withColumn('msect', trim(split_col.getItem(2)))
uv_df = uv_df.filter(col('letter') == 'A')
uv_df = uv_df.select(['lsect', 'msect', 'userid']).groupby(['lsect', 'msect']).agg(countDistinct('userid').alias('uv'))

tmp_view_df = sqlContext.sql("select dt, siteid, if(userid != '', userid, pcid) as userid, itemid, visitedtime, parse_url(currenturl, 'QUERY', 'gsid') as gsid, parse_url(currenturl, 'QUERY', 'sectSeq') as sectseq from user_item_view ").filter(col('gsid') == 'cateshop-mresult').filter(col('siteid') == 'gsshopMobile').filter(col('dt') == yesterday).filter((col('userid') != '') & col('sectSeq').isNotNull())
split_col = split(tmp_view_df['sectSeq'], '-')
tmp_view_df = tmp_view_df.withColumn('msect', trim(split_col.getItem(0)))
tmp_view_df = tmp_view_df.withColumn('l1', trim(split_col.getItem(1)))
tmp_view_df = tmp_view_df.withColumn('l2', trim(split_col.getItem(2)))
clk_uv_df = tmp_view_df.groupby('msect').agg(countDistinct('userid').alias('clk_uv'))


tmp_view_df = tmp_view_df.groupby(['msect','userid','itemid']).agg(min('visitedtime').alias('min_visitedtime'))
tmp_order_df = sqlContext.sql("select dt, siteid, if(userid != '', userid, pcid) as o_userid, prdid as o_prdid, price as o_price, visitedtime as o_visitedtime from user_item_order ").filter(col('siteid') == 'gsshopMobile').filter(col('dt') == yesterday)
cond = [tmp_view_df.userid == tmp_order_df.o_userid, tmp_view_df.itemid == tmp_order_df.o_prdid, tmp_view_df.min_visitedtime < tmp_order_df.o_visitedtime]
ord_uv_df = tmp_view_df.join(tmp_order_df, cond, 'left_outer').filter(col('o_userid').isNotNull())
ord_uv_df = ord_uv_df.select(['msect','userid','itemid', 'o_price']).groupby('msect').agg(countDistinct('userid').alias('ord_uv'), sum('o_price').alias('tot_price') )



cond1 = [uv_df.msect == clk_uv_df.msect]
cond2 = [uv_df.msect == ord_uv_df.msect]
badge_df = uv_df.join(clk_uv_df, cond1, 'left').drop(clk_uv_df.msect).join(ord_uv_df, cond2, 'left').drop(ord_uv_df.msect)
badge_df = badge_df.withColumn('clk_uv', when(badge_df.clk_uv.isNull(), lit(0.01)).otherwise(badge_df.clk_uv))
badge_df = badge_df.withColumn('ord_uv', when(badge_df.ord_uv.isNull(), lit(0.01)).otherwise(badge_df.ord_uv))
badge_df = badge_df.withColumn('tot_price', when(badge_df.tot_price.isNull(), lit(0.01)).otherwise(badge_df.tot_price))
tot_df = badge_df.groupby('lsect').agg(sum('uv').alias('tot_uv'), sum('clk_uv').alias('tot_clk_uv'), sum('ord_uv').alias('tot_ord_uv'))
badge_df = badge_df.join(tot_df, 'lsect')
badge_df = badge_df.withColumn('uv_ratio', round(badge_df.uv / badge_df.tot_uv, 2))
badge_df = badge_df.withColumn('clk_uv_ratio', round(badge_df.clk_uv / badge_df.tot_clk_uv, 2))
badge_df = badge_df.withColumn('ord_uv_ratio', round(badge_df.ord_uv / badge_df.tot_ord_uv, 2))
badge_df = badge_df.withColumn('score', (badge_df.uv_ratio * badge_df.clk_uv_ratio * badge_df.ord_uv_ratio) * 100 )
badge_df = badge_df.select('*', row_number().over(Window.partitionBy(badge_df['lsect']).orderBy(badge_df['score'].desc())).alias('rnum')).filter(col('rnum') <= 2).select('lsect', 'msect', 'score', 'rnum')

out_df = badge_df.select(['lsect', 'msect'])
cate_df =sqlContext.sql("select distinct lsectid as lsect from members_bycho.badge_cate_info")
out_df = out_df.join(cate_df, 'lsect')
out_df.createOrReplaceTempView("out_result")
sqlContext.sql("drop table if exists members_bycho.badge_result_out")
sqlContext.sql("create external table members_bycho.badge_result_out(lsect string, msect string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '/service/member/bycho/badge/out' ")


sqlContext.sql("INSERT OVERWRITE TABLE members_bycho.badge_result_out  SELECT * FROM out_result")

