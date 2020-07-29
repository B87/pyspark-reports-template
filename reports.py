import udf
from base import MockReportBase

from pyspark.sql.functions import col, when, lit, row_number
from pyspark.sql.window import Window

class Top10PagesPerSite(MockReportBase):
    name = 'top10-pages-persite'
    description = 'Top 10 requested pages and number of made requests for each one.'

    def get_result(self, input):
        return input.groupBy('request').count() \
            .sort(col('count').desc()) \
            .limit(10)

class OkRequestsPercent(MockReportBase):
    name = 'ok-requests-percent'
    description = 'Percentage of successful requests (anything in the 200s and 300s range).'

    def get_result(self, input):
        return input.withColumn('ok_count', \
                when(udf.request_ok_udf(col('response_status')), 1) \
                .otherwise(0)) \
            .withColumn('ko_count', \
                when(udf.request_ko_udf(col('response_status')), 1) \
                .otherwise(0)) \
            .groupBy(lit('1')).agg({'ok_count':'sum', 'ko_count': 'sum'}) \
            .toDF('1', 'ko', 'ok') \
            .selectExpr('ok * 100 / (ko + ok) AS percent_ok')

class KoRequestsPercent(MockReportBase):
    name = 'ko-requests-percent'
    description = 'Percentage of unsuccessful requests (anything that is not in the 200s or 300s range).'

    def get_result(self, input):
        return input.withColumn('ok_count', \
                when(udf.request_ok_udf(col('response_status')), 1) \
                .otherwise(0)) \
            .withColumn('ko_count', \
                when(udf.request_ko_udf(col('response_status')), 1) \
                .otherwise(0)) \
            .groupBy(lit('1')).agg({'ok_count':'sum', 'ko_count': 'sum'}) \
            .toDF('1', 'ko', 'ok') \
            .selectExpr('ko * 100 / (ko + ok) AS percent_ko')

class Top10PagesKo(MockReportBase):
    name = 'top10-pages-ko'
    description = 'Top 10 unsuccessful page requests.'

    def get_result(self, input):
        return input.filter(udf.request_ko_udf(col('response_status'))) \
            .groupBy('request').count() \
            .sort(col('count').desc()) \
            .limit(10)

class Top10IpsGrouped(MockReportBase):
    name = 'top10-ips-grouped'
    description = 'The top 10 IPs making the largest number of requests.'

    def get_result(self, input):
        return input.groupBy('ip').count() \
            .sort(col('count').desc()) \
            .limit(10)

class Top10IpsRequestedPages(MockReportBase):
    name = 'top10-ips-requested-pages'
    description = 'For each of the top 10 IPs, show the top 5 pages requested and the number of requests for each one.'

    def get_result(self, input):
        top10Ips = Top10IpsGrouped(self.spark, {}).get_result(input).selectExpr('ip AS key')
        inner_join = top10Ips.join(input, top10Ips.key == input.ip) \
            .groupBy('ip', 'request').count()
        
        windowSpec = Window.partitionBy('ip') \
            .orderBy(col('count').desc())    
        ranked = inner_join.withColumn('row_num', row_number().over(windowSpec))

        return ranked.filter(ranked['row_num'] <= 5).select('ip', 'request', 'count') \
            .orderBy(col('ip'), col('count').desc())

class CountRequestsPerMinute(MockReportBase):
    name = 'count-requests-perminute'
    description = 'Total number of made requests every minute in the entire time period covered by the provided file.'

    def get_result(self, input):
        return input.withColumn('ts', udf.round_date_to_minute_udf('ts')) \
            .groupBy('ts').count() \
            .sort(col('ts').asc())