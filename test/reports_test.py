import unittest
import datetime

from pyspark.sql import SparkSession

from reports import (CountRequestsPerMinute, Top10IpsGrouped, Top10IpsRequestedPages, 
    Top10PagesPerSite, OkRequestsPercent, KoRequestsPercent, Top10PagesKo)

from udf import round_date_to_minute, round_date_fixed_bin, request_ok, request_ko

class TestReports(unittest.TestCase):

    spark = SparkSession \
            .builder \
            .appName('Spark Report Generator') \
            .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    default_test_params = {
        'source_format' : 'local_file',
        'input_path' : './test/data/access.log', # USING REDUCED AND MODIFIED DATASET
        'output' : 'return_object', # RETURN REPORT AS DF INSTEAD OF PRINTING THE RESULT
    }

    time_format = '%Y-%m-%d %H:%M:%S'

    def test_round_date_to_minute(self):
        input1 = datetime.datetime.strptime('2020-02-07 00:21:20', self.time_format)
        expected1 = datetime.datetime.strptime('2020-02-07 00:21:00', self.time_format)
        self.assertEqual(round_date_to_minute(input1), expected1)

        input2 = datetime.datetime.strptime('2020-02-07 00:21:30', self.time_format)
        expected2 = datetime.datetime.strptime('2020-02-07 00:22:00', self.time_format)
        self.assertEqual(round_date_to_minute(input2), expected2)

        input3 = datetime.datetime.strptime('2020-02-07 00:21:45', self.time_format)
        expected3 = datetime.datetime.strptime('2020-02-07 00:22:00', self.time_format)
        self.assertEqual(round_date_to_minute(input3), expected3)

    def test_round_date_fixed_bin(self):
        input1 = datetime.datetime.strptime('2020-02-07 00:21:20', self.time_format)
        expected1 = datetime.datetime.strptime('2020-02-07 00:21:00', self.time_format)
        self.assertEqual(round_date_fixed_bin(input1), expected1)

        input2 = datetime.datetime.strptime('2020-02-07 00:21:30', self.time_format)
        expected2 = datetime.datetime.strptime('2020-02-07 00:21:00', self.time_format)
        self.assertEqual(round_date_fixed_bin(input2), expected2)

        input3 = datetime.datetime.strptime('2020-02-07 00:21:45', self.time_format)
        expected3 = datetime.datetime.strptime('2020-02-07 00:21:00', self.time_format)
        self.assertEqual(round_date_fixed_bin(input3), expected3)

    def test_request_ok(self):
        self.assertTrue(request_ok(200))
        self.assertTrue(request_ko(400))

    def test_top10_pages_persite(self):
        test_params = self.default_test_params
        test_params['report'] = 'top10-pages-persite'
        report = Top10PagesPerSite(self.spark, test_params)
        result = report.generate()
        self.assertEqual(result.take(1)[0].request,'/category/games')
    
    def test_ok_requests_percent(self):
        test_params = self.default_test_params
        test_params['report'] = 'ok-requests-percent'
        report = OkRequestsPercent(self.spark, test_params)
        result = report.generate()
        self.assertEqual(result.take(1)[0].percent_ok, 97.67441860465117)
    
    def test_ko_requests_percent(self):
        test_params = self.default_test_params
        test_params['report'] = 'ko-requests-percent'
        report = KoRequestsPercent(self.spark, test_params)
        result = report.generate()
        self.assertEqual(result.take(1)[0].percent_ko, 2.3255813953488373)
    
    def test_top10_pages_ko(self):
        test_params = self.default_test_params
        test_params['report'] = 'top10-pages-ko'
        report = Top10PagesKo(self.spark, test_params)
        result = report.generate()
        self.assertEqual(result.count(), 1)

    def test_top10_ips_grouped(self):
        test_params = self.default_test_params
        test_params['report'] = 'top10-ips-grouped'
        report = Top10IpsGrouped(self.spark, test_params)
        result = report.generate()
        self.assertEqual(result.take(1)[0]['ip'], '68.96.163.93')

    def test_top10_ips_requested_pages(self):
        test_params = self.default_test_params
        test_params['report'] = 'top10-ips-requested-pages'
        report = Top10IpsRequestedPages(self.spark, test_params)
        result = report.generate().take(1)[0]
        self.assertEqual(result['ip'], '112.96.118.62')
        self.assertEqual(result['request'], '/category/software')
        self.assertEqual(result['count'], 1)
    
    def test_count_requests_perminute(self):
        test_params = self.default_test_params
        test_params['report'] = 'count-requests-perminute'
        report = CountRequestsPerMinute(self.spark, test_params)
        result = report.generate()
        self.assertEqual(result.take(1)[0]['count'], 5)

if __name__ == '__main__':
    unittest.main()