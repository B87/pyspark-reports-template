import argparse
import sys

from pyspark.sql import SparkSession

from base import BaseReport

import os

default_data_file = os.path.join(os.path.dirname(__file__), 'data/access.log')

def all_subclasses(cls):
    import reports # Needed to load reports.py into env and find subclasses of BaseReport
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)])

class ListReports(argparse.Action):
    
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)
        reports = all_subclasses(BaseReport)
        print('ID | Description') 
        print('--- | --- ')
        for report in reports:
            print('{} | {}'.format(report.name, report.description))
        sys.exit(0)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description = 'Shell util to generate reports')

    parser.add_argument("report", type=str, help='ID of the report to execute')
    parser.add_argument("-l", "--list", action=ListReports, nargs=0, help='show all available report IDs and exit', required = False)
    parser.add_argument("-s", "--source_format", help='data source format, local_file by default', \
        required = False, default = 'local_file' )
    parser.add_argument("-i", "--input", help='path of the input file to generate the reports, {} by default'.format(default_data_file), \
        required = False, default = default_data_file)
    parser.add_argument("-o", "--output", help='output mode, it prints the report into the console by default', \
        required = False, default = 'console' )
    
    args = parser.parse_args()
    
    reports = all_subclasses(BaseReport)

    spark = SparkSession \
            .builder \
            .appName('Spark Report Generator') \
            .getOrCreate()

    params = {
        'report' : args.report,
        'source_format' : args.source_format,
        'input_path' : args.input,
        'output' : args.output,
    }

    instance = None
    for report in reports:
        if report.name == args.report:
            print('Creating report [{}]\nWith params : {}'.format(report.name, params))
            instance = report(spark, params)
            instance.generate()
    if not instance:
        raise Exception('Report name provided [{}] is not valid'.format(args.report))