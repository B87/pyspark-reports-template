import datetime

from pyspark.sql.types import IntegerType, TimestampType, BooleanType
from pyspark.sql.functions import udf

def parse_date(date_str):
    return datetime.datetime.strptime(date_str, '%d/%b/%Y:%H:%M:%S +0000')
    
def request_ok(status):
    return status >= 200 and status < 300

def request_ko(status):
    return not request_ok(status)

def round_date_fixed_bin(date):
   return date - datetime.timedelta(seconds=date.time().second)

def round_date_to_minute(date):
   discard = datetime.timedelta(minutes=date.minute % 1, seconds=date.second, microseconds=date.microsecond)
   date -= discard
   if discard >= datetime.timedelta(seconds=30):
      date += datetime.timedelta(minutes=1)
   return date

parse_ts_udf = udf(parse_date, TimestampType())
request_ok_udf = udf(request_ok, BooleanType())
request_ko_udf = udf(request_ko, BooleanType())
round_date_to_minute_udf = udf(round_date_to_minute, TimestampType())