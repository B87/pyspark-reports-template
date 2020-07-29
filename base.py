from array import array
import datetime
from abc import ABCMeta, abstractmethod

import udf

from pyspark.sql.functions import regexp_extract

class Parser():
    __metaclass__ = ABCMeta

    @abstractmethod
    def parse(raw_df):
        pass

class Regexparser(Parser):

    def __init__(self, regexp, groups_to_cols = {}) -> None:
        """
        regex example : r"(.*)"

        groups_to_cols example:
        
        { 
            'col1' : {
                group : 1,
                type : 'int'
            }
        }

        """
        self.regexp = regexp
        self.groups_to_cols = groups_to_cols

    def parse(self, raw_df):
        input = raw_df
        cols = []
        for col_name in self.groups_to_cols.keys():
            meta = self.groups_to_cols[col_name]
            cols.append(col_name)
            if meta['type'] == 'timestamp':
                input = input.withColumn(
                    col_name, 
                    udf.parse_ts_udf(regexp_extract(input.value, self.regexp, meta['group'])).cast(meta['type'])
                )
            else:
                input = input.withColumn(
                    col_name, 
                    regexp_extract(input.value, self.regexp, meta['group']).cast(meta['type'])
                )
        return input.select(*cols)

class Source():
    __metaclass__ = ABCMeta

    def __init__(self, spark, params) -> None:
        self.spark = spark
        self.params = params
    
    @abstractmethod
    def read_as_df():
        pass

class LocalFile(Source):
    def read_as_df(self):
        return self.spark.read.text(self.params['file_path'])

class BaseReport():
    __metaclass__ = ABCMeta

    def __init__(self, spark, params):
        self.spark = spark
        self.params = params
    
    @property
    @abstractmethod
    def name(self):
        """
        ID for the report, , a name attribute has to exist in each subclass
        """
        pass
    
    @property
    @abstractmethod
    def description(self):
        """
        Description of the report, a description attribute has to exist in each subclass
        """
        pass

    @abstractmethod
    def source(self) -> Source:
        """
        Source used read data from somewhere and return a df with a single string row
        """
        pass

    @abstractmethod
    def parser(self) -> Parser:
        pass

    def generate(self):
        """
        Full report generation process
        """
        input = self.parser().parse(self.source().read_as_df())
        result = self.get_result(input)
        return self.print_result(result)

    @abstractmethod
    def get_result(self, input):
        """
        Returns DF with arbitraty content as a report result, needs to be implemented by each subclass
        """
        pass

    def print_result(self, result):
        """
        Output the report result as desired based on params
        """
        if self.params['output'] == 'console':
            print('Printing report result into console ...')
            result.show(50)
        elif self.params['output'] == 'return_object':
            return result
        else:
            raise Exception('output mode \'{}\'not valid'.format(self.params['output']))



class MockReportBase(BaseReport):
    
    def source(self):
        return LocalFile(self.spark, {'file_path' : self.params['input_path']})

    def parser(self):
        return Regexparser(
            r"^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*\[(.*)\] \" ?[GET|POST|PUT|DELETE|PATCH|COPY|OPTIONS|LINK|UNLINK|PURGE|LOCK|UNLOCK|PROPFIND|VIEW|HEAD]+ (.*) HTTP/.*\" (\d{3}).*$", \
            {
                'ts' : {'type': 'timestamp', 'group': 2},
                'ip': {'type' : 'string','group': 1},
                'request': {'type': 'string', 'group': 3}, 
                'response_status' : {'type': 'int', 'group': 4}
            }
        )