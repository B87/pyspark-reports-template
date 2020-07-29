
# PySpark Report

This project is meant to be a template for pyspark reporting projects, it uses data from an apache server.

It uses Python 3.8 and Spark 3 to process the input into different reports.


## Usage

Usage (dependencies from Pipfile or requeriments.txt needed):

Run the cli tool: 

`python cli.py -h`

```
usage: cli.py [-h] [-l] [-src SOURCE_FORMAT] [-i INPUT] [-o OUTPUT] report

Shell util to generate reports

positional arguments:
  report                ID of the report to execute

optional arguments:
  -h, --help            show this help message and exit
  -l, --list            show all available report IDs and exit
  -s SOURCE_FORMAT, --source_format SOURCE_FORMAT
                        data source format, local_file by default
  -i INPUT, --input INPUT
                        path of the input file to generate the reports, data/access.log by default
  -o OUTPUT, --output OUTPUT
                        output mode, it prints the report into the console by default
```

For example: 

`python cli.py top10-pages-persite`

## Source formats

- local_file

## Output formats

- console

## Available Reports

ID | Description
--- | --- 
top10-ips-grouped | The top 10 IPs making the largest number of requests.
top10-pages-ko | Top 10 unsuccessful page requests.
count-requests-perminute | Total number of made requests every minute in the entire time period covered by the provided file.
top10-pages-persite | Top 10 requested pages and number of made requests for each one.
ko-requests-percent | Percentage of unsuccessful requests (anything that is not in the 200s or 300s range).
top10-ips-requested-pages | For each of the top 10 IPs, show the top 5 pages requested and the number of requests for each one.
ok-requests-percent | Percentage of successful requests (anything in the 200s and 300s range).

## Execute tests

Run with pytest or any other method, I used a smaller and modified data set for the test suite
