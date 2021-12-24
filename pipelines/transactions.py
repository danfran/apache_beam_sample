"""
Read transactions from a csv file, filter and sum them by date.
The output generate has optional format: csv or jsonl.
"""

import argparse
from datetime import datetime
import json
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


"""
Contains the Composite Transformation to parse and filter
the data coming from the input.
"""
class SumTransactionsPerDate(beam.PTransform):

    # optimistic here as considering values always present and correctly formatted
    def _parse_transaction_record(self, input_record):
        transaction_timestamp, _, _, transaction_amount = input_record.split(',')
        transaction_date = datetime.strptime(transaction_timestamp, '%Y-%m-%d %H:%M:%S %Z').date()
        logging.log(logging.DEBUG, f'Timestamp conversion from {transaction_timestamp} to {transaction_date}')
        return transaction_date, float(transaction_amount)

    def expand(self, pcoll):
        return \
            pcoll \
            | 'Split' >> beam.Map(self._parse_transaction_record) \
            | 'FilterByTransactionYearAndAmount' >> beam.Filter(
                lambda columns: columns[0].year > 2009 and columns[1] > 20) \
            | 'MapKey' >> beam.Map(lambda columns: (str(columns[0]), columns[1])) \
            | 'GroupingByDate' >> beam.CombinePerKey(sum)


def run(argv=None):
    # parse the input options
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        default='gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv',
        help='Input file to process.')

    parser.add_argument(
        '--output-format',
        dest='output_format',
        default='jsonl',
        help='Output file format (csv, jsonl) to write results to.')

    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    # parse the input file, transform and write the result to output with chosen format and path
    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | 'Read' >> ReadFromText(known_args.input, skip_header_lines=1)
        counts = (lines | 'Transform' >> SumTransactionsPerDate())

        logging.log(logging.INFO, f'Selected output path: {known_args.output} and format: {known_args.output_format}.')

        if known_args.output_format == 'csv':
            counts \
            | 'FormatToCsv' >> beam.MapTuple(lambda k, v: '%s, %.2f' % (k, v)) \
            | 'Write' >> WriteToText(known_args.output, file_name_suffix='.csv.gz', header='date, total_amount')
        elif known_args.output_format == 'jsonl':
            counts \
            | 'FormatToJson' >> beam.MapTuple(lambda k, v: json.dumps({'date': k, 'total_amount': v})) \
            | 'Write' >> WriteToText(known_args.output, file_name_suffix='.jsonl.gz')
        else:
            logging.log(logging.ERROR, 'No valid output format selected.')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
