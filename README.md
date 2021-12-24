# Apache Beam Sample
Sample usage of Apache Beam to process a CSV file.

## How to run

To run the pipeline:
```
python3 pipelines/transaction.py <options>
```

Where the `options` available are:
* `--input` The input file path to be loaded. Default: `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`.
* `--output` The required output file path prefix to be used, e.g. `output/results`.
* `--output-format` The output file format to be used to create the output. Available `csv` and `jsonl`. Default: `jsonl`.

Example:
```
cd apache_beam_sample
python3 pipelines/transactions.py --output output/results --output-format csv
```

will generate the file `output/results-00000-of-00001.csv.gz`.

To run the tests:
```
python3 -m unittest tests/transactions_test.py
```

## Build
The project has been built using `venv` with dependencies specified in the file `requirements.txt`:

```
python3 -m venv apache_beam_sample/
source apache_beam_sample/bin/activate
pip3 install -r apache_beam_sample/requirements.txt
```