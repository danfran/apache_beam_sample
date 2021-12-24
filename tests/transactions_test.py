import unittest
import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from pipelines.transactions import SumTransactionsPerDate


class SumTransactionsPerDateTest(unittest.TestCase):

    def test_empty(self):
        CSV = []

        with TestPipeline() as p:
            input = p | beam.Create(CSV)
            output = input | SumTransactionsPerDate()

            assert_that(
                output,
                equal_to([]))

    def test_sum_of_valid_transactions(self):
        CSV = [
            "2009-01-09 02:54:25 UTC,wallet123,wallet456,1.99",
            "1999-02-03 02:51:21 UTC,wallet123,wallet456,20.12",
            "2017-01-01 04:22:23 UTC,wallet123,wallet456,19.95",
            "2017-01-01 05:29:24 UTC,wallet123,wallet456,3.25",
            "2017-01-01 07:19:11 UTC,wallet123,wallet456,23.25",
            "2017-03-18 14:09:12 UTC,wallet123,wallet456,2102.05",
            "2017-03-18 15:19:16 UTC,wallet123,wallet456,10.22",
            "2017-03-18 16:02:13 UTC,wallet123,wallet456,1000",
            "2017-03-18 17:03:20 UTC,wallet123,wallet456,2000"
        ]

        with TestPipeline() as p:
            input = p | beam.Create(CSV)
            output = input | SumTransactionsPerDate()

            assert_that(
                output,
                equal_to([
                    ('2017-01-01', 23.25),
                    ('2017-03-18', 5102.05)
                ]))


if __name__ == '__main__':
    unittest.main()
