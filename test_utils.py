from unittest import TestCase
from pyspark.sql import SparkSession

from lib.utils import load_csv_df,get_country_count

class UtilsTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder.master("local[3]").appName("PTMAssignment").getOrCreate()

    def test_datafile_loading(self):
        sample_df = load_csv_df(self.spark,"data/sample.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count,9,"Record count should be 9")
