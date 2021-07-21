from unittest import TestCase
from pyspark.sql import SparkSession

from lib.utils import load_csv_df,get_country_count

class UtilsTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder.master("local[3]").appName("PaytmAssignment").getOrCreate()

    def test_datafile_loading(self):
        sample_df = load_csv_df(self.spark,"data/sample.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count,9,"Record count should be 9")
    def test_country_count(self):
        sample_df = load_csv_df(self.spark,"data/sample.csv")
        count_list = get_country_count(sample_df).collect()

        count_dict = dict()
        for row in count_list:
            count_dict[row["Country"]] = row["count"]
        self.assertEqual(count_dict["United States"],4,"Count for Us is 4")
        self.assertEqual(count_dict["Canada"],2,"Count for canada is 2")
        self.assertEqual(count_dict["United Kingdom"],1,"Count for UK is 1")