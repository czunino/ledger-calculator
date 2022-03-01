#!/usr/bin/env python3
from cli import interface
from click.testing import CliRunner
import os
import unittest


def basename(test_file):
    """Generate basename from testfile path."""
    return test_file.split("/")[-1].split(".")[0]


TEST_INPUTS = [
    (
        "test1.csv",
        "2021-05-25",
        "test1.correct.2021-05-25.txt",
    ),
    (
        "test2.csv",
        "2021-07-08",
        "test2.correct.2021-07-08.txt",
    ),
    (
        "test2.csv",
        "2021-10-01",
        "test2.correct.2021-10-01.txt",
    ),
    (
        "test3.csv",
        "2021-06-25",
        "test3.correct.2021-06-25.txt",
    ),
    (
        "test3.csv",
        "2021-06-23",
        "test3.correct.2021-06-23.txt",
    ),
    (
        "test4.csv",
        "2021-07-19",
        "test4.correct.2021-07-19.txt",
    ),
    (
        "test4.csv",
        "2022-01-10",
        "test4.correct.2022-01-10.txt",
    ),
    (
        "test5.csv",
        "2021-11-27",
        "test5.correct.2021-11-27.txt",
    ),
    (
        "test5.csv",
        "2022-01-10",
        "test5.correct.2022-01-10.txt",
    ),
    (
        "test6.csv",
        "2022-01-11",
        "test6.correct.2022-01-11.txt",
    ),
    (
        "test6.csv",
        "2021-12-05",
        "test6.correct.2021-12-05.txt",
    ),
    (
        "test7.csv",
        "2021-10-01",
        "test7.correct.2021-10-01.txt",
    ),
    (
        "test7.csv",
        "2022-01-11",
        "test7.correct.2022-01-11.txt",
    ),
]


class CLITest(unittest.TestCase):
    """Ampla ledger CLI test cases."""

    def __init__(self, *args, **kwargs):
        """Initialize test cases."""
        super(CLITest, self).__init__(*args, **kwargs)
        self.maxDiff = None
        self.runner = CliRunner()
        self.test_dir = os.path.join(os.getcwd(), "tests")

    def setUp(self) -> None:
        """Override setUp of test cases."""
        print(f"\nTesting :: {self._testMethodName}")

    def test_create_db(self):
        """Test creation of DB."""
        with self.runner.isolated_filesystem(temp_dir="/tmp"):
            result = self.runner.invoke(interface, ["create-db"])
            self.assertEqual(0, result.exit_code)
            self.assertEqual(True, os.path.exists(os.path.join(os.getcwd(), "db.sqlite3")))

    def test_drop_db(self):
        """Test deletion of DB."""
        with self.runner.isolated_filesystem(temp_dir="/tmp"):
            self.runner.invoke(interface, ["create-db"])
            result = self.runner.invoke(interface, ["drop-db"])
            self.assertEqual(0, result.exit_code)
            self.assertEqual(False, os.path.exists(os.path.join(os.getcwd(), "db.sqlite3")))

    def test_load(self):
        """
        Test load command against test1 & test7 input files.
        """
        test_file_1 = os.path.join(self.test_dir, "test1.csv")
        test_file_7 = os.path.join(self.test_dir, "test7.csv")
        with self.runner.isolated_filesystem(temp_dir="/tmp"):
            result = self.runner.invoke(interface, ["load", test_file_1])
            self.assertEqual(0, result.exit_code)
            self.assertEqual(
                f"Database does not exist at {os.path.join(os.getcwd(), 'db.sqlite3')},"
                f" please create it using `create-db` command\n",
                result.output,
            )
            self.runner.invoke(interface, ["create-db"])
            result = self.runner.invoke(interface, ["load", test_file_1])
            self.assertEqual(0, result.exit_code)
            self.assertEqual(f"Loaded 3 events from {test_file_1}\n", result.output)
            self.runner.invoke(interface, ["drop-db"])
            self.runner.invoke(interface, ["create-db"])
            result = self.runner.invoke(interface, ["load", test_file_7])
            self.assertEqual(0, result.exit_code)
            self.assertEqual(f"Loaded 500 events from {test_file_7}\n", result.output)

    def test_results(self):
        """Test `balances` results against suite of correct output."""
        for test_filename, output_date, output in TEST_INPUTS:
            msg = f"Testing :: {test_filename} results of {output_date}"
            print(msg)
            with self.runner.isolated_filesystem(temp_dir="/tmp"), self.subTest(
                msg=msg, test_filename=test_filename, output_date=output_date, output=output
            ):
                test_file_location = os.path.join(self.test_dir, test_filename)
                self.runner.invoke(interface, ["create-db"])
                self.runner.invoke(interface, ["load", test_file_location])
                result = self.runner.invoke(interface, ["balances", output_date])
                self.assertEqual(0, result.exit_code)
                output_path = f"{os.path.join(self.test_dir, output)}"
                with open(output_path, "r") as correct_f:
                    self.assertEqual(correct_f.read(), result.output)


if __name__ == "__main__":
    unittest.main()
