import argparse
import csv
import json
import os
import requests

from ResultsRow import ResultsRow
from DownloadsRow import DownloadsRow
from VoiceBaseV3Client import VoiceBaseV3Client

NULL_FILENAME = '/dev/null'

class CallbackTester:
    def __init__(self, **kwargs):
        self.input_csv = kwargs['input_csv']
        self.download_directory = kwargs['download_directory']
        self.output_csv = kwargs['output_csv']
        self.destination_url = kwargs['destination_url']

    def process(self):
        results = self.input()
        tests = self.test(results)
        printable_tests = self.output(tests)
        for test in printable_tests:
            print(
                'ROW media_id:', test.media_id,
                ' status: ', test.status,
                ' filename: ', test.filename
            )

    def input(self):
        return DownloadsRow.read_from_csv_filepath(self.input_csv)

    def test(self, results):
        for result in results:
            test = self.test_one(result)
            yield test

    def output(self, downloads):
        return DownloadsRow.write_to_csv_filepath(downloads, self.output_csv)

    def test_one(self, download):
        media_id = download.media_id
        status = download.status

        filename = media_id + '.json'
        filepath = self.download_directory + '/' + filename

        with open(filepath, 'r') as test_file:
            test_data = json.load(test_file)

        if status == 'finished':
            requests.post(self.destination_url, json = test_data)
        else:
            status = 'skipped'

        return DownloadsRow(
            media_id = media_id,
            status = status,
            filename = filename
        )

    @classmethod
    def _add_command_line_args(cls, parser):
        parser.add_argument(
            '--inputCsv',
            help = 'Input CSV ("downloads": id, mediaId, status)',
            required = True
        )
        parser.add_argument(
            '--downloadDirectory',
            help = 'Directory to place downloaded media',
            required = True
        )
        parser.add_argument(
            '--outputCsv',
            help = 'Input CSV ("callbacks": mediaId, status, filename)',
            required = True
        )
        parser.add_argument(
            '--destinationUrl',
            help = 'Destination URL of the callback',
            required = True
        )

    @classmethod
    def _initialize_tester(cls, args):
        print(args.inputCsv)
        print(args.downloadDirectory)
        print(args.outputCsv)
        print(args.destinationUrl)
        #raise Exception("don't feel like it")
        return CallbackTester(
            input_csv = args.inputCsv,
            download_directory = args.downloadDirectory,
            output_csv = args.outputCsv,
            destination_url = args.destinationUrl
        )

    @classmethod
    def main(cls):
        parser = argparse.ArgumentParser(
            description = "Callback tester for VoiceBase V3"
        )

        cls._add_command_line_args(parser)

        args = parser.parse_args()

        tester = cls._initialize_tester(args)

        tester.process()

if __name__ == '__main__':
    CallbackTester.main()
