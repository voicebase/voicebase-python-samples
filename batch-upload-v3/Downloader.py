import argparse
import csv
import json
import os

from ResultsRow import ResultsRow
from DownloadsRow import DownloadsRow
from VoiceBaseV3Client import VoiceBaseV3Client

NULL_FILENAME = '/dev/null'

class Downloader:
    def __init__(self, **kwargs):
        self.input_csv = kwargs['input_csv']
        self.download_directory = kwargs['download_directory']
        self.output_csv = kwargs['output_csv']
        self.voicebase = VoiceBaseV3Client(kwargs['token'])


    def process(self):
        results = self.input()
        downloads = self.download(results)
        printable_downloads = self.output(downloads)
        for download in printable_downloads:
            print(
                'ROW media_id:', download.media_id,
                ' status: ', download.status,
                ' filename: ', download.filename
            )

    def input(self):
        return ResultsRow.read_from_csv_filepath(self.input_csv)

    def download(self, results):
        for result in results:
            download = self.download_one(result)
            yield download

    def output(self, downloads):
        return DownloadsRow.write_to_csv_filepath(downloads, self.output_csv)

    def download_one(self, result):
        media_id = result.media_id
        filename = media_id + '.json'
        filepath = self.download_directory + '/' + filename
        media_entity = self.voicebase.media[media_id].get()

        status = media_entity['status']

        with open(filepath, 'w') as download_file:
            download_file.write(json.dumps(media_entity))

        return DownloadsRow(
            media_id = media_id,
            status = status,
            filename = filename
        )

    @classmethod
    def _add_command_line_args(cls, parser):
        parser.add_argument(
            '--inputCsv',
            help = 'Input CSV ("results": id, mediaId, status)',
            required = True
        )
        parser.add_argument(
            '--downloadDirectory',
            help = 'Directory to place downloaded media',
            required = True
        )
        parser.add_argument(
            '--outputCsv',
            help = 'Input CSV ("downloads": mediaId, status, filename)',
            required = True
        )
        parser.add_argument(
            '--token',
            help = 'Bearer token for /v3 API (defaults to $TOKEN)',
            default = os.environ.get('TOKEN'),
            required = False
        )

    @classmethod
    def _initialize_downloader(cls, args):
        return Downloader(
            input_csv = args.inputCsv,
            download_directory = args.downloadDirectory,
            output_csv = args.outputCsv,
            token = args.token
        )

    @classmethod
    def main(cls):
        parser = argparse.ArgumentParser(
            description = "Batch downloader for VoiceBase V3"
        )

        cls._add_command_line_args(parser)

        args = parser.parse_args()

        downloader = cls._initialize_downloader(args)

        downloader.process()

if __name__ == '__main__':
    Downloader.main()
