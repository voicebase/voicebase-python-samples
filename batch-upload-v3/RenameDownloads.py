import argparse
import csv
import os
from collections import namedtuple

ResultsRow = namedtuple('ResultsRow', 'id media_id status')

def main():
    parser = argparse.ArgumentParser(
        description = "Batch downloader for VoiceBase V3"
    )
    parser.add_argument(
        '--inputCsv',
        help = 'Output CSV ("downloads": mediaId, status, filename)',
        required = True
    )
    parser.add_argument(
        '--downloadDirectory',
        help = 'Directory to place downloaded media',
        required = True
    )

    args = parser.parse_args()

    download_directory = args.downloadDirectory
    csv_filepath = args.inputCsv

    downloads_iterator = read_from_csv_filepath(csv_filepath)
    rename(downloads_iterator, download_directory)


def read_from_csv_filepath(csv_filepath):
    with open(csv_filepath, 'r') as csv_file:
        reader = csv.reader(csv_file, delimiter = ',')

        for row in reader:
            (id, media_id, status) = row

            yield ResultsRow(
                id = id,
                media_id = media_id,
                status = status
            )

def rename(downloads_iterator, download_directory):
    for download in downloads_iterator:
        old_filename = os.path.join(
            download_directory, download.media_id + '.json'
        )
        new_filename = os.path.join(
            download_directory, download.id + '.json'
        )

        print('Renaming '  + old_filename + ' to ' + new_filename)
        os.rename(old_filename, new_filename)

if __name__ == '__main__':
    main()
