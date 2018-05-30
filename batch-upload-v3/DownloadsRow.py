import os
import json
import csv

MEDIA_ID_COLUMN_NAME = 'mediaId'
STATUS_COLUMN_NAME = 'status'
FILENAME_COLUMN_NAME = 'filename'

HEADER_ROW = [MEDIA_ID_COLUMN_NAME, STATUS_COLUMN_NAME, FILENAME_COLUMN_NAME ]

class DownloadsRow:
    def __init__(self, **kwargs):
        self.media_id = kwargs.get('media_id')
        self.status = kwargs.get('status')
        self.filename = kwargs.get('filename')

    @classmethod
    def write_to_csv_filepath(cls, downloads, csv_filepath, header = True):
        with open(csv_filepath, 'w') as csv_file:
            downloads_writer = csv.writer(
                csv_file, delimiter =  ',', quotechar = '"'
            )

            if header:
                downloads_writer.writerow(HEADER_ROW)

            for download in downloads:
                row = [ download.media_id, download.status, download.filename ]
                downloads_writer.writerow(row)
                yield download

    @classmethod
    def read_from_csv_filepath(cls, csv_filepath, headers = False):
        with open(csv_filepath, 'r') as csv_file:
            reader = csv.DictReader(csv_file, delimiter = ',')
            for row in reader:
                (media_id, status, filename) = row
                yield DownloadsRow(
                    media_id = row.get(MEDIA_ID_COLUMN_NAME),
                    status = row.get(STATUS_COLUMN_NAME),
                    filename = row.get(FILENAME_COLUMN_NAME)
                )

if __name__ == '__main__':
    def downloads_rows():
        rows = [
            [ '81f65182-7017-4036-8294-233e1d52f511', '81f65182-7017-4036-8294-233e1d52f511.json', 'finished' ],
            [ 'ade3faf6-7ee5-4830-8bf0-2cad5eb23b0d', 'ade3faf6-7ee5-4830-8bf0-2cad5eb23b0d.json', 'finished' ],
            [ 'd63864ea-5cda-4e55-bb14-3699eed4f7ae', 'd63864ea-5cda-4e55-bb14-3699eed4f7ae.json', 'rejected' ]
        ]

        for row in rows:
            (media_id, filename, status) = row
            yield DownloadsRow(media_id = media_id, filename = filename, status = status)


    results = DownloadsRow.write_to_csv_filepath(downloads_rows(), '/tmp/downloads.csv')

    readback = DownloadsRow.read_from_csv_filepath('/tmp/downloads.csv')

    for row in readback:
        print('readback row', row.media_id, row.status, row.filename)
