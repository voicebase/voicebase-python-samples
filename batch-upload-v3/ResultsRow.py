import os
import json
import csv

STATUS_ACCEPTED = 'accepted'
STATUS_FINISHED = 'finished'
STATUS_REJECTED = 'rejected'
STATUS_FAILED = 'failed'
STATUS_SCHEDULED = 'scheduled'
STATUS_RUNNING = 'running'
STATUSES = [
    STATUS_ACCEPTED, STATUS_FINISHED, STATUS_REJECTED, STATUS_FAILED, STATUS_SCHEDULED, STATUS_RUNNING
]

class ResultsRow:
    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.media_id = kwargs.get('media_id')
        self.status = kwargs.get('status')
        self.status_http_code = kwargs.get('status_http_code')

    @classmethod
    def read_from_csv_filepath(cls, csv_filepath, headers = False):
        with open(csv_filepath, 'r') as csv_file:
            reader = csv.reader(csv_file, delimiter = ',')

            for row in reader:
                (id, media_id, blended_status) = row
                if blended_status in STATUSES:
                    status = blended_status
                    status_http_code = 200
                else:
                    status = STATUS_REJECTED
                    status_http_code = int(blended_status)

                yield ResultsRow(
                    id = id,
                    media_id = media_id,
                    status = status,
                    status_http_code = status_http_code
                )

if __name__ == '__main__':
    results = ResultsRow.read_from_csv_filepath('examples/results.csv')
    for result in results:
        print('result: ', result.id, result.media_id, result.status, result.status_http_code)
