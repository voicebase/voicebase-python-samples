# ****** Batch Upload from csv list ******

# csv is a simple list of local file names with any directory info
# reqiured to locate the local file

# command line example
#  python BatchUpload.py --list up.txt --mediadir .\media --results .\res.csv --token  --priority low


import argparse
import csv
import json
import os

from VoiceBaseV3Client import VoiceBaseV3Client

# ********* def main ***********
def main():
    parser = argparse.ArgumentParser(
        description = "Batch uploader to VoiceBase V3"
    )
    parser.add_argument(
      '--inputCsv',
      help = 'path to csv list of input files (one per line)',
      required = False
    )
    parser.add_argument(
        '--inputMediaFilenameList',
        help = 'path to simple list of input files (one per line)',
        required = False
    )
    parser.add_argument(
        '--inputMediaUrlList',
        help = 'path to simple list of media URLs (one per line)',
        required = False
    )

    parser.add_argument(
        '--inputMediaFilenameColumn',
        help = 'name of the column with filenames (default: media)',
        default = 'media',
        required = False
    )
    parser.add_argument(
        '--inputMediaUrlColumn',
        help = 'name of the column with media URLs (default: mediaUrl)',
        default = 'media',
        required = False
    )
    parser.add_argument(
        '--updateExistingMedia',
        help = 'update existing media (uncommon)',
        action = 'store_true',
        required = False
    )
    parser.add_argument(
        '--inputMediaIdColumn',
        help = 'name of the column with mediaId (uncommon)',
        default = 'mediaId',
        required = False
    )

    parser.add_argument(
        '--mediadir',
        help = 'path to local media files',
        required = False,
        default = './'
    )
    parser.add_argument(
        '--results',
        help = 'path to output csv file of files, media ids, and status',
        required = True
    )
    parser.add_argument(
        '--token',
        help = 'Bearer token for /v3 API (defaults to $TOKEN)',
        default = os.environ['TOKEN'],
        required = False
    )


    args = parser.parse_args()

    BatchUpload().upload(
        args.inputMediaFilenameList, args.mediadir, args.results, args.token
    )

class BatchUpload:
    def __init(self):
        pass

    # ********* def upload  ***********
    def upload(self, list_path, mdir, results_path, token):

      client = VoiceBaseV3Client(token = token)
      media = client.media

      counter = 0

      with open(list_path, 'r') as list_file:
        with open(results_path, 'w') as results_file:
          results_writer = csv.writer(
            results_file, delimiter = ',', quotechar = '"'
          )

          results_writer.writerow([ 'file', 'mediaId', 'status' ]) # write headers

          for raw_filename in list_file:
            filename = raw_filename.rstrip()

            counter = counter + 1

            pathandfile = os.path.join(mdir, filename)

            response = self.upload_one(media, pathandfile, filename, self.generate_configuration())
            media_id = response['mediaId']
            status = response['status']

            results_writer.writerow([ filename, media_id, status ]);

    # ********* def generate config json ***********
    def generate_configuration(self):
      # Note: we are intentionally going against Python's recommending
      # single-quote style to have valid copy/paste-able JSON
      return json.dumps({
        # "configuration": {
        #   "ingest" : {
        #     "channels": {
        #       "left": {
        #         "speaker": "caller"
        #       },
        #       "right": {
        #         "speaker": "agent"
        #       }
        #     }
        #   },
        #   "executor": "v2"
        # }
      })
      # executor: v2 is not required for newer accounts

    # ********* def upload one ***********
    def upload_one(self, media, filepath, filename, configuration):
      with open(filepath, 'rb') as media_file:

        response = media.post(
            media = media_file,
            filename = 'unknownfile',
            mime_type = 'application/octet-stream',
            configuration = configuration
        )

      return response

if __name__ == "__main__":
  main()
