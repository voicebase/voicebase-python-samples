# ****** Batch Upload from csv list ******

# csv is a simple list of local file names with any directory info
# reqiured to locate the local file

# command line example
#  python BatchUpload.py --list up.txt --mediadir .\media --results .\res.csv --token  --priority low


import argparse
import csv
import json
import os
from collections import namedtuple

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
        '--inputMediaDirectory',
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

    batch_upload = BatchUpload(
        token = args.token,
        media_directory = args.inputMediaDirectory
    )
    batch_upload.upload_from_media_filename_list(
        args.inputMediaFilenameList,
        args.results
    )
    # batch_upload.upload(args.inputMediaFilenameList, args.mediadir, args.results)

class BatchUpload:
    def __init__(self, **kwargs):
        self.voicebase = VoiceBaseV3Client(token = kwargs['token'])
        self.media_directory = kwargs['media_directory']

    # Data classes woule be great here, when Python 3.7 is common
    class UploadItem:
        def __init__(self, **kwargs):
            self.media_url = kwargs.get('media_url')
            self.filename = kwargs.get('filename')
            self.mime_type = kwargs.get('mime_type')
            self.configuration = kwargs.get('configuration')
            self.configuration_filename = kwargs.get('configuration_filename')
            self.metadata = kwargs.get('metadata')
            self.metadata_filename = kwargs.get('metadata_filename')
            self.extra_metadata_columns = kwargs.get('extra_metadata_columns')


        def is_by_url(self):
            return self.media_url is not None

        def is_media_update(self):
            return self.media_id is not None

    def MediaFilenames(self, list_path):
        with open(list_path, 'r') as list_file:
            for raw_filename in list_file:
                filename = raw_filename.rstrip()
                yield filename

    def Uploads(self, media_filenames):
        Upload = namedtuple('Upload', 'filename response')

        for media_filename in media_filenames:
            media_filepath = os.path.join(self.media_directory, media_filename)
            response = self.upload_one(
                media_filepath,
                media_filename,
                self.generate_configuration()
            )

            yield Upload(filename = media_filename, response = response)

    def Results(self, uploads, results_path):
        Result = namedtuple('Result', 'filename response row')
        with open(results_path, 'w') as results_file:
            results_writer = csv.writer(
              results_file, delimiter = ',', quotechar = '"'
            )

            for upload in uploads:
                media_id = upload.response.get('mediaId')
                status = upload.response.get('status')

                row = [ upload.filename, media_id, status ]
                results_writer.writerow(row)

                yield Result(
                    filename = upload.filename,
                    response = upload.response,
                    row = row
                )

    # TODO: replace upload() with generator-based implementation here
    def upload_from_media_filename_list(self, list_path, results_path):
        media_filenames_generator = self.MediaFilenames(list_path)
        uploads_generator = self.Uploads(media_filenames_generator)
        results_generator = self.Results(uploads_generator, results_path)

        for result in results_generator:
            print(result)


    # ********* def generate config json ***********
    def generate_configuration(self):
      # Note: we are intentionally going against Python's recommending
      # single-quote style to have valid copy/paste-able JSON
      return json.dumps({})

    # ********* def upload one ***********
    def upload_one(self, filepath, filename, configuration):
        with open(filepath, 'rb') as media_file:

            response = self.voicebase.media.post(
                media = media_file,
                filename = filename,
                mime_type = 'application/octet-stream',
                configuration = configuration
            )

        return response

if __name__ == "__main__":
  main()
