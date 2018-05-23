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
from BatchUploadInput import *

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
        '--configuration',
        help = 'JSON configuration for VoiceBase API',
        required = False,
        default = json.dumps({})
    )
    parser.add_argument(
        '--results',
        help = 'path to output csv file of files, media ids, and status',
        required = True
    )
    parser.add_argument(
        '--token',
        help = 'Bearer token for /v3 API (defaults to $TOKEN)',
        default = os.environ.get('TOKEN'),
        required = False
    )


    args = parser.parse_args()

    batch_upload = BatchUpload(
        token = args.token,
        media_directory = args.inputMediaDirectory,
        input_media_id_column = args.inputMediaIdColumn,
        default_configuration = args.configuration
    )

    batch_upload.process(
        input_media_filename_list = args.inputMediaFilenameList,
        input_csv = args.inputCsv,
        results_path = args.results,
        update_existing_media = args.updateExistingMedia
    )
    # batch_upload.upload(args.inputMediaFilenameList, args.mediadir, args.results)

class BatchUpload:
    def __init__(self, **kwargs):
        self.voicebase = VoiceBaseV3Client(token = kwargs['token'])

        media_directory = kwargs.get('media_directory')
        input_media_id_column = kwargs.get('input_media_id_column')
        default_configuration = kwargs.get('default_configuration', {})

        self.reader = BatchUploadListReader(
            media_directory = media_directory,
            media_id_column = input_media_id_column,
            default_configuration = default_configuration
        )

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

    def Uploads(self, input_iterable):
        Upload = namedtuple('Upload', 'id response')

        for input in input_iterable:
            response = self.upload_one(input)

            yield Upload(id = input.id, response = response)

    def Results(self, uploads, results_path):
        Result = namedtuple('Result', 'id response row')
        with open(results_path, 'w') as results_file:
            results_writer = csv.writer(
              results_file, delimiter = ',', quotechar = '"'
            )

            for upload in uploads:
                media_id = upload.response.get('mediaId')
                status = upload.response.get('status')

                row = [ upload.id, media_id, status ]
                results_writer.writerow(row)

                yield Result(
                    id = upload.id,
                    response = upload.response,
                    row = row
                )

    def process(self, **kwargs):
        input_media_filename_list = kwargs.get('input_media_filename_list')
        input_csv = kwargs.get('input_csv')
        is_media_update = kwargs.get('update_existing_media')
        input_media_id_column = kwargs.get('input_media_id_column')
        results_path = kwargs.get('results_path')



        if input_media_filename_list is not None:
            input_generator = self.reader.MediaFilenames(
                list_filepath = input_media_filename_list
            )
        elif input_csv is not None:
            if is_media_update:
                input_generator = self.reader.CsvMediaUpdates(
                    csv_filepath = input_csv
                )
            else:
                raise Exception('only media update csv input implemented')
        else:
            raise Exception('other input types not supported')

        uploads_generator = self.Uploads(input_generator)
        results_generator = self.Results(uploads_generator, results_path)

        for result in results_generator:
            print(result)


    # ********* def generate config json ***********
    def generate_configuration(self):
      # Note: we are intentionally going against Python's recommending
      # single-quote style to have valid copy/paste-able JSON
      return json.dumps({})

    # ********* def upload one ***********
    def upload_one(self, input): #filepath, filename, configuration):
        if input.is_url:
            raise Exception('not implemented')
        elif input.is_file:
            with open(input.media_filepath, 'rb') as media_file:

                response = self.voicebase.media.post(
                    media = media_file,
                    filename = input.media_filename,
                    mime_type = input.mime_type,
                    configuration = input.configuration
                )

        elif input.is_media_update:
            response = self.voicebase.media[input.media_id].post(
                configuration = input.configuration,
                metadata = input.metadata
            )
        else:
            raise Exception('no known type - none of: file, url, media update')

        return response


if __name__ == "__main__":
  main()
