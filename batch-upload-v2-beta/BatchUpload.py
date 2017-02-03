# ****** Batch Upload from csv list ******

# csv is a simple list of local file names with any directory info
# reqiured to locate the local file

# command line example
#  python BatchUpload.py --list up.txt --mediadir .\media --results .\res.csv --token  --priority low


import argparse
import csv
import json
import os
import requests

from VoiceBaseV2BetaClient import VoiceBaseV2BetaClient

# ********* def main ***********
def main():
  parser = argparse.ArgumentParser(
    description = "Batch uploader to VoiceBase V2 Beta"
  )
  parser.add_argument(
    '--list', 
    help = "path to csv list of input files (one per line)", 
    required = True
  )
  parser.add_argument(
    '--mediadir',
    help = "path to local media files",
    required = False,
    default = './'
  )
  parser.add_argument(
    '--results',
    help = "path to output csv file of files, media ids, and status",
    required = True
  )
  parser.add_argument(
    '--token',
    help = "Bearer token for V2 API authentication",
    required = True
  )


  args = parser.parse_args()

  upload(args.list, args.mediadir, args.results, args.token)

# ********* def upload  ***********
def upload(list_path, mdir, results_path, token):



  client = VoiceBaseV2BetaClient(token = token)
  media = client.media()

  counter = 0

  with open(list_path, 'r') as list_file:
    with open(results_path, 'wb') as results_file:
      results_writer = csv.writer(
        results_file, delimiter = ',', quotechar = '"'
      )

      results_writer.writerow([ 'file', 'mediaId', 'status' ]) # write headers

      for raw_filename in list_file:
        filename = raw_filename.rstrip()

        counter = counter + 1

        pathandfile = os.path.join(mdir, filename)

        response = upload_one(media, pathandfile, filename, generate_configuration())
        media_id = response['mediaId']
        status = response['status']

        results_writer.writerow([ filename, media_id, status ]);

# ********* def generate config json ***********
def generate_configuration():
  # Note: we are intentionally going against Python's recommending
  # single-quote style to have valid copy/paste-able JSON
  return json.dumps({
    "configuration": {
      "ingest" : {
        "channels": {
          "left": {
            "speaker": "caller"
          },
          "right": {
            "speaker": "agent"
          }
        }
      },
      "executor": "v2"
    }
  })
  # executor: v2 is not required for newer accounts

# ********* def upload one ***********
def upload_one(media, filepath, filename, configuration):
  with open(filepath, 'rb') as media_file:

    response = media.post(
      media_file, filename, 'audio/mpeg', configuration = configuration
    )

  return response

if __name__ == "__main__":
  main()