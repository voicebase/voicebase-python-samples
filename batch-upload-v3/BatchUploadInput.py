import os
import json
import csv

class BatchUploadInput:
    def __init__(self, **kwargs):
        self.is_url = kwargs.get('is_url', False)
        self.is_file = kwargs.get('is_file', False)
        self.is_media_update = kwargs.get('is_media_update', False)

        if self.is_media_update:
            self.media_id = kwargs.get('media_id')
            if not self.media_id:
                raise Exception('is_media_update is True but no valid media_id')
            self.id = self.media_id
        elif self.is_url:
            self.media_url = kwargs.get('media_url')
            if not self.media_url:
                raise Exception('is_url is True but no valid media_url')
            self.id = media_url
        elif self.is_file:
            self.media_filepath = kwargs.get('media_filepath')
            if not self.media_filepath:
                raise Exception('is_url is False but no valid media_filepath')

            self.media_filename = kwargs.get('media_filename')
            if not self.media_filename:
                raise Exception('is_url is False but no valid media_filepath')
            self.id = self.media_filename

            self.mime_type = kwargs.get('mime_type')
            print('kwargs', kwargs)
            print('mime_type', self.mime_type)
        else:
            raise Exception('no known type - none of: file, url, media update')

        self.configuration = kwargs.get('configuration')

        # Convert to JSON string if provided as an object
        if self.configuration is not None:
            if type(self.configuration) is not str:
                self.configuration = json.dumps(self.configuration)

        # Convert to JSON string if provided as an object
        self.metadata = kwargs.get('metadata')
        if self.metadata is not None:
            if type(self.metadata) is not str:
                self.metadata = json.dumps(self.metadata)

class BatchUploadFilenameInput(BatchUploadInput):
    def __init__(self, **kwargs):
        media_filepath = kwargs.get('media_filepath')
        media_filename = kwargs.get(
            'media_filename',
            os.path.basename(media_filepath)
        )
        mime_type = kwargs.get('mime_type', 'application/octet-stream')
        configuration = kwargs.get('configuration')
        metadata = kwargs.get('metadata')

        super().__init__(
            is_file = True,
            media_filepath = media_filepath,
            media_filename = media_filename,
            mime_type = mime_type,
            configuration = configuration,
            metadata = metadata
        )

class BatchUploadUrlInput(BatchUploadInput):
    def __init__(self, **kwargs):
        media_url = kwargs.get('media_url')
        configuration = kwargs.get('configuration')
        metadata = kwargs.get('metadata')
        super().__init__(
            is_url = True,
            media_url = media_url,
            configuration = configuration,
            metadata = metadata
        )

class BatchUploadMediaUpdateInput(BatchUploadInput):
    def __init__(self, **kwargs):
        media_id = kwargs.get('media_id')
        configuration = kwargs.get('configuration')
        metadata = kwargs.get('metadata')
        super().__init__(
            is_media_update = True,
            media_id = media_id,
            configuration = configuration,
            metadata = metadata
        )

class BatchUploadListReader:
    def __init__(self, **kwargs):
        self.media_directory = kwargs.get('media_directory', './')
        self.media_id_column = kwargs.get('media_id_column', 'mediaId')
        self.default_metadata = kwargs.get('default_metadata', {})
        self.default_configuration = kwargs.get('default_configuration', {})

    def MediaFilenames(self, list_filepath):
        with open(list_filepath, 'r') as list_file:
            for raw_filename in list_file:
                media_filename = raw_filename.rstrip()
                media_filepath = os.path.join(
                    self.media_directory,
                    media_filename
                )
                yield BatchUploadFilenameInput(
                    media_filepath = media_filepath,
                    media_filename = media_filename
                )

    def CsvMediaUpdates(self, csv_filepath):
        for row in self._CsvReader(csv_filepath):
            media_id = row[self.media_id_column]
            del row[self.media_id_column]

            metadata = self._extend_metadata(row)
            yield BatchUploadMediaUpdateInput(
                media_id = media_id,
                configuration = self.default_configuration,
                metadata = metadata
            )

    def _CsvReader(self, csv_filepath):
        with open(csv_filepath, 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                yield row

    def _extend_metadata(self, row):
        metadata = { **self.default_metadata }
        if len(row) > 0:
            extended_metadata = metadata.get('extended', {})
            extended_metadata = {
                **extended_metadata,
                **row
            }
            metadata['extended'] = extended_metadata
        return metadata


if __name__ == '__main__':
    reader = BatchUploadListReader(media_directory = './')
    for x in reader.MediaFilenames('medialist'):
        print(x)
