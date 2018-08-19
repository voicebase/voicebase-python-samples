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
            self.id = self.media_url
        elif self.is_file:
            self.media_filepath = kwargs.get('media_filepath')
            if not self.media_filepath:
                raise Exception('is_url is False but no valid media_filepath')

            self.media_filename = kwargs.get('media_filename')
            if not self.media_filename:
                raise Exception('is_url is False but no valid media_filepath')
            self.id = self.media_filename

            self.mime_type = kwargs.get('mime_type')
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

class BatchUploadNewMediaInput(BatchUploadInput):
    def __init__(self, **kwargs):
        configuration = kwargs.get('configuration')
        metadata = kwargs.get('metadata')

        media_filename = kwargs.get('media_filename')
        media_filepath = kwargs.get('media_filepath')
        media_url = kwargs.get('media_url')

        super_kwargs = {
            'configuration': configuration,
            'metadata': metadata
        }

        if not media_url:
            if not media_filename and media_filepath:
                raise Exception(
                    'Need both media_filename/media_filepath: ' + str(kwargs)
                )

            super_kwargs['is_file'] = True
            super_kwargs['media_filename'] = media_filename
            super_kwargs['media_filepath'] = media_filepath
        else:
            if media_filename or media_filepath:
                raise Exception(
                    'Cannot use media_url and media_filename/media_filepath: ' +
                    str(kwargs)
                )
            super_kwargs['is_url'] = True
            super_kwargs['media_url'] = media_url

        super().__init__(**super_kwargs)

class BatchUploadListReader:
    def __init__(self, **kwargs):
        self.media_directory = kwargs.get('media_directory', './')
        self.media_id_column = kwargs.get('media_id_column', 'mediaId')
        self.media_filename_column = kwargs.get(
            'media_filename_column',
            'media'
        )
        self.media_url_column = kwargs.get(
            'media_url_column',
            'mediaUrl'
        )
        self.default_metadata = kwargs.get('default_metadata', {})
        self.default_configuration = kwargs.get('default_configuration', {})
        self.custom_vocab_columns = kwargs.get('custom_vocab_columns', [])

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

    def CsvNewUploads(self, csv_filepath):
        for row in self._CsvReader(csv_filepath):

            metadata = self._extend_metadata(row)
            configuration = self._extend_configuration(row)

            input_kwargs = {
                'configuration': configuration,
                'metadata': metadata
            }

            if self.media_filename_column in row:
                media_filename = row[self.media_filename_column]
                del metadata['extended'][self.media_filename_column]

                input_kwargs['media_filename'] = media_filename

                media_filepath = os.path.join(
                    self.media_directory,
                    media_filename
                )

                input_kwargs['media_filepath'] = media_filepath

            elif self.media_url_column in row:
                media_url = row[self.media_url_column]
                del metadata['extended'][self.media_url_column]

                input_kwargs['media_url'] = media_url

            yield BatchUploadNewMediaInput(**input_kwargs)

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

    def _extend_configuration(self, row):
        custom_vocab_columns = self.custom_vocab_columns
        if custom_vocab_columns is None or len(custom_vocab_columns) == 0:
            return self.default_configuration

        if type(self.default_configuration) is str:
            configuration = json.loads(self.default_configuration)
        else:
            configuration = { ** self.default_configuration }

        if not ('vocabularies' in configuration):
            configuration['vocabularies'] = []

        vocabularies = configuration['vocabularies']

        flat_terms = [
            row.get(column)
            for column
            in self.custom_vocab_columns
        ]

        terms = [
            { 'term': term, 'weight': 2 }
            for term
            in flat_terms
            if term is not None
        ]

        additional_vocabulary = { 'terms': terms }
        vocabularies.append(additional_vocabulary)

        return configuration

if __name__ == '__main__':
    reader = BatchUploadListReader(media_directory = './')
    for x in reader.MediaFilenames('medialist'):
        print(x)
