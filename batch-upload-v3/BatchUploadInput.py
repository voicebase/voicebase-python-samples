import os
import json

class BatchUploadInput:
    def __init__(self, **kwargs):
        self.is_url = kwargs.get('is_url', False)
        self.is_media_update = kwargs.get('is_media_update', False)

        if self.is_media_update:
            self.media_id = kwargs.get('media_id')
            if not self.media_id:
                raise Exception('is_media_update is True but no valid media_id')
        elif self.is_url:
            self.media_url = kwargs.get('media_url')
            if not self.media_url:
                raise Exception('is_url is True but no valid media_url')
        else:
            self.media_filepath = kwargs.get('media_filepath')
            if not self.media_filepath:
                raise Exception('is_url is False but no valid media_filepath')

            self.media_filename = kwargs.get('media_filename')
            if not self.media_filename:
                raise Exception('is_url is False but no valid media_filepath')

            self.mime_type = kwargs.get('mime_type')
            print('kwargs', kwargs)
            print('mime_type', self.mime_type)


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
            is_url = False,
            is_media_update = False,
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
            is_media_update = False,
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
            is_url = False,
            is_media_update = True,
            media_id = media_id,
            configuration = configuration,
            metadata = metadata
        )
