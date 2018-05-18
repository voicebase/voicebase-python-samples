import requests
import json

class VoiceBaseV3Client:
    """Client for the VoiceBase V3 API"""

    def __init__(self, token):
        self.url = 'https://apis.voicebase.com/v3'
        self.default_headers = {
            'Authorization' : 'Bearer ' + token
        }
        self.media = VoiceBaseV3Client.VoiceBaseMedia(self)

    def get(self, relative_url, **kwargs):
        url = self._url(relative_url)

        response = requests.get(url, **self._prepare_kwargs(kwargs))

        return json.loads(response.text)

    def post(self, relative_url, **kwargs):
        url = self._url(relative_url)

        response = requests.post(url, **self._prepare_kwargs(kwargs))

        return json.loads(response.text)

    def delete(self, relative_url, **kwargs):
        url = self._url(relative_url)

        response = requests.delete(url, **self._prepare_kwargs(kwargs))

        return json.loads(response.text)


    class VoiceBaseMedia:
        """Media class for the VoiceBase V3 API Client"""
        def __init__(self, client):
            self.client = client

        def get(self):
            """Get all media list"""
            return self.client.get('/media')

        def __getitem__(self, media_id):
            """Get a media item"""

            return VoiceBaseV3Client.VoiceBaseMediaItem(self.client, media_id)

        def post(self, **kwargs):

            def _get_required_kwarg(kwarg_name):
                return self.client._get_required_kwarg(
                    kwargs,
                    kwarg_name,
                    kwarg_name + ' required in post()'
                )

            self.client._require_exactly_one_of(
                kwargs,
                'exactly one of media_url, media required in post()',
                'media_url',
                'media'
            )

            media_url = self.client._get_optional_kwarg(kwargs, 'media_url')

            if media_url is not None:
                attachments = {
                    'mediaUrl': (
                        'url', media_url, 'text/url'
                    )
                }

            else:
                media = _get_required_kwarg('media')
                filename = _get_required_kwarg('filename')
                mime_type = _get_required_kwarg('mime_type')
                attachments = {
                    'media': (
                        filename, media, mime_type
                    )
                }

            configuration = self.client._get_optional_kwarg(kwargs, 'configuration')
            if configuration is not None:
                attachments['configuration'] = (
                    'configuration.json', configuration, 'application/json'
                )

            metadata = self.client._get_optional_kwarg(kwargs, 'metadata')
            if metadata is not None:
                attachments['metadata'] = (
                    'metadata.json', metadata, 'application/json'
                )

            return self.client.post('/media', files = attachments)

    class VoiceBaseMediaItem:
        """Media item class for the VoiceBase V3 API Client"""
        def __init__(self, client, media_id):
            self.client = client
            self.media_id = media_id

        def get(self):
            """Get a media item"""

            return self.client.get('/media/' + self.media_id)

        def delete(self, mediaId):
            """Get a media item"""

            return self.client.delete('/media/' + self.media_id)

        def post(self, **kwargs):
            attachments = {}
            configuration = self.client._get_optional_kwarg(kwargs, 'configuration')
            if configuration is not None:
                attachments['configuration'] = (
                    'configuration.json', configuration, 'application/json'
                )

            metadata = self.client._get_optional_kwarg(kwargs, 'metadata')
            if metadata is not None:
                attachments['metadata'] = (
                    'metadata.json', metadata, 'application/json'
                )

            return self.client.post('/media/' + self.media_id, files = attachments)

    class MalformedRequestException(Exception):
        pass

    def _get_required_kwarg(self, kwargs_dict, kwarg_name, message_on_error):
        if kwarg_name not in kwargs_dict:
            raise VoiceBaseV3Client.MalformedRequestException(message_on_error)
        return kwargs_dict[kwarg_name]

    def _get_optional_kwarg(self, kwargs_dict, kwarg_name):
        return kwargs_dict[kwarg_name] if kwarg_name in kwargs_dict else None

    def _require_exactly_one_of(self, kwargs_dict, message_on_error, *names):
        found_kwargs = [ True for name in names if name in kwargs_dict ]
        if len(found_kwargs) != 1:
            raise VoiceBaseV3Client.MalformedRequestException(message_on_error)

    def _prepare_kwargs(self, kwargs_dict):
        request_kwargs = self._fill_headers({ **kwargs_dict })
        return request_kwargs

    def _fill_headers(self, kwargs_dict):
        if 'headers' in kwargs_dict:
            kwargs_dict['headers'] = {
                **self.default_headers,
                **kwargs_dict['headers']
            }
        else:
            kwargs_dict['headers'] = self.default_headers

        return kwargs_dict

    def _url(self, relative):
        return self.url + relative



if __name__ == '__main__':
    import os
    import json
    voicebase = VoiceBaseV3Client(os.environ['TOKEN'])

    with open('mpthreetest.mp3', 'rb') as mpthree:
        metadata = json.dumps({"extended":{"this":"is metadata"}})
        configuration = json.dumps({})
        print(voicebase.media.post(
            media = mpthree,
            filename = 'mpthreetest.mp3',
            mime_type = 'audio/mpeg',
            metadata = metadata,
            configuration = configuration
        ))
