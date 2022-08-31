from typing import Union

from .backend import get_producer_backend
from .constants import FORMAT_JSON
from .format import render
from . import settings
import logging

logger = logging.getLogger(__name__)


class Producer(object):
    _client = None

    def __init__(self, topic_name, serializer_class):
        self.client = get_producer_backend()
        self.topic_name = topic_name
        self.serializer_class = serializer_class

    def send(self, instance, renderer=None, action_type='save'):
        # Instantiate the serialize

        # Get the message type and version
        message_type = self.serializer_class.MESSAGE_TYPE
        version = self.serializer_class.VERSION

        # Get the message's partition key
        key_field = getattr(self.serializer_class, "KEY_FIELD", None)
        key = None
        ser = None
        if action_type == 'save':
            ser = self.serializer_class(instance=instance)
            if key_field:
                key = str(ser.data[key_field])
        elif action_type in ['delete', 'class']:
            if not key_field:
                raise KeyError('Add "key_field" to serializer')
            key = str(instance[key_field])
        else:
            raise NotImplementedError('Please specify another action_type, use save/delete/class')
        # Render everything into a string
        renderer = settings.get("DEFAULT_FORMAT", FORMAT_JSON)
        body = {
            "type": message_type,
            "version": version,
            "message": ser.data if ser else instance,
            "action_type": action_type,
        }
        serialized_data = render(renderer, body)

        # Send the message data into the backend
        record_metadata = self.client.send(
            self.topic_name, key=key, value=serialized_data
        )
        logger.debug(
            'Sent message with type "%s", key "%s" to topic "%s"'
            % (message_type, key, self.topic_name)
        )
        return record_metadata
