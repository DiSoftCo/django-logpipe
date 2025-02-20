from django.db import transaction
from rest_framework.exceptions import ValidationError
from rest_framework.serializers import Serializer
from .exceptions import (
    InvalidMessageError,
    IgnoredMessageTypeError,
    UnknownMessageTypeError,
    UnknownMessageVersionError,
)
from .backend import get_offset_backend, get_consumer_backend, get_producer_backend
from .format import parse, render, _delim
from . import settings, FORMAT_JSON
import itertools
import logging
import time

logger = logging.getLogger(__name__)


def consumer_error_handler(inner):

    while True:
        # Try to get the next message
        try:
            yield next(inner)

        # Obey the laws of StopIteration
        except StopIteration:
            return

        # Message format was invalid in some way: log error and move on.
        except InvalidMessageError as e:
            logger.error(
                "Failed to deserialize message in topic {}. Details: {}".format(
                    inner.consumer.topic_name, e
                )
            )
            inner.commit(e.message)

        # Message type has been explicitly ignored: skip it silently and move on.
        except IgnoredMessageTypeError as e:
            logger.debug(
                "Skipping ignored message type in topic {}. Details: {}".format(
                    inner.consumer.topic_name, e
                )
            )
            inner.commit(e.message)

        # Message type is unknown: log error and move on.
        except UnknownMessageTypeError as e:
            logger.error(
                "Skipping unknown message type in topic {}. Details: {}".format(
                    inner.consumer.topic_name, e
                )
            )
            inner.commit(e.message)

        # Message version is unknown: log error and move on.
        except UnknownMessageVersionError as e:
            logger.error(
                "Skipping unknown message version in topic {}. Details: {}".format(
                    inner.consumer.topic_name, e
                )
            )
            inner.commit(e.message)

        # Serializer for message type flagged message as invalid: log warning and move on.
        except ValidationError as e:
            logger.warning(
                "Skipping invalid message in topic {}. Details: {}".format(
                    inner.consumer.topic_name, e
                )
            )
            inner.commit(e.message)
        pass


class Consumer(object):
    _client = None

    def __init__(self, topic_name, throw_errors=False, **kwargs):
        self.consumer = get_consumer_backend(topic_name, **kwargs)
        self.throw_errors = throw_errors
        self.serializer_classes = {}
        self.custom_classes = {}
        self.ignored_message_types = set([])
        self.error_topic = settings.get('ERROR_TOPIC', None)
        self.producer_client = None
        if self.error_topic:
            self.producer_client = get_producer_backend()

    def add_ignored_message_type(self, message_type):
        self.ignored_message_types.add(message_type)

    def commit(self, message):
        get_offset_backend().commit(self.consumer, message)

    def register(self, serializer_class):
        message_type = serializer_class.MESSAGE_TYPE
        version = serializer_class.VERSION
        if message_type not in self.serializer_classes:
            self.serializer_classes[message_type] = {}
        self.serializer_classes[message_type][version] = serializer_class

    def send_message_to_error_topic(self, message, error):
        data = parse(message.value)
        data['error'] = error
        data['can_retry'] = True
        code, _ = message.value.split(_delim, 1)
        message_value = render(code, data)
        self.producer_client.send(
            self.error_topic, key=message.key, value=message_value
        )
        self.commit(message)

    def run(self, iter_limit=0):
        i = 0
        for message, serializer in self:
            with transaction.atomic():
                try:
                    action_type = getattr(serializer, '_action_type', 'save')
                    if action_type == 'delete':
                        serializer.delete() if hasattr(serializer, 'delete') else serializer.instance.delete()
                    elif action_type == 'save':
                        serializer.save()
                    elif action_type == 'class':
                        serializer.receive()
                    self.commit(message)
                except Exception as e:
                    info = (
                        message.key,
                        message.topic,
                        message.partition,
                        message.offset,
                    )
                    logger.exception(
                        'Failed to process message with key "%s" from topic "%s", partition "%s", offset "%s"'
                        % info
                    )
                    if self.error_topic:
                        self.send_message_to_error_topic(message, str(e))
                    else:
                        raise e
            i += 1
            if iter_limit > 0 and i >= iter_limit:
                break

    def __iter__(self):
        if self.throw_errors:
            return self
        return consumer_error_handler(self)

    def __next__(self):
        return self._get_next_message()

    def __str__(self):
        return '<logpipe.consumer.Consumer topic="%s">' % self.consumer.topic_name

    def _get_next_message(self):
        message = next(self.consumer)

        info = (message.key, message.topic, message.partition, message.offset)
        logger.debug(
            'Received message with key "%s" from topic "%s", partition "%s", offset "%s"'
            % info
        )

        # Wait?
        timestamp = getattr(message, "timestamp", None) or (time.time() * 1000)
        lag_ms = (time.time() * 1000) - timestamp
        logger.debug("Message lag is %sms" % lag_ms)
        wait_ms = settings.get("MIN_MESSAGE_LAG_MS", 0) - lag_ms
        if wait_ms > 0:
            logger.debug("Respecting MIN_MESSAGE_LAG_MS by waiting %sms" % wait_ms)
            time.sleep(wait_ms / 1000)
            logger.debug("Finished waiting")

        try:
            serializer = self._unserialize(message)
        except Exception as e:
            e.message = message
            raise e

        return message, serializer

    def _unserialize(self, message):
        data = parse(message.value)
        if "type" not in data:
            raise InvalidMessageError(
                'Received message missing missing a top-level "type" key.'
            )
        if "version" not in data:
            raise InvalidMessageError(
                'Received message missing missing a top-level "version" key.'
            )
        if "message" not in data:
            raise InvalidMessageError(
                'Received message missing missing a top-level "message" key.'
            )

        message_type = data["type"]
        action_type = data.get('action_type', 'save')
        if message_type in self.ignored_message_types:
            raise IgnoredMessageTypeError(
                'Received message with ignored type "%s" in topic %s'
                % (message_type, message.topic)
            )
        if message_type not in self.serializer_classes:
            raise UnknownMessageTypeError(
                'Received message with unknown type "%s" in topic %s'
                % (message_type, message.topic)
            )

        version = data["version"]
        if version not in self.serializer_classes[message_type]:
            raise UnknownMessageVersionError(
                'Received message of type "%s" with unknown version "%s" in topic %s'
                % (message_type, version, message.topic)
            )

        serializer_class = self.serializer_classes[message_type][version]
        is_serializer = issubclass(serializer_class, Serializer)
        instance = None
        if hasattr(serializer_class, "lookup_instance") and is_serializer:
            instance = serializer_class.lookup_instance(**data["message"])
        if action_type == 'save':
            serializer = serializer_class(instance=instance, data=data["message"])
            serializer.is_valid(raise_exception=True)
        elif action_type == 'delete':
            serializer = serializer_class(instance=instance)
        elif action_type == 'class':
            serializer = serializer_class(data=data["message"])
        else:
            raise NotImplementedError("Can't use this action type")
        serializer._action_type = action_type
        return serializer


class MultiConsumer(object):
    def __init__(self, *consumers):
        self.consumers = consumers

    def run(self):
        for consumer in itertools.cycle(self.consumers):
            consumer.run(iter_limit=1)
