import json

from streaming_data_types import (
    deserialise_6s4t,
    deserialise_ADAr,
    deserialise_answ,
    deserialise_ev42,
    deserialise_f142,
    deserialise_hs00,
    deserialise_pl72,
    deserialise_wrdn,
    deserialise_x5f2,
)


class StatusMessage:
    def deserialize(self, message):
        return deserialise_x5f2(message)


class FwResponseMessage:
    def deserialize(self, message):
        return deserialise_answ(message)


class FwFinishedMessage:
    def deserialize(self, message):
        return deserialise_wrdn(message)


class RunStopMessage:
    def deserialize(self, message):
        return deserialise_6s4t(message)


class RunStartMessage:
    def deserialize(self, message):
        return deserialise_pl72(message)


class LogMessage:
    def deserialize(self, message):
        return deserialise_f142(message)


class EventMessage:
    def deserialize(self, message):
        return deserialise_ev42(message)


class HistogramMessage:
    def deserialize(self, message):
        return deserialise_hs00(message)


class JsonMessage:
    def deserialize(self, message):
        return json.loads(message)


class AreaDetectorMessage:
    def deserialize(self, message):
        return deserialise_ADAr(message)


class DeserializerFactory:
    _message_handler = {
        b"x5f2": StatusMessage,
        b"answ": FwResponseMessage,
        b"wrdn": FwFinishedMessage,
        b"6s4t": RunStopMessage,
        b"pl72": RunStartMessage,
        b"f142": LogMessage,
        b"ev42": EventMessage,
        b"hs00": HistogramMessage,
        b"ADAr": AreaDetectorMessage,
        b"json": JsonMessage,
    }

    @classmethod
    def from_serialized_type(cls, type):
        if type in cls._message_handler:
            return cls._message_handler[type]()

        return cls._message_handler[b"json"]()
