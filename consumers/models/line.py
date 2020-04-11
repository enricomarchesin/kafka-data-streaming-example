"""Contains functionality related to Lines"""
import json
import logging

from models import Station


logger = logging.getLogger(__name__)


class Line:
    """Defines the Line Model"""

    def __init__(self, color):
        """Creates a line"""
        self.color = color
        self.color_code = "0xFFFFFF"
        if self.color == "blue":
            self.color_code = "#1E90FF"
        elif self.color == "red":
            self.color_code = "#DC143C"
        elif self.color == "green":
            self.color_code = "#32CD32"
        self.stations = {}

    def _handle_station(self, value):
        """Adds the station to this Line's data model"""
        if value["line"] != self.color:
            return
        self.stations[value["station_id"]] = Station.from_message(value)
        logger.debug("station %d: %s", value["station_id"], self.stations[value["station_id"]])

    def _handle_arrival(self, message):
        """Updates train locations"""
        # logger.debug("arrival: %s", message)
        value = message.value()
        prev_station_id = value.get("prev_station_id")
        prev_dir = value.get("prev_direction")
        if prev_dir is not None and prev_station_id is not None:
            prev_station = self.stations.get(prev_station_id)
            if prev_station is not None:
                prev_station.handle_departure(prev_dir)
            else:
                logger.debug("unable to handle previous station due to missing station: %d", prev_station_id)
        else:
            logger.debug("unable to handle previous station due to missing previous info")

        station_id = value.get("station_id")
        station = self.stations.get(station_id)
        if station is None:
            logger.debug("unable to handle message due to missing station: %d", station_id)
            return
        station.handle_arrival(value.get("direction"), value.get("train_id"), value.get("train_status"))

    def process_message(self, message):
        """Given a kafka message, extract data"""
        logger.debug("message topic: %s", message.topic())
        if message.topic() == 'org.chicago.cta.stations.all.v5':
            try:
                value = json.loads(message.value())
                self._handle_station(value)
            except Exception as e:
                logger.fatal("bad station? %s, %s", value, e)
        elif message.topic().startswith("org.chicago.cta.station.arrivals."):
            self._handle_arrival(message)
        elif message.topic() == 'TURNSTILE_SUMMARY':
            logger.debug("turnstile message: %s", message.value())
            json_data = json.loads(message.value())
            station_id = json_data.get("STATION_ID")
            station = self.stations.get(station_id)
            if station is None:
                logger.debug("unable to handle message due to missing station: %d", station_id)
                return
            station.process_message(json_data)
        else:
            logger.debug("unable to find handler for message from topic %s", message.topic)
