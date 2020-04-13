"""Defines trends calculations for stations"""
import logging
from dataclasses import asdict

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream-4", broker="kafka://localhost:9092", store="memory://", topic_partitions=1)

topic = app.topic("org.chicago.cta.connect.v5.stations", value_type=Station, key_type=None)
out_topic = app.topic("org.chicago.cta.stations.all.v5", value_type=TransformedStation, internal=True)

# table = app.Table(
#    "org.chicago.cta.stations.table",
#    default=int,
#    changelog_topic=out_topic,
# ).tumbling(10.0, expires=10.0)


@app.agent(topic)
async def station_cleaner(stations):
    async for station in stations:
        if station.red:
            line = 'red'
        elif station.blue:
            line = 'blue'
        elif station.green:
            line = 'green'
        else:
            line = None

        t_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line,
        )
        logger.debug("t_station: %s", t_station)
        # table[station.station_id] = t_station
        await out_topic.send(value=t_station)


if __name__ == "__main__":
    app.main()
