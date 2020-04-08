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


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

topic = app.topic("org.chicago.cta.connect.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.all_stations", value_type=TransformedStation)

# table = app.Table(
#    # "TODO",
#    # default=TODO,
#    partitions=1,
#    changelog_topic=out_topic,
# )


@app.agent(topic)
async def station_cleaner(stations):
    async for station in stations:
        print(asdict(station))
        if station.red:
            line = 'red'
        elif station.blue:
            line = 'blue'
        elif station.green:
            line = 'green'
        else:
            line = ''

        t_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line,
        )
        await out_topic.send(value=t_station)


if __name__ == "__main__":
    app.main()
