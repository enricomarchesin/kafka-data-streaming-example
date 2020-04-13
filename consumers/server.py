"""Defines a Tornado Server that consumes Kafka Event data for display"""
import logging
import logging.config
from pathlib import Path

import tornado.ioloop
import tornado.template
import tornado.web


# Import logging before models to ensure configuration is picked up
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")


from consumer import KafkaConsumer
from models import Lines, Weather
from topic_check import topic_exists


logger = logging.getLogger(__name__)


class MainHandler(tornado.web.RequestHandler):
    """Defines a web request handler class"""

    template_dir = tornado.template.Loader(f"{Path(__file__).parents[0]}/templates")
    template = template_dir.load("status.html")

    def initialize(self, weather, lines):
        """Initializes the handler with required configuration"""
        self.weather = weather
        self.lines = lines

    def get(self):
        """Responds to get requests"""
        logging.debug("rendering and writing handler template")
        self.write(MainHandler.template.generate(weather=self.weather, lines=self.lines))


def run_server():
    """Runs the Tornado Server and begins Kafka consumption"""
    if not topic_exists("TURNSTILE_SUMMARY"):
        logger.fatal("Ensure that the KSQL Command has run successfully before running the web server!")
        exit(1)
    if not topic_exists("org.chicago.cta.stations.all.v5"):
        logger.fatal("Ensure that Faust Streaming is running successfully before running the web server!")
        exit(1)

    weather_model = Weather()
    lines = Lines()

    application = tornado.web.Application([(r"/", MainHandler, {"weather": weather_model, "lines": lines})])
    application.listen(8888)

    # Build kafka consumers
    consumers = [
        KafkaConsumer("org.chicago.cta.weather.v2", weather_model.process_message, is_avro=True),
        KafkaConsumer("org.chicago.cta.stations.all.v5", lines.process_message, is_avro=False),
        KafkaConsumer("^org.chicago.cta.station.arrivals.*", lines.process_message, is_avro=True),
        KafkaConsumer("TURNSTILE_SUMMARY", lines.process_message, is_avro=False),
    ]

    try:
        logger.info("Open a web browser to http://localhost:8888 to see the Transit Status Page")
        for consumer in consumers:
            tornado.ioloop.IOLoop.current().spawn_callback(consumer.consume)
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt as e:
        logger.info("shutting down server")
        tornado.ioloop.IOLoop.current().stop()
        for consumer in consumers:
            consumer.close()


if __name__ == "__main__":
    run_server()
