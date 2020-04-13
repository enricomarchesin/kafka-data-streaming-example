import json
from avro_validator.schema import Schema

schema = json.dumps({
  "namespace": "org.chicago.cta",
  "type": "record",
  "name": "arrival.value",
  "fields": [
    { "name": "station_id", "type": "int" },
    { "name": "train_id", "type": "string" },
    { "name": "direction", "type": "string" },
    { "name": "line", "type": ["null", "string"] },
    { "name": "train_status", "type": "string" },
    { "name": "prev_station_id", "type": ["null", "int"] },
    { "name": "prev_direction", "type": ["null", "string"] }
  ]
})

schema = Schema(schema)
parsed_schema = schema.parse()

data_to_validate = {"station_id": 40820, "train_id": "BL002", "direction": "b", "line": "blue", "train_status": "in_service", "prev_station_id": 40890, "prev_direction": "b"}

print("Valid:", parsed_schema.validate(data_to_validate))
