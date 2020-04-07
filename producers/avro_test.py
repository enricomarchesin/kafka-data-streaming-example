import json
from avro_validator.schema import Schema

schema = json.dumps({
  "namespace": "org.chicago.cta",
  "type": "record",
  "name": "arrival.value",
  "fields": [
    { "name": "station_id", "type": "int" },
    { "name": "train_id", "type": "string" },
    { "name": "direction", "type": { "name": "arrival_direction", "type": "enum", "symbols" : ["a", "b" ]}},
    { "name": "line", "type": ["null", { "name": "arrival_line", "type": "enum", "symbols" : ["red", "blue", "green"] }] },
    { "name": "train_status", "type": { "name": "arrival_train_status", "type": "enum", "symbols" : ["out_of_service", "in_service", "broken_down"] } },
    { "name": "prev_station_id", "type": ["null", "int"] },
    { "name": "prev_direction", "type": ["null", { "name": "arrival_prev_direction", "type": "enum", "symbols" : ["a", "b" ]}] }
  ]
})

schema = Schema(schema)
parsed_schema = schema.parse()

data_to_validate = {"station_id": 40890, "train_id": "BL000", "train_status": "in_service", "direction": "b", "prev_station_id": None, "prev_direction": None}

print(parsed_schema.validate(data_to_validate))
