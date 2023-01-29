# from influxdb import InfluxDBClient
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

def connect(json_values):
    client = InfluxDBClient(url = "http://localhost:8086",username="raj", password="Raj@123sh", org="r")
    current_packet =list(json_values.keys())[0]
    print(json_values[current_packet], "added to iflux.")
    p = Point("praan").tag("timestamp", current_packet).field("device_id", json_values[current_packet]['device_id'])
    p = Point("praan").tag("timestamp", current_packet).field("windspeed", json_values[current_packet]['windspeed'])
    p = Point("praan").tag("timestamp", current_packet).field("wind_direction", json_values[current_packet]['wind_direction'])
    p = Point("praan").tag("timestamp", current_packet).field("pm1_particle", json_values[current_packet]['pm1_particle'])
    p = Point("praan").tag("timestamp", current_packet).field("pm25_particle", json_values[current_packet]['pm25_particle'])
    p = Point("praan").tag("timestamp", current_packet).field("pm10_particle", json_values[current_packet]['pm10_particle'])
    
    write_api = client.write_api(write_options=SYNCHRONOUS)
    query_api = client.query_api()
    bucket = "praan-bucket"

    write_api.write(bucket=bucket, record=p)