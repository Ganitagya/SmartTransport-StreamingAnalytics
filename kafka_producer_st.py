import http.client
import sys
import gtfs_realtime_pb2
from kafka import KafkaProducer
from json import dumps
import ssl
import configurations
import tcm-config.yml as tcm

# for below use: pip install protobuf3-to-dict==0.1.5
# because of this issue: https://github.com/benhodgson/protobuf-to-dict/issues/15#issuecomment-368152202
from protobuf_to_dict import protobuf_to_dict

feed = gtfs_realtime_pb2.FeedMessage()

# conn = http.client.HTTPSConnection("api.transport.nsw.gov.au")
# payload = ''
# headers = {
#   'Authorization': 'apikey rLi10wYhQBoTJeQAkLzcSqi0dOFMY4CfgM3f',
#   'Cookie': 'AWSALB=qqoc5tE2zCdJIYSHwXXgIYhyj/rKRBWUnQ0e+q86rMPlM3+SvVRmZqmqRYt+CjynbKUSHJr1qouViE1Hu8v5IeSmPayRG4Q6Udb00cM100mVM60IUEe5nhwuROrz; AWSALBCORS=qqoc5tE2zCdJIYSHwXXgIYhyj/rKRBWUnQ0e+q86rMPlM3+SvVRmZqmqRYt+CjynbKUSHJr1qouViE1Hu8v5IeSmPayRG4Q6Udb00cM100mVM60IUEe5nhwuROrz'
# }
# conn.request("GET", "/v1/gtfs/realtime/buses", payload, headers)

conn = http.client.HTTPSConnection(configurations.http_conn_url)
conn.request("GET", "/v1/gtfs/realtime/buses", configurations.payload, configurations.headers )
res = conn.getresponse()


feed.ParseFromString(res.read())

local_producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

sasl_mechanism = 'PLAIN'
security_protocol = 'SASL_SSL'
# sasl_plain_username = '01FYR6YY0Y73AWGNQW9PW0DXKR/channel'
# sasl_plain_password = 'token:eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbnQiOlsicHViIiwic3ViIl0sImdzYmMiOiIwMUZZUjZZWTBZNzNBV0dOUVc5UFcwRFhLUiIsImlhdCI6MTY1MDI2ODQ1OCwicm9sZSI6IkFrYXNoQWRtaW5Sb2xlIiwic3ViIjoiYmFiZGZmM2JlNTI2NDE2Nzg0YTIxMDE4NjBmYzZiN2UiLCJzdmMiOlsiZWZ0bCIsImZ0bCIsImVtcyIsInB1bHNhciIsImthZmthIl19.cqlfEgOOhY6WOEj-EJsnXqp_yhqRqob-WHWOs_jH4d_yaYb7FLnsKfg1FkcvXTwnm2_J_X8vsQpekyTZqLCV9I8kPXMMB8AAj8VVjJrmcD74p_UcL160vXcPbyyvycC-s_4vNN8tOmrElUylGytfb81sZAVxZEmMDbXHVCvrio2kurTRt5gJ0hA7jptsPcz2ZBFQ6E8d1EbjNLzmjBmb0CHVYubeR8i-VKEm2Y_sASks663tK5MVg96cb1y5zuq_slP3nfOzHLZqCMgr10houVerd3G7ubyUOcm3BunehmiKPn5Dycf8zsWQ1LfauMHsNHrS5ykvTLuAraXvYq9QVQ'

sasl_plain_username = tcm.kafka_username
sasl_plain_password = tcm.kafka_password

# Create a new context using system defaults, disable all but TLS1.2
context = ssl.create_default_context()
context.options &= ssl.OP_NO_TLSv1
context.options &= ssl.OP_NO_TLSv1_1



tci_producer = KafkaProducer(value_serializer=lambda v: dumps(v).encode('utf-8'), bootstrap_servers = ['01fyr6yy0y73awgnqw9pw0dxkr-akd.messaging.cloud.tibco.com:10097'], sasl_plain_username = sasl_plain_username, sasl_plain_password = sasl_plain_password, security_protocol = security_protocol, ssl_context = context, sasl_mechanism = sasl_mechanism)

feed_dict = protobuf_to_dict(feed)
# breakpoint()

for entity in feed_dict['entity']:
  if 'trip_update' in entity.keys():
    # local_producer.send('quickstart-events', value=entity['trip_update'])
    # tci_producer.send('realtime-gtfs-streams', value = entity['trip_update'])
    try:
      # tci_producer.send('test', value = 'test'.encode('utf-8')) 
      tci_producer.send('realtime-gtfs-streams_akash', value = entity['trip_update']) 
    except:
      print("Unexpected error:", sys.exc_info()[0])
      raise

