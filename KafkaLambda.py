from time import sleep
from json import dumps
from kafka import KafkaProducer

def publish_message(producer_instance, data):
    _topic_name='user-tracking'
    try:
        if producer_instance is not None:
            producer_instance.send(_topic_name, value=data)
            producer_instance.flush()
            print('Message publish successfully')
        else:
            print('Producer instance is None')
    except Exception as ex:
        print('Exception in publishing message - ' + str(ex))

def connect_kafka_producer():
    _producer = None
    _brokers = ['localhost:9092']
    try:
        _producer = KafkaProducer(bootstrap_servers=_brokers, value_serializer=lambda x:
            dumps(x).encode('utf-8'))
    except Exception as ex:
         print('Exception while connecting to Kafka - ' + str(ex))
    finally:
        return _producer

##AWS Lambda Handler##
def lambda_handler(event, context):
    print("lambda_handler event: ", event)
    publish_message(producer_instance=connect_kafka_producer(), data=event)

##Comment below this line when deploying in AWS##
if(__name__=="__main__"):

    #test payload
    json='''{
        "business":"Home",
        "address:"Sharondale"
    }'''
    lambda_handler(event=json, context=None)