from confluent_kafka import Consumer

'''Create a consumer with group id and offset'''

c = Consumer({
    'bootstrap.servers':'localhost',
    'group.id':'G1',
    'auto.offset.reset':'earliest'
})

'''Subscribe to a topic'''
c.subscribe(['trial2'])

while True:
    msg = c.poll()

    if msg is None:
        print("No message...!!!")
        continue
    if msg.error():
        print("consumer error: {}".format(msg.error()))
        continue
    print("Received message: {}".format(msg.value().decode('utf-8')))

