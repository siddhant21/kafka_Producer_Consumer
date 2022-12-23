from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost'})

send_data = "This is the data number => "

send_arr = [1, 2, 3, 4, 5]


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


for data in send_arr:
    send_msg = send_data + str(data + 100)

    # send data
    p.produce('trial2', send_msg.encode('utf-8'), callback=delivery_report)
# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()
