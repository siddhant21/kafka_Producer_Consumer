
from confluent_kafka.admin import AdminClient, NewTopic

ac = AdminClient({'bootstrap.servers': 'localhost'})
#     create a topic
topic = NewTopic('trial2', num_partitions=1, replication_factor=1)

fs = ac.create_topics([topic])
fs.values()

print(fs)


for topic, f in fs.items():
    try:
        f.result()  # The result itself is None
        print("Topic {} created".format(topic))
    except Exception as e:
        print("Failed to create topic {}: {}".format(topic, e))