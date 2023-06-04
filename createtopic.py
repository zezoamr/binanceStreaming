from confluent_kafka.admin import AdminClient, NewTopic
from configparser import ConfigParser
from argparse import ArgumentParser, FileType

class createTopic():
    
    configloc = r'D:\projects\binanceStreaming\config.ini'
    
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    
    
    
    def __init__(self):
        self.kadmin = AdminClient(self.config)
        self.topics= ['binance']
        
        new_topics = [NewTopic(topic, num_partitions=6, replication_factor=1) for topic in self.topics]
        # Call create_topics to asynchronously create topics. A dict
        # of <topic,future> is returned.
        fs = self.kadmin.create_topics(new_topics)

        # Wait for each operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} created".format(topic))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))
                
#if __name__ == '__main__':        
    #m = createTopic()