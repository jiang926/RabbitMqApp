from Tools.RabbitAndZmq import *
from datetime import datetime

if __name__ == '__main__':
    data = {
        "Messages": {
            "Url": "tcp://192.168.1.116:9300"
        }
    }
    rabbitmq_s = RabbitMQProducer('Recv_log', 'test')
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
    rabbitmq_s.send_message(data)
    rabbitmq_s.close_connection()