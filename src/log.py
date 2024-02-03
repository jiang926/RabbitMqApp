import pika
import json
from datetime import datetime


class RabbitMQLogConsumer:
    def __init__(self, vhost, log_queue):
        self.vhost = vhost
        self.log_queue = log_queue
        self.host = '192.168.1.180'
        self.port = 5672  # 注意这里将prot变量名改为port，这是一个常见的命名习惯
        credentials = pika.PlainCredentials(username="mq_admin", password='dw123#@!')
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.vhost,
            credentials=credentials
        )
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.log_queue)

    def callback(self, ch, method, properties, body):
        try:
            message = body.decode('utf-8')
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]: Received message - {message}")
        except Exception as e:
            print(f"Error processing message: {e}")

    def start_consuming(self):
        self.channel.basic_consume(queue=self.log_queue,
                                   on_message_callback=self.callback,
                                   auto_ack=True)
        print('Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    def close_connection(self):
        self.channel.close()
        self.connection.close()


if __name__ == '__main__':
    resprocelog = RabbitMQLogConsumer('Recv_log', 'log')
    resprocelog.start_consuming()
    resprocelog.close_connection()