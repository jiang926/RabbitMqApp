import pika
import zmq
import json
from datetime import datetime


# RabbitMQ生产者类
class RabbitMQProducer:
    def __init__(self, vhost, queue_name):
        # 初始化连接和队列
        self.host = '192.168.1.180'
        self.prot = 5672
        self.vhost = vhost
        self.queue_name = queue_name
        # 建立到RabbitMQ服务器的连接
        # self.connection = pika.BlockingConnection(pika.ConnectionParameters(virtual_host=self.vhost))
        # 建立到RabbitMQ服务器的连接，指定用户名和密码
        credentials = pika.PlainCredentials(username='mq_admin', password='dw123#@!')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=self.host,
            port=self.prot,
            virtual_host=self.vhost,
            credentials=credentials
        ))
        # 创建一个通道
        self.channel = self.connection.channel()
        # 确保队列存在
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def send_message(self, message):
        message_json = json.dumps(message)
        # 发送消息到队列中
        self.channel.basic_publish(exchange='',
                                   routing_key=self.queue_name,
                                   body=message_json)
        print(f"Sent: {message_json}")

    def close_connection(self):
        # 关闭连接
        self.connection.close()


# ZeroMQ通信器类
class ZeroMQCommunicator:
    def __init__(self, zmq_conn_str):
        # 初始化ZeroMQ连接
        self.zmq_conn_str = zmq_conn_str
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(self.zmq_conn_str)

    def send_message(self, message):
        # 发送消息到ZeroMQ服务器并等待回复
        self.socket.send_json(message)
        return self.socket.recv_json()

    def close(self):
        # 关闭ZeroMQ socket
        self.socket.close()
        self.context.term()


# RabbitMQ消费者类
class RabbitMQConsumer:
    def __init__(self, vhost, input_queue, log_queue):
        # 初始化连接和队列
        self.vhost = vhost
        self.input_queue = input_queue
        self.log_queue = log_queue
        self.host = '192.168.1.180'
        self.prot = 5672
        # 创建ZeroMQ通信器实例
        self.zmq_connectors = {}
        # self.zmq_communicator = ZeroMQCommunicator(zmq_conn_str)
        # 建立到RabbitMQ服务器的连接
        # self.connection = pika.BlockingConnection(pika.ConnectionParameters(virtual_host=self.vhost))
        credentials = pika.PlainCredentials(username="mq_admin", password='dw123#@!')
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.prot,
            virtual_host=self.vhost,
            credentials=credentials
        )
        self.connection = pika.BlockingConnection(parameters)
        # 创建一个通道
        self.channel = self.connection.channel()
        # 确保日志队列存在
        self.channel.queue_declare(queue=self.log_queue)

    def get_zmq_connector(self, zmq_conn_str):
        # 根据连接字符串获取或者创建ZeroMQ连接
        if zmq_conn_str not in self.zmq_connectors:
            self.zmq_connectors[zmq_conn_str] = ZeroMQCommunicator(zmq_conn_str)
        return self.zmq_connectors[zmq_conn_str]

    def callback(self, ch, method, properties, body):
        # 当收到消息时的回调函数
        message = body.decode()
        print(f"Received: {message}")

        # 解析消息内容中的ZeroMQ连接字符串
        message_data = json.loads(message)
        zmq_conn_str = message_data.get('Messages', {})
        actual_message = zmq_conn_str.get('Url')

        # 获取或创建对应的ZeroMQ连接
        zmq_communicator = self.get_zmq_connector(actual_message)

        # 使用ZeroMQ发送消息到其他服务器
        response = zmq_communicator.send_message(zmq_conn_str)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
        print(f"Received reply from ZeroMQ server: {response}")

        # 将回复消息存入日志队列
        self.channel.basic_publish(exchange='',
                                   routing_key=self.log_queue,
                                   body=response)

    def start_consuming(self):
        # 开始监听输入队列上的消息
        self.channel.basic_consume(queue=self.input_queue,
                                   on_message_callback=self.callback,
                                   auto_ack=True)
        print('Waiting for messages. To exit press CTRL+C')
        # 开始消费
        self.channel.start_consuming()

    def close_connection(self):
        # 关闭连接
        for connector in self.zmq_connectors.values():
            connector.close()
        self.zmq_connectors.clear()
        self.connection.close()


# 示例用法
if __name__ == "__main__":
    # 设置RabbitMQ生产者
    producer = RabbitMQProducer(vhost='message', queue_name='task_queue')
    # 发送一条消息
    producer.send_message('Hello World!')
    # 关闭生产者连接
    producer.close_connection()

    # 设置RabbitMQ消费者
    consumer = RabbitMQConsumer(vhost='message',
                                input_queue='task_queue',
                                log_queue='log_queue',
                                zmq_conn_str='tcp://localhost:5555')
    # 开始监听消息
    consumer.start_consuming()
    # 关闭消费者连接
    consumer.close_connection()
