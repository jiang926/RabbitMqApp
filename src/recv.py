from Tools.RabbitAndZmq import *

if __name__ == '__main__':
    rabbitmq_r = RabbitMQConsumer('Recv_log', 'test', 'log')
    rabbitmq_r.start_consuming()
    rabbitmq_r.close_connection()