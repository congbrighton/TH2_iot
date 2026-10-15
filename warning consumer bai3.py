import pika

EXCHANGE_NAME = 'iot_alert_exchange'
QUEUE_NAME = 'warning_queue'
ROUTING_KEY = 'warning'


def callback(ch, method, properties, body):
    message = body.decode('utf-8')
    print(f"[{QUEUE_NAME}] Da nhan: {message}")
    # Xác nhận đã xử lý xong tin nhắn
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = None
    try:
        # 1. Kết nối tới RabbitMQ Broker
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', port=5672)
        )
        channel = connection.channel()


        channel.exchange_declare(
            exchange=EXCHANGE_NAME,
            exchange_type='direct',
            durable=True
        )

        # 3. Khai báo Queue bền vững
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        s
        channel.queue_bind(
            exchange=EXCHANGE_NAME,
            queue=QUEUE_NAME,
            routing_key=ROUTING_KEY
        )

        # 5. Cấu hình nhận và lắng nghe tin nhắn
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=QUEUE_NAME,
            on_message_callback=callback
        )

        print(f"[Warning Consumer] Dang cho canh bao muc '{ROUTING_KEY}' trong queue '{QUEUE_NAME}'...")
        channel.start_consuming()

    except KeyboardInterrupt:
        print('\n[Warning Consumer] Dung chuong trinh.')
    except Exception as exc:
        print(f"[Warning Consumer] Khong the ket noi RabbitMQ: {exc}")
        print('Vui long kiem tra RabbitMQ da chay tren localhost:5672')
    finally:
        if connection and connection.is_open:
            connection.close()


if __name__ == '__main__':
    main()