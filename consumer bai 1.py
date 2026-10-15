import pika
from datetime import datetime

QUEUE_NAME = 'iot_lab_queue'


def callback(ch, method, properties, body):
    message = body.decode('utf-8')
    recv_time = datetime.now().strftime('%H:%M:%S')

    # In theo đúng định dạng mẫu của đề bài
    print(f"Da nhan message: {message}")
    print(f"Thoi gian nhan: {recv_time}")
    print("-" * 40)

    # Xác nhận đã xử lý xong
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', port=5672)
        )
        channel = connection.channel()

        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

        print(f"[*] Consumer đang chờ tin nhắn trong queue '{QUEUE_NAME}'. Nhấn CTRL+C để thoát...\n")
        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as exc:
        print(f"\n[Consumer] Khong the ket noi RabbitMQ: {exc}")
        print("Vui long kiem tra RabbitMQ da chay tren localhost:5672")
    except KeyboardInterrupt:
        print("\n[Consumer] Đã dừng chương trình.")
        try:
            connection.close()
        except Exception:
            pass
    except Exception as exc:
        print(f"\n[Consumer] Lỗi không xác định: {exc}")


if __name__ == '__main__':
    main()