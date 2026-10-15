import pika
import sys

QUEUE_NAME = 'iot_lab_queue'
STUDENT_INFO = 'B23DCCN102 - Nguyen Duc Cong'


def main():
    try:
        # 1. Kết nối tới RabbitMQ Broker
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', port=5672)
        )
        channel = connection.channel()

        # 2. Khai báo Queue bền vững (durable=True)
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        print("=== PRODUCER BÀI 1 ===")
        print("Nhập nội dung tin nhắn (Nhấn Enter để dùng nội dung mặc định, gõ 'exit' để thoát):")

        count = 1
        while True:
            user_input = input(f"\n[Message {count}] > ").strip()

            if user_input.lower() == 'exit':
                break

            if not user_input:
                message = f"Xin chao tu ung dung Python AMQP - {STUDENT_INFO} (Tin nhan #{count})"
            else:
                message = f"{user_input} - {STUDENT_INFO}"

            # 3. Gửi message bền vững (delivery_mode=2)
            channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=message.encode('utf-8'),
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent  # Tương đương delivery_mode=2
                )
            )
            print(f"[Producer] Đã gửi vào '{QUEUE_NAME}': {message}")
            count += 1

        connection.close()
        print("\n[Producer] Đã đóng kết nối an toàn.")

    except pika.exceptions.AMQPConnectionError as exc:
        print(f"\n[Lỗi] Không thể kết nối tới RabbitMQ: {exc}")
        print("Vui lòng kiểm tra dịch vụ RabbitMQ đã được bật tại localhost:5672 chưa.")
    except KeyboardInterrupt:
        print("\n[Producer] Đã dừng chương trình bằng bàn phím.")
        try:
            connection.close()
        except Exception:
            pass
    except Exception as exc:
        print(f"\n[Lỗi không xác định]: {exc}")


if __name__ == '__main__':
    main()