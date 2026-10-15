import pika

EXCHANGE_NAME = 'iot_alert_exchange'


def main():
    connection = None
    try:
        # 1. Kết nối tới RabbitMQ Broker
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', port=5672)
        )
        channel = connection.channel()

        # 2. Khai báo Direct Exchange bền vững (durable=True)
        channel.exchange_declare(
            exchange=EXCHANGE_NAME,
            exchange_type='direct',
            durable=True
        )

        # Danh sách các cảnh báo với mức độ (routing_key) tương ứng
        alerts = [
            ("info", "He thong kho lanh hoat dong binh thuong"),
            ("warning", "Nhiet do phong may vuot nguong warning"),
            ("critical", "Cam bien kho lanh mat ket noi critical"),
            ("warning", "Dien ap thap tai tram bien ap B2"),
            ("critical", "Phat hien ro ri khi gas tai xuong A")
        ]

        print(f"[Alert Producer] Đang gửi các cảnh báo tới exchange '{EXCHANGE_NAME}'...\n")

        # 3. Gửi lần lượt các message kèm routing key
        for routing_key, message in alerts:
            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=routing_key,
                body=message.encode('utf-8'),
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent  # Lưu trữ bền vững
                )
            )
            print(f"[Alert Producer] Da gui [{routing_key}]: '{message}'")

        print("\n[Alert Producer] Hoàn thành gửi tất cả cảnh báo.")

    except KeyboardInterrupt:
        print('\n[Alert Producer] Dung chuong trinh.')
    except Exception as exc:
        print(f"[Alert Producer] Khong the ket noi RabbitMQ: {exc}")
        print('Vui long kiem tra RabbitMQ da chay tren localhost:5672')
    finally:
        if connection and connection.is_open:
            connection.close()


if __name__ == '__main__':
    main()