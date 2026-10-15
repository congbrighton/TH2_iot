import pika
import json
import random
import time

QUEUE_NAME = 'sensor_data_queue'


def main():
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', port=5672)
        )
        channel = connection.channel()

        # Khai báo queue bền vững
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        print(f"[Sensor Producer] Đang gửi dữ liệu vào queue '{QUEUE_NAME}' (mỗi 3s)...")

        while True:
            # Sinh dữ liệu ngẫu nhiên
            payload = {
                "device_id": "sensor01",
                "temperature": round(random.uniform(25.0, 40.0), 1),
                "humidity": round(random.uniform(30.0, 70.0), 1)
            }
            message = json.dumps(payload)

            channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)  # Bền vững
            )

            print(f"[Sensor Producer] Đã gửi: {message}")
            time.sleep(3)

    except Exception as exc:
        print(f"[Sensor Producer] Khong the ket noi RabbitMQ hoac da dung: {exc}")
        print('Vui long kiem tra RabbitMQ da chay tren localhost:5672')


if __name__ == '__main__':
    main()