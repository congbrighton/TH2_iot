import json

import pika

QUEUE_NAME = 'sensor_data_queue'


def callback(ch, method, properties, body):
    try:
        payload = json.loads(body.decode('utf-8'))
        device_id = payload.get('device_id', 'Unknown')
        temperature = payload.get('temperature', 0)
        humidity = payload.get('humidity', 0)

        print(f"[Monitor] Device: {device_id} | Temp: {temperature} | Humidity: {humidity}")

        if temperature > 35:
            print('CANH BAO: Nhiệt độ cao')
        if humidity < 40:
            print('CANH BAO: Độ ẩm thấp')

    except json.JSONDecodeError:
        print("[Monitor] Error: Message khong dung dinh dang JSON!")
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = None
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', port=5672)
        )
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

        print(f"[Monitor] Dang cho du lieu tu queue '{QUEUE_NAME}'...")
        channel.start_consuming()

    except KeyboardInterrupt:
        print('\n[Monitor] Dung chuong trinh.')
    except Exception as exc:
        print(f"[Monitor] Khong the ket noi RabbitMQ: {exc}")
        print('Vui long kiem tra RabbitMQ da chay tren localhost:5672')
    finally:
        if connection and connection.is_open:
            connection.close()


if __name__ == '__main__':
    main()