# TH2_iot
# BÁO CÁO THỰC HÀNH 2: LẬP TRÌNH PYTHON VỚI GIAO THỨC AMQP

## 1. Broker
- **RabbitMQ local:** `localhost:5672`
- Không yêu cầu xác thực theo cấu hình mặc định.

## 2. Cài đặt
Cài đặt thư viện `pika`:
```bash
pip install pika
Nếu dùng môi trường ảo (Virtual Environment):

PowerShell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install pika
3. Chạy các chương trình
Bài 1: Producer và Consumer cơ bản
Bash
# Terminal 1
python consumer_bai1.py

# Terminal 2
python producer_bai1.py
Bài 2: Sensor Producer và Monitor Consumer
Bash
# Terminal 1
python monitor_consumer_bai2.py

# Terminal 2
python sensor_producer_bai2.py
Bài 3: Alert Producer và Consumer với Exchange
Bash
# Terminal 1 (Lắng nghe các cảnh báo warning)
python warning_consumer_bai3.py

# Terminal 2 (Lắng nghe các cảnh báo critical)
python critical_consumer_bai3.py

# Terminal 3 (Gửi cảnh báo theo mức độ)
python alert_producer_bai3.py warning "Nhiệt độ phòng máy vượt ngưỡng"
python alert_producer_bai3.py critical "Cảm biến kho lạnh mất kết nối"
python alert_producer_bai3.py info "Hệ thống khởi động"
4. Kết quả đạt được
Bài 1
Producer kết nối RabbitMQ và gửi bản tin vào queue iot_lab_queue.

Consumer nhận và in ra nội dung message cùng thời gian nhận.

Bài 2
Sensor Producer gửi dữ liệu JSON theo chu kỳ 3 giây với các trường: device_id, temperature, humidity.

Monitor Consumer phân tích dữ liệu và cảnh báo:

Nhiệt độ > 35 → cảnh báo nhiệt độ cao.

Độ ẩm < 40 → cảnh báo độ ẩm thấp.

Bài 3
Alert Producer gửi tin nhắn tới exchange iot_alert_exchange với routing key info, warning, hoặc critical.

warning_consumer_bai3.py chỉ nhận tin nhắn có routing key warning.

critical_consumer_bai3.py chỉ nhận tin nhắn có routing key critical.

Message info không được consumer nào nhận vì không có queue nào bind với routing key info.

5. Ghi chú
Đảm bảo dịch vụ RabbitMQ đang chạy trước khi thực thi các script.

Nếu RabbitMQ chưa được cài đặt trực tiếp trên máy, có thể khởi động nhanh bằng Docker:

Bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

---

### Cập nhật mã nguồn `alert_producer_bai3.py` (Hỗ trợ chạy CLI)

Để chạy được lệnh ở Bài 3 giống hệt file README (`python alert_producer_bai3.py <routing_key> "<message>"`), bạn dùng đoạn code đã được nâng cấp xử lý tham số dòng lệnh bên dưới:

```python
import sys
import pika

EXCHANGE_NAME = 'iot_alert_exchange'


def main():
    # Đọc routing_key và message từ tham số dòng lệnh (Command Line Arguments)
    if len(sys.argv) >= 3:
        routing_key = sys.argv[1]
        message = " ".join(sys.argv[2:])
    else:
        # Giá trị mặc định nếu không truyền tham số
        routing_key = "info"
        message = "Hệ thống khởi động mặc định"

    connection = None
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', port=5672)
        )
        channel = connection.channel()

        # Khai báo Direct Exchange bền vững
        channel.exchange_declare(
            exchange=EXCHANGE_NAME, 
            exchange_type='direct', 
            durable=True
        )

        # Gửi tin nhắn với routing key được truyền vào
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=routing_key,
            body=message.encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            )
        )
        print(f"[Alert Producer] Da gui [{routing_key}]: '{message}'")

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
