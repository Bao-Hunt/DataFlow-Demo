# DataFlow Demo

Mini web app để hỗ trợ xây dựng data pipeline mẫu. Ứng dụng chạy bằng Flask và được đóng gói trong Docker image.

Quick start:

Build Docker image:

```bash
docker build -t dataflow-demo .
```

Run container:

```bash
docker run -p 8080:8080 dataflow-demo
```

Mở trình duyệt tới http://localhost:8080

Files:

- `app.py`: Flask app
- `templates/index.html`: giao diện chính
- `static/js/app.js`: frontend logic
- `Dockerfile`, `requirements.txt`
