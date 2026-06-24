Build and export Docker image (DataFlow-Demo)

This project includes `Dockerfile` to run the app with `gunicorn` on port 8080.

Quick steps (local):

1. Build image:

```bash
./scripts/build_image_and_export.sh dataflow-demo latest dataflow-demo_latest.tar
```

2. Send `dataflow-demo_latest.tar` to your manager (via secure file transfer).

3. On target host, load and run:

```bash
docker load -i dataflow-demo_latest.tar
docker run -d -p 8080:8080 --name dataflow-demo dataflow-demo:latest
```

Optional: push to Docker Hub

```bash
docker tag dataflow-demo:latest yourhubuser/dataflow-demo:latest
docker push yourhubuser/dataflow-demo:latest
```

Notes:
- Ensure target host has Docker installed.
- The app listens on port 8080; open port on firewall if necessary.
