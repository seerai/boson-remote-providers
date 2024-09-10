FROM python:3.8-slim-buster as builder

# Update base container install
RUN apt-get update \
    && apt-get install -y python3-pip\
    && apt-get install -y git \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

RUN python3 -m pip install pip --upgrade

WORKDIR /app
COPY requirements.txt .
RUN pip install --user -r requirements.txt

FROM python:3.8-slim-buster as runner
COPY --from=builder /root/.local /root/.local

WORKDIR /app
COPY provider.py .
ENV PATH=/root/.local/bin:$PATH
CMD /usr/local/bin/python3 -u -m uvicorn --host 0.0.0.0 --port ${PORT} --log-level trace provider:app