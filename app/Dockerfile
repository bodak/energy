FROM python:3.9-slim
WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY octopus_to_influxdb.py ./

ENTRYPOINT [ "python3", "./octopus_to_influxdb.py" ]
