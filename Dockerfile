FROM python:3.10-alpine

WORKDIR /py-docker
EXPOSE 5000
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
ENV CONTAINER_NAME=ccmongo-srv

COPY . .

CMD ["python3", "mongo_flask_server.py"]