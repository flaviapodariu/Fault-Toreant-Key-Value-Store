FROM python:3.10.12

ADD ./ /db-replica2

WORKDIR "/db-replica2"

RUN python3 -m pip install --upgrade pip && \
    apt update -y && \
    pip install pyyaml

CMD ["python", "-u", "replica.py"]
 