FROM python:3.10.12

ADD ./ /db-replica

WORKDIR "/db-replica"

RUN python3 -m pip install --upgrade pip && \
    apt update -y && \
    pip install pyyaml

CMD ["python", "-u", "replica.py"]
 