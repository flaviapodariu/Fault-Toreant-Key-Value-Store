FROM python:3.10.12

ADD ./ /db-master

WORKDIR "/db-master"

RUN python3 -m pip install --upgrade pip

RUN apt update -y && pip install pyyaml

EXPOSE 5001

CMD ["python", "-u", "master.py"]
