FROM python:3.10.12

ADD ./ /server
WORKDIR "/server"

RUN python3 -m pip install --upgrade pip && \
    apt update -y && \
    pip install Flask

EXPOSE 5000
CMD ["python", "-u", "server.py"]
 