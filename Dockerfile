FROM python:3.10

RUN python3 --version

ADD process.py /opt
ADD cfg /opt/cfg
WORKDIR /opt

CMD [ "python3", "./process.py" ]