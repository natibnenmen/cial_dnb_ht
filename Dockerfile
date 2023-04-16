#FROM ubuntu:18.04
FROM python:3.10

#RUN apt-get -yqq update
#RUN apt-get -yqq install python3-pip python3-dev
RUN python3 --version

ADD process.py /opt
ADD cfg /opt/cfg
WORKDIR /opt

CMD [ "python3", "./process.py" ]