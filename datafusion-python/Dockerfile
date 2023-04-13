FROM ubuntu

RUN apt update
RUN apt install -y python3 python3-pip

ADD datafusion-python/entrypoint.sh /
ADD datafusion-python/requirements.txt /
ADD datafusion-python/sqlbench-datafusion-python.py /

RUN pip3 install -r /requirements.txt

ENTRYPOINT ["/entrypoint.sh"]

