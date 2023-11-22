FROM ubuntu

RUN apt update
RUN apt install -y python3 python3-pip

ADD entrypoint.sh /
ADD requirements.txt /
ADD sqlbench-duckdb.py /

RUN pip3 install -r /requirements.txt

ENTRYPOINT ["/entrypoint.sh"]

