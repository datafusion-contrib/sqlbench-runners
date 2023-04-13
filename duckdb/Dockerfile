FROM ubuntu

RUN apt update
RUN apt install -y python3 python3-pip

ADD duckdb/entrypoint.sh /
ADD duckdb/requirements.txt /
ADD duckdb/sqlbench-duckdb.py /

RUN pip3 install -r /requirements.txt

ENTRYPOINT ["/entrypoint.sh"]

