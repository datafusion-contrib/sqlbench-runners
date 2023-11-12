FROM ubuntu

RUN apt update
RUN apt install -y python3 python3-pip

ADD polars/entrypoint.sh /
ADD polars/requirements.txt /
ADD polars/sqlbench-polars.py /

RUN pip3 install -r /requirements.txt

RUN pip3 install pyarrow

ENTRYPOINT ["/entrypoint.sh"]

