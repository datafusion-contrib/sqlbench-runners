FROM ubuntu

RUN apt update
RUN apt install -y python3 python3-pip

ADD datafusion-python/entrypoint.sh /
ADD datafusion-python/requirements.txt /
ADD datafusion-python/sqlbench-datafusion-python.py /

#RUN pip3 install -r /requirements.txt

RUN pip3 install pyarrow
RUN pip3 install -i https://test.pypi.org/simple/ datafusion==23.0.0

ENTRYPOINT ["/entrypoint.sh"]

