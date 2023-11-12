FROM rust:1.73.0

RUN apt-get update && apt-get upgrade

RUN mkdir -p /code/src

ADD datafusion/entrypoint.sh /code/
ADD datafusion/Cargo.* /code/
ADD datafusion/src/* /code/src/

WORKDIR /code

RUN cargo build --release

ENTRYPOINT ["/code/entrypoint.sh"]

