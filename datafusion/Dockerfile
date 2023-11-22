FROM rust:1.73.0

RUN apt-get update && apt-get upgrade

RUN mkdir -p /code/src

ADD entrypoint.sh /code/
ADD Cargo.* /code/
ADD src/* /code/src/

WORKDIR /code

RUN cargo build --release

ENTRYPOINT ["/code/entrypoint.sh"]

