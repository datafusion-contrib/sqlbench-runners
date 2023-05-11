FROM rust

RUN mkdir -p /code/src

ADD datafusion/entrypoint.sh /code/
ADD datafusion/Cargo.* /code/
ADD datafusion/src/* /code/src/

WORKDIR /code

RUN find .
RUN cargo build --release

ENTRYPOINT ["/entrypoint.sh"]

