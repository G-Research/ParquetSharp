ARG MANYLINUX_ARCH=x86_64
FROM quay.io/pypa/manylinux2014_${MANYLINUX_ARCH}

ARG BISON_VERSION=3.8.2

RUN yum install -y \
        devtoolset-10 \
        rh-git227 \
        httpd24-curl \
        flex \
        perl-Data-Dumper \
        perl-IPC-Cmd \
        perl-Time-Piece \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN uv tool install ninja

RUN curl -L -o /tmp/bison-${BISON_VERSION}.tar.gz \
        https://ftp.gnu.org/gnu/bison/bison-${BISON_VERSION}.tar.gz \
    && tar -xf /tmp/bison-${BISON_VERSION}.tar.gz -C /tmp \
    && cd /tmp/bison-${BISON_VERSION} \
    && ./configure && make && make install \
    && rm -rf /tmp/bison-${BISON_VERSION}*
