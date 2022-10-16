FROM ubuntu:18.04

ARG ROOTDIR=/root/grizzly_bench

ADD ./. $ROOTDIR
WORKDIR $ROOTDIR

RUN apt-get update
RUN apt-get install -y wget

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get install -y libboost-all-dev

RUN apt install -y lsb-release wget software-properties-common gnupg
RUN wget https://apt.llvm.org/llvm.sh
RUN chmod +x llvm.sh
RUN ./llvm.sh 11
RUN ln -s /usr/bin/clang++-11 /usr/bin/clang++
RUN ln -s /usr/bin/clang-11 /usr/bin/clang

RUN apt-get install -y libtbb-dev

RUN apt-get install -y cmake
RUN apt-get install -y libpthread-stubs0-dev
RUN apt-get install -y libpapi-dev

WORKDIR ${ROOTDIR}/data-generator
RUN g++ yahooDataGenerator.cpp -o yahoo_data
RUN ./yahoo_data 20000000 1

WORKDIR ${ROOTDIR}/build
RUN cmake -DCMAKE_BUILD_TYPE=Release ..
RUN make -j$(nproc)
