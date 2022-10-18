FROM ubuntu:20.04

ARG ROOTDIR=/root/tilt_bench

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update
RUN apt-get install -y wget build-essential libssl-dev cmake python3-dev ninja-build git

WORKDIR /root
RUN wget https://github.com/llvm/llvm-project/releases/download/llvmorg-11.0.0/llvm-project-11.0.0.tar.xz
RUN tar -xvf llvm-project-11.0.0.tar.xz
WORKDIR llvm-project-11.0.0/build
RUN cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DLLVM_ENABLE_RTTI=ON \
    -DLLVM_TARGETS_TO_BUILD="X86" \
    -DLLVM_ENABLE_PROJECTS="clang;clang-tools-extra;libcxx;libcxxabi" \
    -DLLVM_BUILD_LLVM_DYLIB=ON \
    -DLLVM_LINK_LLVM_DYLIB=ON \
    ../llvm
RUN cmake --build .
RUN cmake --build . --target install

ADD ./. $ROOTDIR
WORKDIR ${ROOTDIR}/build
RUN cmake ..
RUN make -j$(nproc)
