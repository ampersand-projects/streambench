#### Dependencies and usage:

Install Boost Library:

```
git clone https://github.com/boostorg/boost.git && \
cd boost && \
git checkout boost-1.78.0 && \
git submodule init && \
git submodule update && \
./bootstrap.sh --prefix=/usr/local && \
./b2 install --prefix=/usr/local
```

Install Protobuf Library:

```
git clone https://github.com/protocolbuffers/protobuf.git && \
cd protobuf && \
git checkout v3.19.4 && \
git submodule init && \
git submodule update && \
cd cmake && \
mkdir build && \
cd build  && \
cmake -DCMAKE_INSTALL_PREFIX=/usr/local ..  && \
make -j$(nproc) && \
make install
```

Install Protobuf Java Runtime (protobuf-java-3.19.4.jar):

```
cd protobuf/java && \
mvn test && \
mvn package
```

Load Dataset with Java:

```
protoc -I /path/to/protos --java_out=. data.proto
javac -cp /path/to/protobuf-java-3.19.4.jar:. DataLoader.java
./dataset-parser | java -cp /path/to/protobuf-java-3.19.4.jar:. DataLoader
```
