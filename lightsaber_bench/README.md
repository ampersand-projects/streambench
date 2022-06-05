```
docker build -t lightsaber .
docker run -it lightsaber /bin/bash
docker exec -it {CONTAINER_ID} /bin/bash (From another terminal)
cd /root/lightsaber_bench/build
./remoteSink {testcase} {size}
./lightsaber {testcase} {size}
```

To view the result, `TCP_OUTPUT` must be defined in the CMakeLists.txt. The program will not wait until all outputs in the buffer have been sent over to the sink. It may be necessary to add a sleep() call in main.cpp at the end to wait for all outputs to be delivered. 