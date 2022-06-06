#include <netinet/in.h>
#include <sys/socket.h>
#include <vector>
#include <iostream>

// To import PORT from SystemConf
#include <utils/SystemConf.h> 

struct alignas(16) OutputSchema {
    long st;
    long dur;
    float payload;

    void print_data() {
        std::cout << "[" << st << "-" << st + dur << "]: " << payload << std::endl;
    }
};

struct alignas(16) AggregateOutputSchema {
    long timestamp;
    float sum;

    void print_data() {
        std::cout << "[" << timestamp << "]: " << sum << std::endl;
    }
};

struct alignas(16) YahooOutputSchema {
    long timestamp;
    int count;

    void print_data() {
        std::cout << "[" << timestamp << "]: " << count << std::endl;
    }
};

template<typename T>
class RemoteSink {
private:
    int64_t batch_size;
    int64_t buffer_size;
    int m_sock = 0;
    int m_server_fd = 0;
    std::vector<char> buffer;

    void read_one_batch() {
        int64_t bytes_read = 0;
        while (bytes_read < buffer_size) {
            auto valread = read(m_sock, (char *)(buffer.data()) + bytes_read, buffer_size - bytes_read);
            bytes_read += valread;
        }
    }

    void print_buffer() {
        auto arr = (T *) buffer.data();
        for (unsigned long idx = 0; idx < batch_size; idx++) {
            arr[idx].print_data();
        }
    }

    void setup_socket() {
        struct sockaddr_in address {};
        int opt = 1;
        int addrlen = sizeof(address);

        if ((m_server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
            throw std::runtime_error("error: Socket file descriptor creation error");
        }

        if (setsockopt(m_server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
            throw std::runtime_error("error: setsockopt");
        }
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY;
        address.sin_port = htons(PORT);

        if (bind(m_server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
            throw std::runtime_error("error: bind failed");
        }
        if (listen(m_server_fd, 3) < 0) {
            throw std::runtime_error("error: listen");
        }

        if ((m_sock = accept(m_server_fd, (struct sockaddr *)&address, (socklen_t *)&addrlen)) < 0) {
            throw std::runtime_error("error: accept");
        }
    }

public:
    RemoteSink(int64_t batch_size) :
        batch_size(batch_size),
        buffer_size(batch_size * sizeof(T)),
        buffer(buffer_size)
    {}

    void run()
    {
        setup_socket();

        while (true) {
            read_one_batch();
            std::cout << "Successfully read " << batch_size << " tuples." << std::endl;
            // print_buffer();
        }
    }
};

int main(int argc, const char **argv) {
    std::string testcase = (argc > 1) ? argv[1] : "select";
    int64_t batch_size = (argc > 2) ? atoi(argv[2]) : 10000000;

    if (testcase == "aggregate") {
        auto remoteSink = std::make_unique<RemoteSink<AggregateOutputSchema>>(batch_size);
        remoteSink->run();
    } else if (testcase == "yahoo") {
        auto remoteSink = std::make_unique<RemoteSink<YahooOutputSchema>>(batch_size);
        remoteSink->run();
    } else {
        auto remoteSink = std::make_unique<RemoteSink<OutputSchema>>(batch_size);
        remoteSink->run();
    }

    return 0;
}