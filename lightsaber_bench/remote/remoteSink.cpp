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
    protected:
    int m_sock = 0;
    int m_server_fd = 0;
    std::vector<char> *buffer = nullptr;

    public:
    int run(int64_t output_size) {
        InitializeBuffer(output_size * sizeof(T));

        setupSocket();
        readBytes(m_sock, output_size * sizeof(T), (void *)buffer->data());

        print_buffer(buffer, output_size);
        return 0;
    }

    private:
    void InitializeBuffer(int64_t size) {
        buffer = new std::vector<char>(size);
    }

    static void readBytes(int socket, unsigned int length, void *buffer) {
        size_t bytesRead = 0;
        while (bytesRead < length) {
            auto valread = read(socket, (char *)buffer + bytesRead, length - bytesRead);
            bytesRead += valread;
        }
    }

    void print_buffer(std::vector<char> *buf, int64_t len) {
        auto arr = (T *) buf->data();
        for (unsigned long idx = 0; idx < len; idx++) {
            arr[idx].print_data();
        }
    }

    void setupSocket() {
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
};

int main(int argc, const char **argv) {
    std::string testcase = (argc > 1) ? argv[1] : "select";
    int64_t output_size = (argc > 2) ? atoi(argv[2]) : 10000000;
    if (testcase == "aggregate") {
        auto remoteSink = std::make_unique<RemoteSink<AggregateOutputSchema>>();
        remoteSink->run(output_size);
    } else if (testcase == "yahoo") {
        auto remoteSink = std::make_unique<RemoteSink<YahooOutputSchema>>();
        remoteSink->run(output_size);
    } else {
        auto remoteSink = std::make_unique<RemoteSink<OutputSchema>>();
        remoteSink->run(output_size);
    }
    return 0;
}