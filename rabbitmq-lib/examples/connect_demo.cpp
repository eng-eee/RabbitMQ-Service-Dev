// Connects to a broker, prints every state change and runs until Ctrl-C
// (or for the given number of seconds).
//
//   rmq_connect_demo [host] [port] [seconds]

#include <rmq/rmq.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <thread>

namespace {
std::atomic<bool> g_stop{false};
void onSignal(int) { g_stop = true; }
}  // namespace

int main(int argc, char** argv) {
    rmq::ConnectionOptions opts;
    if (argc > 1) opts.host = argv[1];
    if (argc > 2) opts.port = static_cast<std::uint16_t>(std::atoi(argv[2]));
    const int seconds = argc > 3 ? std::atoi(argv[3]) : 0;
    opts.heartbeat_seconds = 10;
    opts.reconnect_min_delay = std::chrono::milliseconds(200);

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    try {
        rmq::Connection connection(opts);
        connection.setStateCallback([](rmq::ConnectionState s, const std::string& detail) {
            std::cout << "[state] " << rmq::to_string(s) << " - " << detail << std::endl;
        });
        connection.setErrorCallback([](const rmq::Status& st) {
            std::cout << "[error] " << rmq::to_string(st.code) << ": " << st.message << std::endl;
        });

        connection.start();
        if (!connection.waitConnected(std::chrono::seconds(10))) {
            std::cout << "not connected after 10s (state: " << rmq::to_string(connection.state())
                      << "), still retrying in the background\n";
        }

        const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
        while (!g_stop && (seconds == 0 || std::chrono::steady_clock::now() < until)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        connection.stop();  // also done by the destructor
    } catch (const rmq::Error& e) {
        std::cerr << "rmq error (" << rmq::to_string(e.code()) << "): " << e.what() << "\n";
        return 1;
    }
    return 0;
}
