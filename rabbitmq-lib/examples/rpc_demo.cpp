// Request/reply in one process.   rmq_rpc_demo [host] [port]

#include <rmq/rmq.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>

using namespace std::chrono_literals;

int main(int argc, char** argv) {
    rmq::ConnectionOptions opts;
    if (argc > 1) opts.host = argv[1];
    if (argc > 2) opts.port = static_cast<std::uint16_t>(std::atoi(argv[2]));

    try {
        rmq::Connection conn(opts);
        conn.start();
        if (!conn.waitConnected(10s)) {
            std::cerr << "cannot reach the broker\n";
            return 1;
        }

        // ---- server ----
        rmq::RpcServerOptions so;
        so.queue = "demo.rpc.upper";
        so.durable = false;
        rmq::RpcServer server(conn, so);
        server.start([](const rmq::Delivery& request) {
            if (request.message.body.empty()) throw std::runtime_error("empty request");  // -> Remote error
            rmq::Message reply;
            reply.body = request.message.body;
            std::transform(reply.body.begin(), reply.body.end(), reply.body.begin(),
                           [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
            return reply;
        });
        std::this_thread::sleep_for(500ms);

        // ---- client ----
        rmq::RpcClientOptions co;
        co.routing_key = "demo.rpc.upper";  // default exchange -> the server's queue
        co.default_timeout = 3s;
        rmq::RpcClient client(conn, co);

        // 1) blocking style via std::future
        std::cout << "reply: " << client.call(rmq::Message{"hello rpc"}).get().body << "\n";

        // 2) callback style (callback runs on the IO thread)
        std::promise<void> done;
        client.callAsync(rmq::Message{"async hello"}, [&](const rmq::Status& st, rmq::Message reply) {
            std::cout << (st ? "async reply: " + reply.body : "async failed: " + st.message) << "\n";
            done.set_value();
        });
        done.get_future().wait();

        // 3) server-side exception arrives as rmq::Error{Remote}
        try {
            client.call(rmq::Message{""}).get();
        } catch (const rmq::Error& e) {
            std::cout << "remote error (" << rmq::to_string(e.code()) << "): " << e.what() << "\n";
        }
    } catch (const rmq::Error& e) {
        std::cerr << "rmq error (" << rmq::to_string(e.code()) << "): " << e.what() << "\n";
        return 1;
    }
    return 0;
}
