// Topic-exchange pub/sub in one process.   rmq_pubsub_demo [host] [port]

#include <rmq/rmq.hpp>

#include <atomic>
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
        conn.setErrorCallback([](const rmq::Status& s) { std::cerr << "[error] " << s.message << "\n"; });
        conn.start();
        if (!conn.waitConnected(10s)) {
            std::cerr << "cannot reach the broker\n";
            return 1;
        }

        // Consumer: server-named, exclusive queue bound to "demo.events" / "sensor.*"
        rmq::SubscriberOptions so;
        so.durable = false;
        so.exclusive = true;
        so.bindings.push_back({"demo.events", "sensor.*"});

        // Publisher declares the exchange first so the binding can succeed.
        rmq::PublisherOptions po;
        po.exchange = "demo.events";
        po.exchange_type = "topic";
        po.declare_exchange = true;
        po.durable = false;
        rmq::Publisher publisher(conn, po);
        publisher.publish("warmup", rmq::Message{"ignored"});  // blocks until the exchange exists

        rmq::Subscriber subscriber(conn, so);
        std::atomic<int> received{0};
        subscriber.start([&](const rmq::Delivery& d) {
            std::cout << "got  " << d.routing_key << " = " << d.message.body << "\n";
            ++received;
            return rmq::AckAction::Ack;  // runs on the IO thread: keep it short
        });
        std::this_thread::sleep_for(500ms);  // let the queue + binding be declared

        for (int i = 0; i < 5; ++i) {
            rmq::Message m;
            m.body = "reading #" + std::to_string(i);
            m.content_type = "text/plain";
            const rmq::Status st = publisher.publish(i % 2 ? "sensor.temp" : "sensor.humidity", std::move(m));
            if (!st) std::cerr << "publish failed: " << st.message << "\n";
        }
        publisher.publishAsync("other.topic", rmq::Message{"not routed to us"});  // fire and forget

        std::this_thread::sleep_for(500ms);
        std::cout << "received " << received << " of 5\n";
    } catch (const rmq::Error& e) {
        std::cerr << "rmq error (" << rmq::to_string(e.code()) << "): " << e.what() << "\n";
        return 1;
    }
    return 0;
}
