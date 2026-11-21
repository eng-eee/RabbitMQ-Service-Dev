// Integration test against a real broker (no test framework needed).
//
//   rmq_integration_test [host] [port] [mode]
//     mode "basic"     (default): pub/sub, RPC, error/timeout paths
//     mode "reconnect" : runs ~20 s; restart the broker's app/connections meanwhile
//                        (e.g. `rabbitmqctl stop_app; sleep 2; rabbitmqctl start_app`)

#include <rmq/rmq.hpp>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <mutex>
#include <set>
#include <thread>

using namespace std::chrono_literals;

static int g_failures = 0;
#define CHECK(cond)                                                                   \
    do {                                                                              \
        if (!(cond)) {                                                                \
            ++g_failures;                                                             \
            std::cerr << "CHECK FAILED " << __FILE__ << ":" << __LINE__ << ": " #cond "\n"; \
        }                                                                             \
    } while (0)

template <typename Pred>
static bool waitFor(Pred pred, std::chrono::milliseconds timeout) {
    const auto end = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < end) {
        if (pred()) return true;
        std::this_thread::sleep_for(10ms);
    }
    return pred();
}

static rmq::ConnectionOptions makeOptions(int argc, char** argv) {
    rmq::ConnectionOptions o;
    if (argc > 1) o.host = argv[1];
    if (argc > 2) o.port = static_cast<std::uint16_t>(std::atoi(argv[2]));
    o.heartbeat_seconds = 10;
    o.reconnect_min_delay = 200ms;
    o.reconnect_max_delay = 2s;
    return o;
}

static void basic(rmq::Connection& conn) {
    CHECK(conn.waitConnected(10s));

    // ---------------- pub/sub: default exchange -> durable queue ----------------
    {
        rmq::SubscriberOptions so;
        so.queue = "rmq.test.queue";
        so.durable = false;
        so.auto_delete = true;
        rmq::Subscriber sub(conn, so);

        std::mutex m;
        std::set<std::string> bodies;
        std::atomic<int> withHeader{0};
        sub.start([&](const rmq::Delivery& d) {
            std::lock_guard<std::mutex> lock(m);
            bodies.insert(d.message.body);
            auto it = d.message.headers.find("k");
            if (it != d.message.headers.end() && it->second == "v" && d.message.content_type == "text/plain")
                ++withHeader;
            return rmq::AckAction::Ack;
        });
        std::this_thread::sleep_for(500ms);  // let the queue be declared

        rmq::Publisher pub(conn);
        for (int i = 0; i < 50; ++i) {
            rmq::Message msg;
            msg.body = "sync-" + std::to_string(i);
            msg.content_type = "text/plain";
            msg.headers["k"] = "v";
            auto st = pub.publish("rmq.test.queue", std::move(msg));
            CHECK(st.ok());
            if (!st.ok()) std::cerr << "publish: " << rmq::to_string(st.code) << " " << st.message << "\n";
        }
        std::atomic<int> confirmed{0};
        for (int i = 0; i < 50; ++i) {
            rmq::Message msg;
            msg.body = "async-" + std::to_string(i);
            msg.content_type = "text/plain";
            msg.headers["k"] = "v";
            pub.publishAsync("rmq.test.queue", std::move(msg),
                             [&](const rmq::Status& s) { if (s.ok()) ++confirmed; });
        }
        CHECK(waitFor([&] { return confirmed == 50; }, 5s));
        CHECK(waitFor([&] { std::lock_guard<std::mutex> l(m); return bodies.size() == 100; }, 5s));
        CHECK(withHeader == 100);
        std::cout << "pub/sub (default exchange): received " << bodies.size() << "/100\n";
    }

    // ---------------- pub/sub: topic exchange, server-named queue ----------------
    {
        rmq::PublisherOptions po;
        po.exchange = "rmq.test.topic";
        po.exchange_type = "topic";
        po.declare_exchange = true;
        po.durable = false;
        rmq::Publisher pub(conn, po);
        CHECK(pub.publish("warmup", rmq::Message{"x"}).ok());  // makes sure the exchange exists

        rmq::SubscriberOptions so;
        so.durable = false;
        so.exclusive = true;
        so.bindings.push_back({"rmq.test.topic", "sensor.*"});
        rmq::Subscriber sub(conn, so);
        std::atomic<int> got{0};
        sub.start([&](const rmq::Delivery& d) {
            if (d.routing_key == "sensor.temp") ++got;
            return rmq::AckAction::Ack;
        });
        CHECK(waitFor([&] { return !sub.queueName().empty(); }, 3s));
        std::this_thread::sleep_for(500ms);

        CHECK(pub.publish("sensor.temp", rmq::Message{"21.5"}).ok());
        CHECK(pub.publish("other.thing", rmq::Message{"ignored"}).ok());
        CHECK(waitFor([&] { return got == 1; }, 3s));
        std::this_thread::sleep_for(200ms);
        CHECK(got == 1);
        std::cout << "pub/sub (topic exchange, server-named queue): ok, queue=" << sub.queueName() << "\n";
    }

    // ---------------- publisher failure path: unknown exchange ----------------
    {
        rmq::PublisherOptions po;
        po.exchange = "rmq.does.not.exist";
        rmq::Publisher pub(conn, po);
        auto st = pub.publish("k", rmq::Message{"x"}, 3s);
        CHECK(!st.ok());
        std::cout << "publish to unknown exchange -> " << rmq::to_string(st.code) << ": " << st.message << "\n";
    }

    // ---------------- RPC ----------------
    {
        rmq::RpcServerOptions so;
        so.queue = "rmq.test.rpc";
        so.durable = false;
        rmq::RpcServer server(conn, so);
        server.start([](const rmq::Delivery& req) {
            if (req.message.body == "boom") throw std::runtime_error("kaboom");
            rmq::Message reply;
            reply.body = "echo:" + req.message.body;
            return reply;
        });
        std::this_thread::sleep_for(500ms);

        rmq::RpcClientOptions co;
        co.routing_key = "rmq.test.rpc";
        co.default_timeout = 3s;
        rmq::RpcClient client(conn, co);

        std::vector<std::future<rmq::Message>> futures;
        for (int i = 0; i < 20; ++i) futures.push_back(client.call(rmq::Message{"hello" + std::to_string(i)}));
        int ok = 0;
        for (int i = 0; i < 20; ++i) {
            try {
                if (futures[i].get().body == "echo:hello" + std::to_string(i)) ++ok;
            } catch (const std::exception& e) {
                std::cerr << "rpc call failed: " << e.what() << "\n";
            }
        }
        CHECK(ok == 20);
        std::cout << "rpc: " << ok << "/20 replies correct\n";

        try {
            client.call(rmq::Message{"boom"}).get();
            CHECK(false);
        } catch (const rmq::Error& e) {
            CHECK(e.code() == rmq::ErrorCode::Remote);
            std::cout << "rpc remote error -> " << rmq::to_string(e.code()) << ": " << e.what() << "\n";
        }

        rmq::RpcClientOptions dead;
        dead.routing_key = "rmq.test.nobody.listens";
        rmq::RpcClient lonely(conn, dead);
        const auto t0 = std::chrono::steady_clock::now();
        try {
            lonely.call(rmq::Message{"anyone?"}, 400ms).get();
            CHECK(false);
        } catch (const rmq::Error& e) {
            CHECK(e.code() == rmq::ErrorCode::Timeout);
            const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
            CHECK(ms >= 350 && ms < 1500);
            std::cout << "rpc timeout -> " << rmq::to_string(e.code()) << " after " << ms << " ms\n";
        }
    }

    // ---------------- clean shutdown with live objects ----------------
    {
        rmq::Publisher pub(conn);
        rmq::SubscriberOptions so;
        so.durable = false;
        so.exclusive = true;
        rmq::Subscriber sub(conn, so);
        sub.start([](const rmq::Delivery&) { return rmq::AckAction::Ack; });
        std::this_thread::sleep_for(200ms);
    }  // destructors must not hang or crash
    std::cout << "object teardown: ok\n";
}

static void reconnect(rmq::Connection& conn) {
    CHECK(conn.waitConnected(10s));

    std::atomic<int> received{0};
    std::atomic<int> confirmed{0}, failed{0};
    std::atomic<bool> sawReconnecting{false};
    conn.setStateCallback([&](rmq::ConnectionState s, const std::string&) {
        std::cout << "[state] " << rmq::to_string(s) << std::endl;
        if (s == rmq::ConnectionState::Reconnecting) sawReconnecting = true;
    });

    rmq::SubscriberOptions so;
    so.queue = "rmq.test.reconnect";
    so.durable = false;
    rmq::Subscriber sub(conn, so);
    sub.start([&](const rmq::Delivery&) { ++received; return rmq::AckAction::Ack; });

    rmq::Publisher pub(conn);
    std::cout << "publishing for 20 s - restart the broker connections now..." << std::endl;
    const auto end = std::chrono::steady_clock::now() + 20s;
    int sentAfter = 0;
    while (std::chrono::steady_clock::now() < end) {
        pub.publishAsync("rmq.test.reconnect", rmq::Message{"tick"},
                         [&](const rmq::Status& s) { s.ok() ? ++confirmed : ++failed; });
        std::this_thread::sleep_for(50ms);
        if (sawReconnecting && conn.isConnected()) ++sentAfter;
    }
    std::this_thread::sleep_for(500ms);
    std::cout << "confirmed=" << confirmed << " failed=" << failed << " received=" << received
              << " publishedAfterReconnect=" << sentAfter << "\n";
    CHECK(sawReconnecting);
    CHECK(sentAfter > 20);
    CHECK(received > 0);
}

int main(int argc, char** argv) {
    const std::string mode = argc > 3 ? argv[3] : "basic";
    try {
        rmq::Connection conn(makeOptions(argc, argv));
        conn.setErrorCallback([](const rmq::Status& s) {
            std::cerr << "[conn error] " << rmq::to_string(s.code) << ": " << s.message << "\n";
        });
        conn.start();
        if (mode == "reconnect") reconnect(conn); else basic(conn);
        conn.stop();
    } catch (const std::exception& e) {
        std::cerr << "exception: " << e.what() << "\n";
        return 2;
    }
    std::cout << (g_failures ? "FAILED" : "ALL OK") << " (" << g_failures << " failures)\n";
    return g_failures ? 1 : 0;
}
