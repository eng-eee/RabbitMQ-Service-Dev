#include "app_common/config.hpp"
#include "app_common/event.hpp"
#include "app_common/logging.hpp"
#include "log.h"

#include <rmq/rmq.hpp>

#include <cctype>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>

namespace {

using namespace std::chrono_literals;

struct Options {
    std::string customer = "";
    std::string product = "P-100";
    long long quantity = 1;
    double price = 19.99;
    int count = 1;
    int intervalMs = 200;
    bool forcePaymentFailure = false;
    bool demo = false;
    bool interactive = false;
};

Options parseOptions(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        const auto next = [&](const char* name) -> std::string {
            if (i + 1 >= argc) {
                std::cerr << "missing value for " << name << "\n";
                std::exit(2);
            }
            return argv[++i];
        };
        if (arg == "--customer") {
            options.customer = next("--customer");
        } else if (arg == "--product") {
            options.product = next("--product");
        } else if (arg == "--quantity") {
            options.quantity = std::stoll(next("--quantity"));
        } else if (arg == "--price") {
            options.price = std::stod(next("--price"));
        } else if (arg == "--count") {
            options.count = std::stoi(next("--count"));
        } else if (arg == "--interval") {
            options.intervalMs = std::stoi(next("--interval"));
        } else if (arg == "--force-payment-failure") {
            options.forcePaymentFailure = true;
        } else if (arg == "--demo") {
            options.demo = true;
        } else if (arg == "--interactive" || arg == "-i") {
            options.interactive = true;
        } else if (arg == "--help" || arg == "-h") {
            std::cout
                << "usage: order_cli [options]\n"
                << "  --customer <id>              customer id (default generated)\n"
                << "  --product <id>               product id (default P-100)\n"
                << "  --quantity <n>               quantity (default 1)\n"
                << "  --price <x>                  unit price (default 19.99)\n"
                << "  --count <n>                  number of orders (default 1)\n"
                << "  --interval <ms>              delay between orders (default 200)\n"
                << "  --force-payment-failure      make the payment service fail\n"
                << "  --demo                       send one normal, one out-of-stock and "
                   "one payment-failure order\n"
                << "  -i, --interactive            prompt for each order (default when "
                   "run in a terminal)\n";
            std::exit(0);
        } else {
            std::cerr << "unknown option: " << arg << "\n";
            std::exit(2);
        }
    }
    return options;
}

app::Event makeRequest(const std::string& customer, const std::string& product,
                       long long quantity, double price, bool forcePaymentFailure) {
    app::json::Value item = app::json::Value::makeObject();
    item["product_id"] = app::json::Value(product);
    item["quantity"] = app::json::Value(quantity);
    item["unit_price"] = app::json::Value(price);

    app::json::Value items = app::json::Value::makeArray();
    items.pushBack(std::move(item));

    app::json::Value data = app::json::Value::makeObject();
    data["customer_id"] = app::json::Value(customer);
    data["items"] = std::move(items);
    if (forcePaymentFailure) {
        data["force_payment_failure"] = app::json::Value(true);
    }

    app::Event event;
    event.type = app::event_type::OrderRequest;
    event.data = std::move(data);
    return event;
}

std::string promptLine(const std::string& label, const std::string& defaultValue) {
    std::cout << label;
    if (!defaultValue.empty()) {
        std::cout << " [" << defaultValue << "]";
    }
    std::cout << ": " << std::flush;

    std::string line;
    if (!std::getline(std::cin, line)) {
        std::cout << "\n";
        return defaultValue;
    }
    return line.empty() ? defaultValue : line;
}

long long promptPositiveInt(const std::string& label, long long defaultValue) {
    while (true) {
        const std::string value = promptLine(label, std::to_string(defaultValue));
        try {
            std::size_t used = 0;
            const long long parsed = std::stoll(value, &used);
            if (used == value.size() && parsed > 0) {
                return parsed;
            }
        } catch (...) {
        }
        if (std::cin.eof()) {
            return defaultValue;
        }
        std::cout << "Please enter a positive integer.\n";
    }
}

double promptNonNegativeDouble(const std::string& label, double defaultValue) {
    while (true) {
        std::ostringstream fallback;
        fallback << defaultValue;
        const std::string value = promptLine(label, fallback.str());
        try {
            std::size_t used = 0;
            const double parsed = std::stod(value, &used);
            if (used == value.size() && parsed >= 0.0) {
                return parsed;
            }
        } catch (...) {
        }
        if (std::cin.eof()) {
            return defaultValue;
        }
        std::cout << "Please enter a non-negative number.\n";
    }
}

bool promptBool(const std::string& label, bool defaultValue) {
    const std::string hint = defaultValue ? "Y/n" : "y/N";
    const std::string value = promptLine(label + " (" + hint + ")", "");
    if (value.empty()) {
        return defaultValue;
    }
    const char c = static_cast<char>(std::tolower(static_cast<unsigned char>(value[0])));
    return c == 'y' || c == '1';
}

}  // namespace

int main(int argc, char** argv) {
    Options options = parseOptions(argc, argv);
    if (argc == 1 && isatty(STDIN_FILENO)) {
        options.interactive = true;
    }

    app::initLogging("order-cli");
    const app::Config config = app::Config::load();
    const std::string exchange = config.getString("RABBITMQ_EXCHANGE", "order.events");

    rmq::ConnectionOptions connectionOptions;
    connectionOptions.host = config.getString("RABBITMQ_HOST", "localhost");
    connectionOptions.port =
        static_cast<std::uint16_t>(config.getInt("RABBITMQ_PORT", 5672));
    connectionOptions.username = config.getString("RABBITMQ_DEFAULT_USER", "developer");
    connectionOptions.password = config.getString("RABBITMQ_DEFAULT_PASS", "developer123");
    connectionOptions.vhost = config.getString("RABBITMQ_VHOST", "/");
    connectionOptions.heartbeat_seconds =
        static_cast<std::uint16_t>(config.getInt("RABBITMQ_HEARTBEAT_SECONDS", 60));
    connectionOptions.auto_reconnect = config.getBool("RABBITMQ_AUTO_RECONNECT", true);

    try {
        rmq::Connection connection(connectionOptions);
        connection.setErrorCallback([](const rmq::Status& status) {
            LOG(ERROR) << "[rmq] " << rmq::to_string(status.code) << ": " << status.message
                       << "\n";
        });
        connection.start();
        if (!connection.waitConnected(10s)) {
            LOG(ERROR) << "cannot reach RabbitMQ at " << connectionOptions.host << ":"
                       << connectionOptions.port << "\n";
            return 1;
        }

        rmq::PublisherOptions publisherOptions;
        publisherOptions.exchange = exchange;
        publisherOptions.exchange_type = "topic";
        publisherOptions.declare_exchange = true;
        publisherOptions.durable = true;
        rmq::Publisher publisher(connection, publisherOptions);

        rmq::Message warmup;
        warmup.body = "{}";
        warmup.content_type = "application/json";
        publisher.publish("app.warmup", warmup, 5s);

        const auto submit = [&](const app::Event& event) {
            const rmq::Status status =
                publisher.publish(app::event_type::OrderRequest, event.toMessage(), 5s);
            if (!status) {
                LOG(ERROR) << "failed to submit order: " << status.message << "\n";
                return false;
            }
            LOG(INFO) << "submitted order request: "
                      << event.data["customer_id"].asString() << " product="
                      << event.data["items"][0]["product_id"].asString() << " qty="
                      << event.data["items"][0]["quantity"].asInt(0) << "\n";
            return true;
        };

        if (options.interactive) {
            std::cout << "\nInteractive order creator. Press Ctrl+D to exit.\n\n";
            while (true) {
                const std::string customerInput = promptLine("Customer ID (or 'auto')", "auto");
                const std::string customer =
                    (customerInput.empty() || customerInput == "auto")
                        ? app::generateId("C")
                        : customerInput;
                const std::string product = promptLine("Product ID", options.product);
                const long long quantity = promptPositiveInt("Quantity", options.quantity);
                const double price = promptNonNegativeDouble("Unit price", options.price);
                const bool forceFailure = promptBool("Force payment failure", false);

                if (!submit(makeRequest(customer, product, quantity, price, forceFailure))) {
                    return 1;
                }
                std::cout << "Order request submitted for customer " << customer
                          << " (the order service assigns the order ID; watch its logs "
                             "or the notification service).\n\n";

                if (std::cin.eof() || !promptBool("Create another order", true)) {
                    break;
                }
                std::cout << "\n";
            }
        } else if (options.demo) {
            submit(makeRequest(app::generateId("C"), options.product, 2, options.price, false));
            submit(makeRequest(app::generateId("C"), options.product, 1000, 1.0, false));
            submit(makeRequest(app::generateId("C"), options.product, 1, options.price, true));
        } else {
            for (int i = 0; i < options.count; ++i) {
                const std::string customer =
                    options.customer.empty() ? app::generateId("C") : options.customer;
                if (!submit(makeRequest(customer, options.product, options.quantity,
                                        options.price, options.forcePaymentFailure))) {
                    return 1;
                }
                if (i + 1 < options.count) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(options.intervalMs));
                }
            }
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "order_cli failed: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
