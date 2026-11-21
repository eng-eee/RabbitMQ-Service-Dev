#pragma once
#include "log.h"
#include <rmq/rmq.hpp>
#include "rabbit_mq_service_base.h"
#include "config_param_helper.h"
#include <string>

namespace rabbitmq_service {

class RabbitMQService: public RabbitMQServiceBase {
public:
    RabbitMQService(const std::string& host, int port, const std::string& username, const std::string& password, const std::string&  vhost, int heartbeat_seconds, bool auto_reconnect)
        : RabbitMQServiceBase(host, port, username, password, vhost, heartbeat_seconds, auto_reconnect) {}
    
    RabbitMQService(const std::string& host, int port, const std::string& username, const std::string& password, const std::string&  vhost, int heartbeat_seconds)
        : RabbitMQServiceBase(host, port, username, password, vhost, heartbeat_seconds, true) {}

    RabbitMQService(const std::shared_ptr<configuration::ParameterReader>& paramReader)
        : RabbitMQServiceBase(
            paramReader->getParameter("HOST").value_or("localhost"),
            std::stoi(paramReader->getParameter("RABBITMQ_PORT").value_or("5672")),
            paramReader->getParameter("RABBITMQ_DEFAULT_USER").value_or("developer"),
            paramReader->getParameter("RABBITMQ_DEFAULT_PASS").value_or("developer123"),
            paramReader->getParameter("VHOST").value_or("/"),
            std::stoi(paramReader->getParameter("HEARTBEAT_SECONDS").value_or("60")),
            paramReader->getParameter("AUTO_RECONNECT").value_or("true") == "true"
        ) {}

    RabbitMQService(const RabbitMQService&) = delete;
    RabbitMQService& operator=(const RabbitMQService&) = delete;
    RabbitMQService(RabbitMQService&&) = delete;
    RabbitMQService& operator=(RabbitMQService&&) = delete;
    ~RabbitMQService() override = default;
    
    bool init() {
        rmq::ConnectionOptions opts;
        opts.host     = getHost();
        opts.port     = getPort();
        opts.username = getUsername();
        opts.password = getPassword();
        opts.vhost    = getVhost();
        opts.heartbeat_seconds = getHeartbeatSeconds();
        opts.auto_reconnect    = getAutoReconnect();

        try {
            m_conn = std::make_shared<rmq::Connection>(opts);                 // validates options, throws rmq::Error
        
            // Both callbacks run on the library's IO thread: keep them short, never block.
            m_conn->setStateCallback([](rmq::ConnectionState s, const std::string& why) {
                LOG(INFO) << "[rmq] " << rmq::to_string(s) << " - " << why << "\n";
            });
            m_conn->setErrorCallback([](const rmq::Status& st) {
                LOG(ERROR) << "[rmq] " << rmq::to_string(st.code) << ": " << st.message << "\n";
            });
        
            m_conn->start();                               // spawns the IO thread, returns immediately
        
            if (!m_conn->waitConnected(std::chrono::seconds(10))) {
                LOG(ERROR) << "broker not reachable yet, library keeps retrying in background\n";
            }
        
        } catch (const rmq::Error& e) {
            LOG(ERROR) << "rmq error (" << rmq::to_string(e.code()) << "): " << e.what() << "\n";
            return 1;
        }
        return m_conn->isConnected();
    }
    

    void PublishMessage(const std::string& exchange, const std::string& routing_key, const std::string& message) {
        rmq::Publisher pub(*m_conn);
        rmq::Message msg;
        msg.body = "hello";
        msg.content_type = "text/plain";
        const rmq::Status st = pub.publish("app.events", std::move(msg));   // blocks until the broker confirmed
        if (!st) LOG(ERROR) << "publish failed: " << st.message << "\n";

        std::this_thread::sleep_for(std::chrono::milliseconds(100));  // give the IO thread a chance to run the Completion callback
    }

    bool stop() {
        if (m_conn) {
            m_conn->stop();                                // graceful close + join (destructor does this too)
        }
        return true;
    }
    
private:
    std::shared_ptr<rmq::Connection> m_conn;
};

} // namespace rabbitmq_service