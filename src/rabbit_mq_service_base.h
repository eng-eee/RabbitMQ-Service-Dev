#include <string>

namespace rabbitmq_service {

class RabbitMQServiceBase {
public:
    RabbitMQServiceBase(const std::string& host, int port, const std::string& username, const std::string& password, const std::string&  vhost, int heartbeat_seconds, bool auto_reconnect)
        : host_(std::move(host)), port_(port), username_(std::move(username)), password_(std::move(password)), vhost_(std::move(vhost)), heartbeat_seconds_(heartbeat_seconds), auto_reconnect_(auto_reconnect) {}
    
    RabbitMQServiceBase(const std::string& host, int port, const std::string& username, const std::string& password, const std::string&  vhost, int heartbeat_seconds)
        : RabbitMQServiceBase(host, port, username, password, vhost, heartbeat_seconds, true) {}

    RabbitMQServiceBase(const RabbitMQServiceBase&) = delete;
    RabbitMQServiceBase& operator=(const RabbitMQServiceBase&) = delete;
    RabbitMQServiceBase(RabbitMQServiceBase&&) = delete;
    RabbitMQServiceBase& operator=(RabbitMQServiceBase&&) = delete;

    virtual std::string getHost() const { return host_; }
    virtual int getPort() const { return port_; }
    virtual std::string getUsername() const { return username_; }
    virtual std::string getPassword() const { return password_; }
    virtual std::string getVhost() const { return vhost_; }
    virtual int getHeartbeatSeconds() const { return heartbeat_seconds_; }
    virtual bool getAutoReconnect() const { return auto_reconnect_; }
    
    virtual ~RabbitMQServiceBase() = default;
private:
    std::string host_;
    int port_;
    std::string username_;
    std::string password_;
    std::string vhost_;
    int heartbeat_seconds_;
    bool auto_reconnect_;
};

} // namespace rabbitmq_service