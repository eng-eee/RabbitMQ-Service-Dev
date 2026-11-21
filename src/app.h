#pragma once

#include <condition_variable>
#include <thread>
#include "rabbit_mq_service.h"

namespace app {
 
class App {
public:
    void stop();
    bool init();
    bool start();    
private:
    void run();
    bool m_is_stopped = false;
    std::mutex m_mutex;
    std::condition_variable m_cv;
    std::thread m_runner;
    std::shared_ptr<rabbitmq_service::RabbitMQService> m_rabbitMQService;
    
};


} // namespace app