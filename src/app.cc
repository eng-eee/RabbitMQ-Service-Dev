#include "app.h"
#include <log.h>
#include <mutex>
#include "rabbit_mq_service.h"
#include <condition_variable>
#include "config_param_helper.h"

namespace app {
 

void App::stop(){
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_is_stopped = true;
    }
    
    m_cv.notify_all();
    m_rabbitMQService->stop();
    m_runner.join();
}

bool App::init(){
    std::shared_ptr<configuration::ParameterReader> paramReader = std::make_shared<configuration::ParameterReader>();
    if (!paramReader->Initialize("app.conf")) {
        LOG(ERROR) << "Failed to initialize configuration parameters from app.conf\n";
        return false;
    }

    m_rabbitMQService = std::make_shared<rabbitmq_service::RabbitMQService>(paramReader);

    return m_rabbitMQService->init();
}

bool App::start(){
    m_runner = std::thread(&App::run, this);
    return m_runner.joinable();
}

void App::run(){
    while(!m_is_stopped){
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cv.wait_for(lock, std::chrono::seconds(1), [this] { return m_is_stopped; });
        LOG(INFO) << "Application is running...";
        // Just for testing, publish a message to RabbitMQ every second
        m_rabbitMQService->PublishMessage("test_queue", "routing_key" , "Hello, RabbitMQ!");
    }
    LOG(INFO) << "Application is stopping...";
}


} // namespace app