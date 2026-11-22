#include "app_common/logging.hpp"

#include "app_common/service.hpp"
#include "log.h"

#include <log4cxx/basicconfigurator.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <pthread.h>

log4cxx::LoggerPtr g_logger;

namespace app {

namespace {
sigset_t g_shutdownSignals;
bool g_signalsInstalled = false;
}  // namespace

void initLogging(const std::string& serviceName) {
    log4cxx::BasicConfigurator::configure();
    g_logger = log4cxx::Logger::getLogger(serviceName);
    if (g_logger == nullptr) {
        std::fprintf(stderr, "failed to initialise the logger\n");
        std::exit(1);
    }
    LOG(INFO) << serviceName << " starting\n";
}

void installSignalHandlers() {
    sigemptyset(&g_shutdownSignals);
    sigaddset(&g_shutdownSignals, SIGINT);
    sigaddset(&g_shutdownSignals, SIGTERM);
    sigaddset(&g_shutdownSignals, SIGHUP);
    if (pthread_sigmask(SIG_BLOCK, &g_shutdownSignals, nullptr) != 0) {
        LOG(ERROR) << "failed to block shutdown signals\n";
        return;
    }
    g_signalsInstalled = true;
}

void waitForShutdownSignal() {
    if (!g_signalsInstalled) {
        installSignalHandlers();
    }
    int signal = 0;
    if (sigwait(&g_shutdownSignals, &signal) != 0) {
        LOG(ERROR) << "sigwait failed\n";
        return;
    }
    LOG(INFO) << "received signal " << signal << ", shutting down\n";
}

int runService(Service& service) {
    initLogging(service.name());
    installSignalHandlers();

    try {
        if (!service.init()) {
            LOG(ERROR) << service.name() << " failed to initialise\n";
            return 1;
        }
        if (!service.start()) {
            LOG(ERROR) << service.name() << " failed to start\n";
            service.stop();
            return 1;
        }
        LOG(INFO) << service.name() << " is running. Press Ctrl+C to stop.\n";
        waitForShutdownSignal();
        service.stop();
    } catch (const std::exception& e) {
        LOG(ERROR) << service.name() << " stopped with an exception: " << e.what() << "\n";
        service.stop();
        return 1;
    }

    LOG(INFO) << service.name() << " stopped\n";
    return 0;
}

}  // namespace app
