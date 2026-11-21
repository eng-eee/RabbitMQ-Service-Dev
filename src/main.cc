#include "log.h"
#include "app.h"

#include <log4cxx/basicconfigurator.h>

#include <iostream>
#include <csignal>
#include <exception>

// Global logger instance
log4cxx::LoggerPtr g_logger;

// Initialize logger, signal handling, and run the application.
void InitLogger();
// Initialize signal handling for the specified signals.
bool InitSignalHandling(sigset_t& signals, std::initializer_list<int> signal_list);
// Run the application and wait for signals to stop it.
int RunApp(app::App& app, sigset_t& signals);


int main()
{
    // Initialize logger
    InitLogger();

    // Block SIGINT before creating application threads.
    sigset_t signals;
    if (!InitSignalHandling(signals, {SIGINT, SIGTERM, SIGHUP}))
    {
        LOG(ERROR) << "Failed to initialize signal handling.";
        return 1;
    }

    app::App app;
    int exit_code = RunApp(app, signals);


    LOG(INFO) << "Application stopped successfully. Terminating...";

    return exit_code;
}


int RunApp(app::App& app, sigset_t& signals)
{
    try
    {
        if (!app.init())
        {
            LOG(ERROR) << "Failed to initialize application.";
            return 1;
        }

        if (!app.start())
        {
            LOG(ERROR) << "Failed to run application.";
            app.stop();
            return 1;
        }

        LOG(INFO) << "Application started. Press Ctrl+C to stop.";

        // Wait synchronously for SIGINT.
        int signal = 0;
        if (sigwait(&signals, &signal) != 0)
        {
            LOG(ERROR) << "Failed to wait for signal.";
            app.stop();
            return 1;
        }

        LOG(INFO) << "SIGINT received. Stopping application...";

        app.stop();
    }
    catch (const std::exception& e)
    {
        LOG(ERROR) << "Exception occurred: " << e.what();
        app.stop();
        return 1;
    }
    return 0;
}

void InitLogger()
{
    log4cxx::BasicConfigurator::configure();
    g_logger = log4cxx::Logger::getLogger("App");
    if (g_logger == nullptr)
    {
        std::cerr << "Failed to initialize logger." << std::endl;
        std::exit(1);
    }
}

bool InitSignalHandling(sigset_t& signals, std::initializer_list<int> signal_list)
{
    sigemptyset(&signals);
    for (int sig : signal_list)
    {
        sigaddset(&signals, sig);
    }

    if (pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0)
    {
        LOG(ERROR) << "Failed to block signals.";
        return false;
    }
    return true;
}