#pragma once

#include <string>

namespace app {

class Service;

/// Initialises log4cxx and the global logger used by the LOG() macro.
void initLogging(const std::string& serviceName);

/// Blocks SIGINT/SIGTERM/SIGHUP in the calling thread. Call this before
/// starting any thread so the signals are delivered to the waiter below.
void installSignalHandlers();

/// Blocks until one of the shutdown signals arrives.
void waitForShutdownSignal();

/// Standard executable entry point: logging, signals, init, start, wait, stop.
int runService(Service& service);

}  // namespace app
