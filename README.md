# Execution Wrapper

## Purpose

Wraps the execution of command lines, so that the service API is informed of the progress, and
so that multiple concurrent executions are prevented.

## Requirements

Python 3.6+

## Usage
  ```
  usage: exec_wrapper.py [-h] [--api-base-url API_BASE_URL] [--api-key API_KEY]
                         [--api-heartbeat-interval API_HEARTBEAT_INTERVAL]
                         [--api-retries API_RETRIES]
                         [--api-retries-for-final-update API_RETRIES_FOR_FINAL_UPDATE]
                         [--api-retries-for-process-creation-conflict API_RETRIES_FOR_PROCESS_CREATION_CONFLICT]
                         [--api-retry-delay API_RETRY_DELAY]
                         [--api-timeout API_TIMEOUT]
                         [--process-type-id PROCESS_TYPE_ID] [--service]
                         [--process-max-concurrency PROCESS_MAX_CONCURRENCY]
                         [--max-conflicting-age MAX_CONFLICTING_AGE]
                         [--log-level LOG_LEVEL] [--log-secrets]
                         [--work-dir WORK_DIR]
                         [--process-timeout PROCESS_TIMEOUT]
                         [--process-max-retries PROCESS_MAX_RETRIES]
                         [--process-retry-delay PROCESS_RETRY_DELAY]
                         [--process-check-interval PROCESS_CHECK_INTERVAL]
                         [--process-termination-grace-period PROCESS_TERMINATION_GRACE_PERIOD]
                         [--enable-status-update-listener]
                         [--status-update-socket-port STATUS_UPDATE_SOCKET_PORT]
                         [--status-update-message-max-bytes STATUS_UPDATE_MESSAGE_MAX_BYTES]
                         [--status-update-interval STATUS_UPDATE_INTERVAL]
                         [--process-name PROCESS_NAME] [--project PROJECT]
                         [--program-version PROGRAM_VERSION]
                         [--program-log-query PROGRAM_LOG_QUERY]
                         [--wrapper-version WRAPPER_VERSION]
                         [--process-cron-schedule PROCESS_CRON_SCHEDULE]
                         [--send-pid] [--send-hostname]
                         [--deployment DEPLOYMENT]
                         [--rollbar-access-token ROLLBAR_ACCESS_TOKEN]
                         [--rollbar-retries ROLLBAR_RETRIES]
                         [--rollbar-retry-delay ROLLBAR_RETRY_DELAY]
                         [--rollbar-timeout ROLLBAR_TIMEOUT]
                         ...
  
  positional arguments:
    command
  
  optional arguments:
    -h, --help            show this help message and exit
    --api-base-url API_BASE_URL
                          Base URL of API server
    --api-key API_KEY     API key
    --api-heartbeat-interval API_HEARTBEAT_INTERVAL
                          Number of seconds to wait between sending heartbeats
                          to the API server. -1 means to not send heartbeats.
                          Defaults to 30 for concurrency limited services, 600
                          otherwise.
    --api-retries API_RETRIES
                          Number of retries per API request. Defaults to 2.
    --api-retries-for-final-update API_RETRIES_FOR_FINAL_UPDATE
                          Number of retries for final process status update.
                          Defaults to infinity.
    --api-retries-for-process-creation-conflict API_RETRIES_FOR_PROCESS_CREATION_CONFLICT
                          Number of retries after a process creation conflict is
                          detected. Defaults to 4 for concurrency limited
                          services, 0 otherwise.
    --api-retry-delay API_RETRY_DELAY
                          Number of seconds to wait before retrying an API
                          request. Defaults to 600.
    --api-timeout API_TIMEOUT
                          Timeout for contacting API server, in seconds.
                          Defaults to 30.
    --process-type-id PROCESS_TYPE_ID
                          Process Type ID
    --service             Indicate that this is a process that should run
                          indefinitely
    --process-max-concurrency PROCESS_MAX_CONCURRENCY
                          Maximum number of concurrent processes allowed with
                          the same Process Type ID. Defaults to 1.
    --max-conflicting-age MAX_CONFLICTING_AGE
                          Maximum age of conflicting processes to consider, in
                          seconds. -1 means no limit. Defaults to the heartbeat
                          interval, plus 60 seconds for services that send
                          heartbeats. Otherwise, defaults to no limit.
    --log-level LOG_LEVEL
                          Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
                          Defaults to WARNING.
    --log-secrets         Log sensitive information
    --work-dir WORK_DIR   Working directory
    --process-timeout PROCESS_TIMEOUT
                          Timeout for process execution, in seconds. Defaults to
                          None for non-services, infinite for services. -1 means
                          no timeout
    --process-max-retries PROCESS_MAX_RETRIES
                          Maximum number of times to retry failed processes. -1
                          means to retry forever. Defaults to 0.
    --process-retry-delay PROCESS_RETRY_DELAY
                          Number of seconds to wait before retrying the process
                          execution. Defaults to 600.
    --process-check-interval PROCESS_CHECK_INTERVAL
                          Number of seconds to wait between checking the status
                          of processes. Defaults to 10.
    --process-termination-grace-period PROCESS_TERMINATION_GRACE_PERIOD
                          Number of seconds to wait after sending SIGTERM to a
                          process, but before killing it with SIGKILL. Defaults
                          to 30.
    --enable-status-update-listener
                          Listen for status updates from the process, sent on
                          the status socket port via UDP. If not specified,
                          status update messages will not be read.
    --status-update-socket-port STATUS_UPDATE_SOCKET_PORT
                          The port used to receive status updates from the
                          process. Defaults to 2048
    --status-update-message-max-bytes STATUS_UPDATE_MESSAGE_MAX_BYTES
                          The maximum number of bytes status update messages can
                          be. Defaults to 65536
    --status-update-interval STATUS_UPDATE_INTERVAL
                          Minimum of seconds to wait between sending status
                          updates to the API server. -1 means to not send status
                          updates except with heartbeats. Defaults to -1.
    --process-name PROCESS_NAME
                          Process Name (optional)
    --project PROJECT     Project Name (optional)
    --program-version PROGRAM_VERSION
                          Version of hash of the program's source code
                          (optional)
    --program-log-query PROGRAM_LOG_QUERY
                          Query to use to view program logs (optional)
    --wrapper-version WRAPPER_VERSION
                          Version of hash of the execution wrapper (optional)
    --process-cron-schedule PROCESS_CRON_SCHEDULE
                          Process Cron Schedule. Reports the schedule the task
                          runs (optional)
    --send-pid            Send the process ID to the API server
    --send-hostname       Send the hostname to the API server
    --deployment DEPLOYMENT
                          Deployment name (production, staging, etc.)
    --rollbar-access-token ROLLBAR_ACCESS_TOKEN
                          Access token for Rollbar (used to report error when
                          communicating with API server)
    --rollbar-retries ROLLBAR_RETRIES
                          Number of retries per Rollbar request. Defaults to 2.
    --rollbar-retry-delay ROLLBAR_RETRY_DELAY
                          Number of seconds to wait before retrying a Rollbar
                          request. Defaults to 600.
    --rollbar-timeout ROLLBAR_TIMEOUT
                          Timeout for contacting Rollbar server, in seconds.
                          Defaults to 30.
  ```
                          
Environment variables read (command-line arguments take precedence):

* EXEC_WRAPPER_PROCESS_TYPE_ID
* EXEC_WRAPPER_PROCESS_IS_SERVICE
* EXEC_WRAPPER_PROCESS_MAX_CONCURRENCY
* EXEC_WRAPPER_PROCESS_NAME
* EXEC_WRAPPER_PROJECT
* EXEC_WRAPPER_PROGRAM_VERSION
* EXEC_WRAPPER_PROGRAM_LOG_QUERY
* EXEC_WRAPPER_VERSION
* EXEC_WRAPPER_PROCESS_CRON_SCHEDULE
* EXEC_WRAPPER_LOG_LEVEL
* EXEC_WRAPPER_DEPLOYMENT
* EXEC_WRAPPER_API_BASE_URL
* EXEC_WRAPPER_API_KEY
* EXEC_WRAPPER_API_HEARTBEAT_INTERVAL
* EXEC_WRAPPER_API_RETRIES
* EXEC_WRAPPER_API_RETRIES_FOR_PROCESS_CREATION_CONFLICT
* EXEC_WRAPPER_API_RETRIES_FOR_FINAL_UPDATE
* EXEC_WRAPPER_API_RETRY_DELAY_SECONDS
* EXEC_WRAPPER_API_TIMEOUT_SECONDS
* EXEC_WRAPPER_SEND_PID
* EXEC_WRAPPER_SEND_HOSTNAME
* EXEC_WRAPPER_ROLLBAR_ACCESS_TOKEN
* EXEC_WRAPPER_ROLLBAR_TIMEOUT_SECONDS
* EXEC_WRAPPER_ROLLBAR_RETRIES
* EXEC_WRAPPER_ROLLBAR_RETRY_DELAY_SECONDS
* EXEC_WRAPPER_MAX_CONFLICTING_AGE_SECONDS
* EXEC_WRAPPER_WORK_DIR
* EXEC_WRAPPER_PROCESS_MAX_RETRIES
* EXEC_WRAPPER_PROCESS_TIMEOUT_SECONDS
* EXEC_WRAPPER_PROCESS_RETRY_DELAY_SECONDS
* EXEC_WRAPPER_PROCESS_CHECK_INTERVAL_SECONDS
* EXEC_WRAPPER_PROCESS_TERMINATION_GRACE_PERIOD_SECONDS
* EXEC_WRAPPER_STATUS_UPDATE_SOCKET_PORT
* EXEC_WRAPPER_STATUS_UPDATE_MESSAGE_MAX_BYTES

The command is executed with the same environment that the wrapper script gets,
except that these properties are copied/overridden:

* EXEC_WRAPPER_DEPLOYMENT
* EXEC_WRAPPER_API_BASE_URL
* EXEC_WRAPPER_API_KEY
* EXEC_WRAPPER_API_RETRIES
* EXEC_WRAPPER_API_RETRY_DELAY_SECONDS
* EXEC_WRAPPER_API_TIMEOUT_SECONDS
* EXEC_WRAPPER_ROLLBAR_ACCESS_TOKEN
* EXEC_WRAPPER_ROLLBAR_TIMEOUT_SECONDS
* EXEC_WRAPPER_ROLLBAR_RETRIES
* EXEC_WRAPPER_ROLLBAR_RETRY_DELAY_SECONDS
* EXEC_WRAPPER_PROCESS_EXECUTION_ID
* EXEC_WRAPPER_PROCESS_TYPE_ID
* EXEC_WRAPPER_PROCESS_NAME
* EXEC_WRAPPER_PROCESS_CRON_SCHEDULE
* EXEC_WRAPPER_PROCESS_TIMEOUT_SECONDS
* EXEC_WRAPPER_PROCESS_MAX_CONCURRENCY
* EXEC_WRAPPER_PROCESS_TERMINATION_GRACE_PERIOD_SECONDS
* EXEC_WRAPPER_ENABLE_STATUS_UPDATE_LISTENER
* EXEC_WRAPPER_STATUS_UPDATE_SOCKET_PORT
* EXEC_WRAPPER_STATUS_UPDATE_INTERVAL_SECONDS
* EXEC_WRAPPER_STATUS_UPDATE_MESSAGE_MAX_BYTES

This script is open-source with an Apache license. Contributions are welcome!
