#!/usr/local/bin/python

import logging
import math
import argparse
import signal
from subprocess import Popen, TimeoutExpired
import os
import socket
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from urllib.parse import quote_plus
from http import HTTPStatus
import json
import time
import atexit

_DEFAULT_LOG_LEVEL = 'WARNING'

HEARTBEAT_DELAY_TOLERANCE_SECONDS = 60

DEFAULT_API_HEARTBEAT_INTERVAL_SECONDS = 600
DEFAULT_API_CONCURRENCY_LIMITED_SERVICE_HEARTBEAT_INTERVAL_SECONDS = 30
DEFAULT_API_TIMEOUT_SECONDS = 30
DEFAULT_API_RETRIES = 2
DEFAULT_API_RETRIES_FOR_FINAL_UPDATE = math.inf
DEFAULT_API_CONCURRENCY_LIMITED_SERVICE_CONFLICT_RETRIES = 4
DEFAULT_API_RETRY_DELAY_SECONDS = 600

DEFAULT_STATUS_UPDATE_SOCKET_PORT = 2048

DEFAULT_ROLLBAR_TIMEOUT_SECONDS = 30
DEFAULT_ROLLBAR_RETRIES = 2
DEFAULT_ROLLBAR_RETRY_DELAY_SECONDS = 600

DEFAULT_PROCESS_NAME = '[Unnamed Process]'
DEFAULT_PROCESS_TIMEOUT_SECONDS = None
DEFAULT_PROCESS_CHECK_INTERVAL_SECONDS = 10
DEFAULT_PROCESS_RETRY_DELAY_SECONDS = 600
DEFAULT_PROCESS_TERMINATION_GRACE_PERIOD_SECONDS = 30

DEFAULT_STATUS_UPDATE_MESSAGE_MAX_BYTES = 64 * 1024

def _exit_handler(wrapper):
    # Prevent re-entrancy and changing of the exit code
    atexit.unregister(_exit_handler)
    wrapper._handle_exit()


def _signal_handler(signum, frame):
    # This will cause the exit handler to be executed, if it is registered.
    raise RuntimeError('Caught SIGTERM, exiting.')


class ExecWrapper:
    VERSION = '1.2.2'

    STATUS_RUNNING = 'running'
    STATUS_SUCCEEDED = 'succeeded'
    STATUS_FAILED = 'failed'
    STATUS_TERMINATED_AFTER_TIME_OUT = 'terminated_after_time_out'

    _ALLOWED_FINAL_STATUSES =  set([
      STATUS_SUCCEEDED, STATUS_FAILED, STATUS_TERMINATED_AFTER_TIME_OUT
    ])

    _EXIT_CODE_SUCCESS = 0
    _EXIT_CODE_GENERIC_ERROR = 1
    _EXIT_CODE_CONFIGURATION_ERROR = 78

    _RESPONSE_CODE_TO_EXIT_CODE = {
        409: 75,  # temp failure
        403: 77  # permission denied
    }

    _RETRYABLE_HTTP_STATUS_CODES = set([
        HTTPStatus.SERVICE_UNAVAILABLE.value, HTTPStatus.BAD_GATEWAY.value
    ])

    _STATUS_BUFFER_SIZE = 4096

    @staticmethod
    def make_arg_parser(require_command=True):
        parser = argparse.ArgumentParser()

        if require_command:
            parser.add_argument('command', nargs=argparse.REMAINDER)

        parser.add_argument('--api-base-url', help='Base URL of API server')
        parser.add_argument('--api-key', help='API key')
        parser.add_argument('--api-heartbeat-interval',
                            help=f"Number of seconds to wait between sending heartbeats to the API server. -1 means to not send heartbeats. Defaults to {DEFAULT_API_CONCURRENCY_LIMITED_SERVICE_HEARTBEAT_INTERVAL_SECONDS} for concurrency limited services, {DEFAULT_API_HEARTBEAT_INTERVAL_SECONDS} otherwise.")
        parser.add_argument('--api-retries',
                            help=f"Number of retries per API request. Defaults to {DEFAULT_API_RETRIES}.")
        parser.add_argument('--api-retries-for-final-update',
                            help=f"Number of retries for final process status update. Defaults to infinity.")
        parser.add_argument('--api-retries-for-process-creation-conflict',
                            help=f"Number of retries after a process creation conflict is detected. Defaults to {DEFAULT_API_CONCURRENCY_LIMITED_SERVICE_CONFLICT_RETRIES} for concurrency limited services, 0 otherwise.")
        parser.add_argument('--api-retry-delay',
                            help=f"Number of seconds to wait before retrying an API request. Defaults to {DEFAULT_API_RETRY_DELAY_SECONDS}.")
        parser.add_argument('--api-timeout',
                            help=f"Timeout for contacting API server, in seconds. Defaults to {DEFAULT_API_TIMEOUT_SECONDS}.")
        parser.add_argument('--process-type-id', help='Process Type ID')
        parser.add_argument('--service', action='store_true',
                            help='Indicate that this is a process that should run indefinitely')
        parser.add_argument('--process-max-concurrency',
                            help='Maximum number of concurrent processes allowed with the same Process Type ID. Defaults to 1.')
        parser.add_argument('--max-conflicting-age',
                            help=f"Maximum age of conflicting processes to consider, in seconds. -1 means no limit. Defaults to the heartbeat interval, plus {HEARTBEAT_DELAY_TOLERANCE_SECONDS} seconds for services that send heartbeats. Otherwise, defaults to no limit.")
        parser.add_argument('--log-level',
                            help=f"Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Defaults to {_DEFAULT_LOG_LEVEL}.")
        parser.add_argument('--log-secrets', action='store_true', help='Log sensitive information')
        parser.add_argument('--work-dir', help='Working directory')
        parser.add_argument('--process-timeout',
                            help=f"Timeout for process execution, in seconds. Defaults to {DEFAULT_PROCESS_TIMEOUT_SECONDS} for non-services, infinite for services. -1 means no timeout")
        parser.add_argument('--process-max-retries',
                            help='Maximum number of times to retry failed processes. -1 means to retry forever. Defaults to 0.')
        parser.add_argument('--process-retry-delay',
                            help=f"Number of seconds to wait before retrying the process execution. Defaults to {DEFAULT_PROCESS_RETRY_DELAY_SECONDS}.")
        parser.add_argument('--process-check-interval',
                            help=f"Number of seconds to wait between checking the status of processes. Defaults to {DEFAULT_PROCESS_CHECK_INTERVAL_SECONDS}.")
        parser.add_argument('--process-termination-grace-period',
                            help=f"Number of seconds to wait after sending SIGTERM to a process, but before killing it with SIGKILL. Defaults to {DEFAULT_PROCESS_TERMINATION_GRACE_PERIOD_SECONDS}.")
        parser.add_argument('--enable-status-update-listener', action='store_true',
                            help='Listen for status updates from the process, sent on the status socket port via UDP. If not specified, status update messages will not be read.')
        parser.add_argument('--status-update-socket-port',
                            help=f"The port used to receive status updates from the process. Defaults to {DEFAULT_STATUS_UPDATE_SOCKET_PORT}")
        parser.add_argument('--status-update-message-max-bytes',
                            help=f"The maximum number of bytes status update messages can be. Defaults to {DEFAULT_STATUS_UPDATE_MESSAGE_MAX_BYTES}")
        parser.add_argument('--status-update-interval',
                            help=f"Minimum of seconds to wait between sending status updates to the API server. -1 means to not send status updates except with heartbeats. Defaults to -1.")
        parser.add_argument('--process-name', help='Process Name (optional)')
        parser.add_argument('--project', help='Project Name (optional)')
        parser.add_argument('--program-version', help="Version of hash of the program's source code (optional)")
        parser.add_argument('--program-log-query', help="Query to use to view program logs (optional)")
        parser.add_argument('--wrapper-version', help="Version of hash of the execution wrapper (optional)")
        parser.add_argument('--process-cron-schedule',
                            help='Process Cron Schedule. Reports the schedule the task runs (optional)')
        parser.add_argument('--send-pid', action='store_true', help='Send the process ID to the API server')
        parser.add_argument('--send-hostname', action='store_true', help='Send the hostname to the API server')
        parser.add_argument('--deployment', help='Deployment name (production, staging, etc.)')
        parser.add_argument('--rollbar-access-token',
                            help='Access token for Rollbar (used to report error when communicating with API server)')
        parser.add_argument('--rollbar-retries',
                            help=f"Number of retries per Rollbar request. Defaults to {DEFAULT_ROLLBAR_RETRIES}.")
        parser.add_argument('--rollbar-retry-delay',
                            help=f"Number of seconds to wait before retrying a Rollbar request. Defaults to {DEFAULT_ROLLBAR_RETRY_DELAY_SECONDS}.")
        parser.add_argument('--rollbar-timeout',
                            help=f"Timeout for contacting Rollbar server, in seconds. Defaults to {DEFAULT_ROLLBAR_TIMEOUT_SECONDS}.")
        return parser

    @staticmethod
    def make_default_args():
        return ExecWrapper.make_arg_parser(require_command=False).parse_args([])

    def __init__(self, args=None, embedded_mode=True):
        self._logger = logging.getLogger(__name__)
        self._logger.addHandler(logging.NullHandler())

        self.id = None
        self.was_conflict = False
        self.called_exit = False
        self.attempt_count = 0
        self.timeout_count = 0
        self.process = None
        self.timed_out = False
        self.api_server_retries_exhausted = False
        self._embedded_mode = embedded_mode
        self.command = None
        self.status_update_socket_port = None
        self._status_socket = None
        self._status_buffer = None
        self._status_message_so_far = None
        self.status_dict = {}
        self.status_update_interval = None
        self.status_update_message_max_bytes = None
        self.last_update_sent_at = None

        if not args:
            args = ExecWrapper.make_default_args()

        self.rollbar_access_token = args.rollbar_access_token or os.environ.get('EXEC_WRAPPER_ROLLBAR_ACCESS_TOKEN')
        rollbar_retries = args.api_retries or os.environ.get('EXEC_WRAPPER_ROLLBAR_RETRIES', DEFAULT_ROLLBAR_RETRIES)
        self.rollbar_retries = int(rollbar_retries)

        rollbar_retry_delay = args.api_retry_delay or os.environ.get('EXEC_WRAPPER_ROLLBAR_RETRY_DELAY_SECONDS',
                                                                     DEFAULT_ROLLBAR_RETRY_DELAY_SECONDS)
        self.rollbar_retry_delay = int(rollbar_retry_delay)

        rollbar_timeout = args.api_timeout or os.environ.get('EXEC_WRAPPER_ROLLBAR_TIMEOUT_SECONDS',
                                                             DEFAULT_ROLLBAR_TIMEOUT_SECONDS)
        self.rollbar_timeout = int(rollbar_timeout)

        self.rollbar_retries_exhausted = False

        process_type_id = args.process_type_id or os.environ.get('EXEC_WRAPPER_PROCESS_TYPE_ID')

        if not process_type_id:
            logging.critical("No process type ID specified, exiting.")
            self._exit_or_raise(self._EXIT_CODE_CONFIGURATION_ERROR)

        self.process_type_id = int(process_type_id)

        self.log_secrets = args.log_secrets or (
                    os.environ.get('EXEC_WRAPPER_API_LOG_SECRETS', 'false').lower() == 'true')
        self.process_is_service = args.service or (
                    os.environ.get('EXEC_WRAPPER_PROCESS_IS_SERVICE', 'false').lower() == 'true')
        process_max_concurrency = args.process_max_concurrency or os.environ.get('EXEC_WRAPPER_PROCESS_MAX_CONCURRENCY')
        process_max_retries = args.process_max_retries or os.environ.get('EXEC_WRAPPER_PROCESS_MAX_RETRIES')
        self.process_termination_grace_period_seconds = int(args.process_termination_grace_period or os.environ.get(
            'EXEC_WRAPPER_PROCESS_TERMINATION_GRACE_PERIOD_SECONDS') or DEFAULT_PROCESS_TERMINATION_GRACE_PERIOD_SECONDS)
        self.process_name = args.process_name or os.environ.get('EXEC_WRAPPER_PROCESS_NAME', DEFAULT_PROCESS_NAME)
        self.project = args.project or os.environ.get('EXEC_WRAPPER_PROJECT')
        self.program_version = args.program_version or os.environ.get('EXEC_WRAPPER_PROGRAM_VERSION')
        self.program_log_query = args.program_log_query or os.environ.get('EXEC_WRAPPER_PROGRAM_LOG_QUERY')
        self.wrapper_version = args.wrapper_version or os.environ.get('EXEC_WRAPPER_VERSION') or ExecWrapper.VERSION
        self.process_cron_schedule = args.process_cron_schedule or os.environ.get('EXEC_WRAPPER_PROCESS_CRON_SCHEDULE')

        api_base_url = args.api_base_url or os.environ.get('EXEC_WRAPPER_API_BASE_URL')
        if not api_base_url:
            logging.critical("No API server host specified, exiting.")
            self._exit_or_raise(self._EXIT_CODE_CONFIGURATION_ERROR)

        self.api_base_url = api_base_url.rstrip('/')

        self.api_key = args.api_key or os.environ.get('EXEC_WRAPPER_API_KEY')
        if not self.api_key:
            logging.critical("No API key specified, exiting.")
            self._exit_or_raise(self._EXIT_CODE_CONFIGURATION_ERROR)

        self.deployment = args.deployment or os.environ.get('EXEC_WRAPPER_DEPLOYMENT')
        if not self.deployment:
            logging.critical("No deployment specified, exiting.")
            exit(self._EXIT_CODE_CONFIGURATION_ERROR)

        max_conflicting_age_seconds = args.max_conflicting_age or os.environ.get(
            'EXEC_WRAPPER_MAX_CONFLICTING_AGE_SECONDS')

        self.send_pid = args.send_pid or (os.environ.get('EXEC_WRAPPER_SEND_PID', 'false').lower() == 'true')
        send_hostname = args.send_hostname or (os.environ.get('EXEC_WRAPPER_SEND_HOSTNAME', 'false').lower() == 'true')

        self.hostname = None
        if send_hostname:
            try:
                self.hostname = socket.gethostname()
                self._logger.debug(f"Hostname = '{self.hostname}'")
            except:
                self._logger.warning("Can't get hostname")

        api_retries = args.api_retries or os.environ.get('EXEC_WRAPPER_API_RETRIES')
        self.api_retries = ExecWrapper._string_to_int(api_retries,
                                                      default_value=DEFAULT_API_RETRIES, negative_value=math.inf)

        api_retry_delay = args.api_retry_delay or os.environ.get('EXEC_WRAPPER_API_RETRY_DELAY_SECONDS')
        self.api_retry_delay = ExecWrapper._string_to_int(api_retry_delay,
                                                          default_value=DEFAULT_API_RETRY_DELAY_SECONDS,
                                                          negative_value=0)

        api_timeout = args.api_timeout or os.environ.get('EXEC_WRAPPER_API_TIMEOUT_SECONDS')
        self.api_timeout = ExecWrapper._string_to_int(api_timeout, default_value=DEFAULT_API_TIMEOUT_SECONDS,
                                                      negative_value=math.inf)

        self.last_api_request_data = None

        # Now we have enough info to try to send errors if problems happen below.

        if not self._embedded_mode:
            atexit.register(_exit_handler, self)

            # The function registered with atexit.register() isn't called when python receives
            # most signals, include SIGTERM. So register a signal handler which will cause the
            # program to exit with an error, triggering the exit handler.
            signal.signal(signal.SIGTERM, _signal_handler)

        env_process_timeout_seconds = os.environ.get('EXEC_WRAPPER_PROCESS_TIMEOUT_SECONDS')

        process_timeout = None
        if not self.process_is_service:
            process_timeout = args.process_timeout or env_process_timeout_seconds or DEFAULT_PROCESS_TIMEOUT_SECONDS

        process_retry_delay = args.process_retry_delay or os.environ.get('EXEC_WRAPPER_PROCESS_RETRY_DELAY_SECONDS')

        if self.process_is_service:
            if args.process_timeout and (int(args.process_timeout) > 0):
                self._logger.warning('Ignoring argument --process-timeout since process is a service')

            if env_process_timeout_seconds and (int(env_process_timeout_seconds) > 0):
                self._logger.warning(
                    'Ignoring environment variable EXEC_WRAPPER_PROCESS_TIMEOUT_SECONDS since process is a service')

        self.process_timeout = ExecWrapper._string_to_int(process_timeout, default_value=None, negative_value=None)
        self.process_max_concurrency = ExecWrapper._string_to_int(
            process_max_concurrency, default_value=1, negative_value=None)
        self.process_max_retries = ExecWrapper._string_to_int(
            process_max_retries, default_value=0, negative_value=math.inf)

        self.process_retry_delay = ExecWrapper._string_to_int(process_retry_delay,
                                                              default_value=DEFAULT_PROCESS_RETRY_DELAY_SECONDS,
                                                              negative_value=0)

        self.is_concurrency_limited_service = self.process_is_service and (self.process_max_concurrency is not None)

        api_heartbeat_interval = args.api_heartbeat_interval or \
                                 os.environ.get('EXEC_WRAPPER_API_HEARTBEAT_INTERVAL_SECONDS')
        api_retries_for_final_update = args.api_retries_for_final_update or \
                                       os.environ.get('EXEC_WRAPPER_API_RETRIES_FOR_FINAL_UPDATE')
        api_retries_for_process_creation_conflict = args.api_retries_for_process_creation_conflict or \
            os.environ.get('EXEC_WRAPPER_API_RETRIES_FOR_PROCESS_CREATION_CONFLICT',
            DEFAULT_API_CONCURRENCY_LIMITED_SERVICE_CONFLICT_RETRIES)

        default_heartbeat_interval = DEFAULT_API_HEARTBEAT_INTERVAL_SECONDS

        if self.is_concurrency_limited_service:
            default_heartbeat_interval = DEFAULT_API_CONCURRENCY_LIMITED_SERVICE_HEARTBEAT_INTERVAL_SECONDS

        self.api_heartbeat_interval = ExecWrapper._string_to_int(api_heartbeat_interval,
                                                                 default_value=default_heartbeat_interval,
                                                                 negative_value=None)
        self.api_retries_for_final_update = ExecWrapper._string_to_int(api_retries_for_final_update,
                                                                       default_value=DEFAULT_API_RETRIES_FOR_FINAL_UPDATE,
                                                                       negative_value=math.inf)
        self.api_retries_for_process_creation_conflict = ExecWrapper._string_to_int(
            api_retries_for_process_creation_conflict,
            default_value=DEFAULT_API_CONCURRENCY_LIMITED_SERVICE_CONFLICT_RETRIES,
            negative_value=math.inf)

        default_max_conflicting_age_seconds = None

        if self.process_is_service and self.api_heartbeat_interval:
            default_max_conflicting_age_seconds = self.api_heartbeat_interval + HEARTBEAT_DELAY_TOLERANCE_SECONDS

        self.max_conflicting_age_seconds = ExecWrapper._string_to_int(max_conflicting_age_seconds,
                                                                      default_value=default_max_conflicting_age_seconds,
                                                                      negative_value=None)

        status_update_interval = args.status_update_interval or \
                                 os.environ.get('EXEC_WRAPPER_STATUS_UPDATE_INTERVAL_SECONDS')
        self.status_update_interval = ExecWrapper._string_to_int(status_update_interval,
                                                                 negative_value=None)

        if self._embedded_mode:
            self.process_check_interval = None
        else:
            process_check_interval = args.process_check_interval or os.environ.get(
                'EXEC_WRAPPER_PROCESS_CHECK_INTERVAL_SECONDS')

            self.process_check_interval = ExecWrapper._string_to_int(process_check_interval,
                                                                     default_value=DEFAULT_PROCESS_CHECK_INTERVAL_SECONDS,
                                                                     negative_value=-1)
            if self.process_check_interval <= 0:
                logging.critical(f"Process check interval {self.process_check_interval} must be positive.")
                self._exit_or_raise(self._EXIT_CODE_CONFIGURATION_ERROR)

            if not args.command:
                logging.critical('Command expected in wrapped mode, but not found')
                self._exit_or_raise(self._EXIT_CODE_CONFIGURATION_ERROR)

            self.command = args.command
            self.working_dir = args.work_dir or os.environ.get('EXEC_WRAPPER_WORK_DIR')

            enable_status_update_listener = args.enable_status_update_listener or \
                (os.environ.get('EXEC_WRAPPER_ENABLE_STATUS_UPDATE_LISTENER', 'FALSE').upper() == 'TRUE')
            if enable_status_update_listener:
                status_update_socket_port = args.status_update_socket_port or os.environ.get(
                    'EXEC_WRAPPER_STATUS_UPDATE_SOCKET_PORT')
                self.status_update_socket_port = ExecWrapper._string_to_int(
                    status_update_socket_port,
                    default_value=DEFAULT_STATUS_UPDATE_SOCKET_PORT)
                self._status_buffer = bytearray(self._STATUS_BUFFER_SIZE)
                self._status_message_so_far = bytearray()
                status_update_message_max_bytes = args.status_update_message_max_bytes or \
                    os.environ.get('EXEC_WRAPPER_STATUS_UPDATE_MESSAGE_MAX_BYTES')
                self.status_update_message_max_bytes = ExecWrapper._string_to_int(
                    status_update_message_max_bytes, default_value=DEFAULT_STATUS_UPDATE_MESSAGE_MAX_BYTES)

        self.log_configuration()

    def log_configuration(self):
        self._logger.info(f"Deployment = '{self.deployment}'")
        self._logger.debug(f"API base URL = '{self.api_base_url}'")

        if self.log_secrets:
            self._logger.debug(f"API key = '{self.api_key}'")

        self._logger.debug(f"API heartbeat interval in seconds = {self.api_heartbeat_interval}")
        self._logger.debug(f"API timeout = {self.api_timeout}")
        self._logger.debug(f"API retries = {self.api_retries}")
        self._logger.debug(f"API retries for final update = {self.api_retries_for_final_update}")
        self._logger.debug(f"API retries for process creation conflict = {self.api_retries_for_process_creation_conflict}")
        self._logger.debug(f"API retry delay = {self.api_retry_delay}")

        if self.log_secrets:
            self._logger.debug(f"Rollbar API key = '{self.rollbar_access_token}'")

        self._logger.debug(f"Rollbar timeout = {self.rollbar_timeout}")
        self._logger.debug(f"Rollbar retries = {self.rollbar_retries}")
        self._logger.debug(f"Rollbar retry delay = {self.rollbar_retry_delay}")

        self._logger.debug(f"Process name = {self.process_name}")
        self._logger.debug(f"Project = {self.project}")
        self._logger.debug(f"Program version = {self.program_version}")
        self._logger.debug(f"Wrapper version = {self.wrapper_version}")
        self._logger.debug(f"Process cron schedule = {self.process_cron_schedule}")
        self._logger.debug(f"Process type = {self.process_type_id}")
        self._logger.debug(f"Process is a service = {self.process_is_service}")
        self._logger.debug(f"Process max concurrency = {self.process_max_concurrency}")
        self._logger.debug(f"Process retries = {self.process_max_retries}")
        self._logger.debug(f"Process retry delay = {self.process_retry_delay}")
        self._logger.debug(f"Process check interval = {self.process_check_interval}")

        self._logger.debug(f"Maximum age of conflicting processes in seconds = {self.max_conflicting_age_seconds}")

        if not self._embedded_mode:
            self._logger.info(f"Command = {self.command}")
            self._logger.info(f"Work dir = '{self.working_dir}'")

            enable_status_update_listener = (self.status_update_socket_port is not None)
            self._logger.debug(f"Enable status listener = {enable_status_update_listener}")

            if enable_status_update_listener:
                self._logger.debug(f"Status socket port = {self.status_update_socket_port}")
                self._logger.debug(f"Status update message max bytes = {self.status_update_message_max_bytes}")

        self._logger.debug(f"Status update interval = {self.status_update_interval}")


    def request_process_start_if_max_concurrency_ok(self):
        """
        Make a request to the process management server to start an instance
        of this process type. Retry and wait between requests if so configured
        and the maximum concurrency has already been reached.
        """
        url = f"{self.api_base_url}/v1/process_executions"
        headers = self._make_headers()

        body = {
            'process_type_id': self.process_type_id,
            'process_name': self.process_name,
            'project': self.project,
            'program_version': self.program_version,
            'wrapper_version': self.wrapper_version,
            'log_query': self.program_log_query,
            'process_cron_schedule': self.process_cron_schedule,
            'process_is_service': self.process_is_service,
            'process_max_concurrency': self.process_max_concurrency,
            'heartbeat_interval_seconds': self.api_heartbeat_interval,
            'max_age_seconds': None
        }

        if self.hostname is not None:
            body['hostname'] = self.hostname

        if self.max_conflicting_age_seconds is not None:
            body['max_age_seconds'] = self.max_conflicting_age_seconds

        data = json.dumps(body).encode('utf-8')
        req = Request(url, data=data, headers=headers, method='POST')

        with self._send_api_request(req, api_retries=self.api_retries_for_process_creation_conflict,
                                    is_process_creation_request=True) as f:
            self._logger.info("Process execution creation request was successful.")

            response_body = f.read().decode('utf-8')
            self._logger.debug(f"Got creation response '{response_body}'")

            response_dict = json.loads(response_body)
            self.id = response_dict['result']['id']

    def update_status(self, failed_attempts=None, timed_out_attempts=None,
                      pid=None, success_count=None, error_count=None,
                      skipped_count=None, expected_count=None, last_status_message=None,
                      extra_status_props=None):
        """Update the status of the process. Send to the process management
           server if the last status was sent more than status_update_interval
           seconds ago.
        """

        if success_count is not None:
            self.status_dict['success_count'] = int(success_count)

        if error_count is not None:
            self.status_dict['error_count'] = int(error_count)

        if skipped_count is not None:
            self.status_dict['skipped_count'] = int(skipped_count)

        if expected_count is not None:
            self.status_dict['expected_count'] = int(expected_count)

        if last_status_message is not None:
            self.status_dict['last_status_message'] = last_status_message

        if extra_status_props:
            self.status_dict.update(extra_status_props)

        should_send = self._is_status_update_due()

        # These are important updates that should be sent no matter what.
        if not ((failed_attempts is None) and (timed_out_attempts is None) and \
            (pid is None)) and (last_status_message is None):
            should_send = True

        if should_send:
            self._send_update(failed_attempts=failed_attempts,
                              timed_out_attempts=timed_out_attempts,
                              pid=pid)

    def send_completion(self, status,
                        failed_attempts=None, timed_out_attempts=None,
                        exit_code=None, pid=None):
        """Send the final result to the process management server."""

        if status not in self._ALLOWED_FINAL_STATUSES:
            self._exit_or_raise(self._EXIT_CODE_GENERIC_ERROR)

        if exit_code is None:
            if status == self.STATUS_SUCCEEDED:
                exit_code = self._EXIT_CODE_SUCCESS
            else:
                exit_code = self._EXIT_CODE_GENERIC_ERROR

        self._send_update(status=status, failed_attempts=failed_attempts,
                          timed_out_attempts=timed_out_attempts,
                          exit_code=exit_code, pid=pid)

    def managed_call(self, fun, data=None):
        """
        Call the argument object, which must be callable, doing retries as necessary,
        and wrapping with calls to the the process manager server.
        """
        if not callable(fun):
            raise RuntimeError("managed_call() argument is not callable")

        self.request_process_start_if_max_concurrency_ok()

        self.attempt_count = 0

        if self.process_max_retries < 0:
            max_attempts = math.inf
        else:
            max_attempts = self.process_max_retries + 1

        rv = None
        success = False
        saved_ex = None
        while self.attempt_count < max_attempts:
            self.attempt_count += 1

            self._logger.info(f"Calling managed function (attempt {self.attempt_count}/{max_attempts}) ...")

            try:
                rv = fun(self, data)
                success = True
            except Exception as ex:
                saved_ex = ex
                self._logger.exception('Managed function failed')

                if self.attempt_count < max_attempts:
                    self._send_update(failed_attempts=self.attempt_count)
                    if self.process_retry_delay:
                        self._logger.debug('Sleeping after managed function failed ...')
                        time.sleep(self.process_retry_delay)
                        self._logger.debug('Done sleeping after managed function failed.')

            if success:
                self.send_completion(status=ExecWrapper.STATUS_SUCCEEDED,
                                     failed_attempts=self.attempt_count - 1)
                return rv

        self.send_completion(status=ExecWrapper.STATUS_FAILED,
                             failed_attempts=self.attempt_count)
        raise saved_ex

    def run(self):
        """
        Run the wrapped process, first requesting to start, then executing
        the process command line, retying for errors and timeouts.
        """
        self._ensure_non_embedded_mode()

        self.request_process_start_if_max_concurrency_ok()

        self._logger.info(f"Created process execution {self.id}, starting now.")

        process_env = self._make_process_env()

        if self.log_secrets:
            self._logger.debug(f"Process environment = {process_env}")

        self.attempt_count = 0
        self.timeout_count = 0

        if self.process_max_retries < 0:
            max_attempts = math.inf
        else:
            max_attempts = self.process_max_retries + 1

        self._open_status_socket()

        while self.attempt_count < max_attempts:
            self.attempt_count += 1
            self.timed_out = False

            self._logger.info(f"Running process (attempt {self.attempt_count}/{max_attempts}) ...")

            current_time = time.time()

            process_finish_deadline = math.inf
            if self.process_timeout:
                process_finish_deadline = current_time + self.process_timeout

            next_heartbeat_time = math.inf
            if self.api_heartbeat_interval:
                next_heartbeat_time = current_time + self.api_heartbeat_interval

            next_process_check_time = current_time + self.process_check_interval

            # Set the session ID so we can kill the process as a group, so we kill
            # all subprocesses. See https://stackoverflow.com/questions/4789837/how-to-terminate-a-python-subprocess-launched-with-shell-true
            self.process = Popen(' '.join(self.command), shell=True, stdout=None,
                                 stderr=None, env=process_env, cwd=self.working_dir,
                                 preexec_fn=os.setsid)

            pid = self.process.pid
            self._logger.info(f"pid = {pid}")

            if self.send_pid:
                self._send_update(pid=pid)

            done_polling = False
            while not done_polling:
                self.process.poll()

                self._read_from_status_socket()

                current_time = time.time()

                # None means the process is still running
                if self.process.returncode is None:
                    if current_time >= process_finish_deadline:
                        done_polling = True
                        self.timed_out = True
                        self.timeout_count += 1

                        if self.attempt_count < max_attempts:
                            self._send_update(failed_attempts=self.attempt_count,
                                              timed_out_attempts=self.timeout_count)

                        self._logger.warning(f"Process timed out after {self.process_timeout} seconds, sending SIGTERM ...")
                        self._terminate_or_kill_process()
                    else:
                        if current_time >= next_process_check_time:
                            next_process_check_time = current_time + self.process_check_interval

                        if (self.last_update_sent_at is not None) and self.api_heartbeat_interval:
                            next_heartbeat_time = max(next_heartbeat_time,
                                                      self.last_update_sent_at + self.api_heartbeat_interval)

                        if current_time >= next_heartbeat_time:
                            self._send_update()
                            current_time = time.time()
                            next_heartbeat_time = current_time + self.api_heartbeat_interval

                        sleep_until = min(process_finish_deadline or math.inf, next_heartbeat_time or math.inf,
                                          next_process_check_time)
                        sleep_duration = sleep_until - current_time

                        if sleep_duration > 0:
                            # self._logger.debug('Sleeping while process is running ...')
                            time.sleep(sleep_duration)
                            # self._logger.debug('Done sleeping while process is running.')

                else:
                    self._logger.info(f"Process exited with exit code {self.process.returncode}")

                    done_polling = True

                    if self.process.returncode == 0:
                        exit(0)
                    elif self.attempt_count >= max_attempts:
                        exit(self.process.returncode)

                    self._send_update(failed_attempts=self.attempt_count)

                    if self.process_retry_delay:
                        self._logger.debug('Sleeping after process failed ...')
                        time.sleep(self.process_retry_delay)
                        self._logger.debug('Done sleeping after process failed.')

        if self.attempt_count >= max_attempts:
            exit(self._EXIT_CODE_GENERIC_ERROR)
        else:
            self._send_update(failed_attempts=self.attempt_count,
                              timed_out_attempts=self.timeout_count)

        # Exit handler will send update to API server

    def _handle_exit(self):
        self._logger.debug(f"Exit handler, wrapper ID = {self.id}")

        status = self.STATUS_FAILED
        failed_attempts = self.attempt_count
        notification_required = True

        try:
            if self.was_conflict:
                self._logger.debug('Conflict detected, not notifying because server-side should have notified already.')
                notification_required = False
            elif self.api_base_url and self.api_key and self.id:
                if not self.api_server_retries_exhausted:
                    if self.process and self.process.returncode == 0:
                        status = self.STATUS_SUCCEEDED
                        failed_attempts -= 1
                    elif self.timed_out:
                        status = self.STATUS_TERMINATED_AFTER_TIME_OUT

                    exit_code = None
                    pid = None
                    if self.process:
                        pid = self.process.pid
                        if pid is not None:
                            if self.process.returncode is None:
                                self._terminate_or_kill_process()
                            else:
                                exit_code = self.process.returncode

                    self.send_completion(status=status,
                                         failed_attempts=failed_attempts,
                                         timed_out_attempts=self.timeout_count,
                                         exit_code=exit_code, pid=pid)
                    notification_required = False
        except Exception:
            self._logger.exception("Exception in exit_handler")
        finally:
            self._close_status_socket()

        if notification_required:
            if self.rollbar_access_token and not self.rollbar_retries_exhausted:
                error_message = 'API Server not configured'

                if self.api_server_retries_exhausted:
                    error_message = 'API Server not responding properly'
                elif not self.id:
                    error_message = 'Process Execution ID not assigned'

                self._send_rollbar_error(error_message, self.last_api_request_data)
            else:
                self._logger.error("Can't notify any valid sink!")

    def _terminate_or_kill_process(self):
        self._ensure_non_embedded_mode()

        if (not self.process) or (not self.process.pid):
            self._logger.info('No process found, not terminating')
            return

        pid = self.process.pid

        exit_code = self.process.returncode
        if exit_code is not None:
            self._logger.info(f"Process {pid} already exited with code {exit_code}, not terminating")
            return

        self._logger.warning(f"Sending SIGTERM ...")

        os.killpg(os.getpgid(pid), signal.SIGTERM)

        try:
            self.process.communicate(timeout=self.process_termination_grace_period_seconds)

            self._logger.info("Process terminated successfully after SIGTERM.")
        except TimeoutExpired:
            self._logger.info("Timeout after SIGTERM expired, killing with SIGKILL ...")
            try:
                os.killpg(os.getpgid(pid), signal.SIGKILL)
                self.process.communicate()
            except Exception:
                self._logger.exception(f"Could not kill process with pid {pid}")

        self.process = None

    def _ensure_non_embedded_mode(self):
        if self._embedded_mode:
            raise RuntimeError('Called method is only for wrapped process (non-embedded) mode')

    def _open_status_socket(self):
        if self.status_update_socket_port is None:
            return None

        try:
            self._logger.info('Opening status update socket ...')
            self._status_socket = socket.socket(socket.AF_INET,
                                                socket.SOCK_DGRAM)
            self._status_socket.bind(('127.0.0.1', self.status_update_socket_port))
            self._status_socket.setblocking(False)
            self._logger.info('Successfully created status update socket')
            return self._status_socket
        except Exception:
            self._logger.exception("Can't create status update socket")
            self._close_status_socket()
            return None

    def _read_from_status_socket(self):
        if self._status_socket is None:
            return

        nbytes = 1
        while nbytes > 0:
            try:
                nbytes = self._status_socket.recv_into(self._status_buffer)
            except OSError:
                # Happens when there is no data to be read, since the socket is non-blocking
                return

            start_index = 0
            end_index = 0

            while (end_index >= 0) and (start_index < nbytes):
                end_index = self._status_buffer.find(b'\n', start_index, nbytes)

                end_index_for_copy = end_index
                if end_index < 0:
                    end_index_for_copy = nbytes

                bytes_to_copy = end_index_for_copy - start_index
                if len(self._status_buffer) + bytes_to_copy > self.status_update_message_max_bytes:
                    self._logger.warning(f"Discarding status message which exceeded maximum size: {self._status_buffer}")
                    self._status_buffer.clear()
                else:
                    self._status_message_so_far.extend(self._status_buffer[start_index:end_index_for_copy])
                    if end_index >= 0:
                        self._handle_status_message_complete()

                if end_index >= 0:
                    start_index = end_index + 1

    def _close_status_socket(self):
        if self._status_socket:
            try:
                self._status_socket.close()
            except Exception:
                self._logger.warning("Can't close socket")

            self._status_socket = None

    def _handle_status_message_complete(self):
        try:
            message = self._status_message_so_far.decode('utf-8')

            self._logger.debug(f"Got status message '{message}'")

            try:
                self.status_dict.update(json.loads(message))

                if self._is_status_update_due():
                    self._send_update()
            except json.JSONDecodeError:
                self._logger.debug('Error decoding JSON, this can happen due to missing out of order messages')
        except UnicodeDecodeError:
            self._logger.debug('Error decoding message as UTF-8, this can happen due to missing out of order messages')
        finally:
            self._status_message_so_far.clear()

    def _is_status_update_due(self):
        return (self.last_update_sent_at is None) or \
               ((self.status_update_interval is not None) and \
                (time.time() - self.last_update_sent_at > self.status_update_interval))

    @staticmethod
    def _string_to_int(s, default_value=None, negative_value=None):
        if s is None:
            return default_value
        else:
            x = int(s)

            if x < 0:
                x = negative_value

            return x

    def _make_headers(self):
        headers = {
            'Authorization': f"Token token={self.api_key}",
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        return headers

    def _make_process_env(self):
        process_env = os.environ.copy()
        process_env['EXEC_WRAPPER_DEPLOYMENT'] = self.deployment
        process_env['EXEC_WRAPPER_API_BASE_URL'] = self.api_base_url
        process_env['EXEC_WRAPPER_API_KEY'] = self.api_key
        process_env['EXEC_WRAPPER_API_TIMEOUT'] = str(self.api_timeout)
        process_env['EXEC_WRAPPER_API_RETRIES'] = str(self.api_retries)
        process_env['EXEC_WRAPPER_API_RETRY_DELAY_SECONDS'] = \
            str(self.api_retry_delay)

        enable_status_update_listener = (self.status_update_socket_port is not None)
        process_env['EXEC_WRAPPER_ENABLE_STATUS_UPDATE_LISTENER'] = \
            str(enable_status_update_listener).upper()
        if enable_status_update_listener:
            process_env['EXEC_WRAPPER_STATUS_UPDATE_SOCKET_PORT'] = \
                str(self.status_update_socket_port)
            process_env['EXEC_WRAPPER_STATUS_UPDATE_INTERVAL_SECONDS'] = \
                str(self.status_update_interval)
            process_env['EXEC_WRAPPER_STATUS_UPDATE_MESSAGE_MAX_BYTES'] = \
                str(self.status_update_message_max_bytes)            

        process_env['EXEC_WRAPPER_ROLLBAR_ACCESS_TOKEN'] = str(self.rollbar_access_token)
        process_env['EXEC_WRAPPER_ROLLBAR_TIMEOUT'] = str(self.rollbar_timeout)
        process_env['EXEC_WRAPPER_ROLLBAR_RETRIES'] = str(self.rollbar_retries)
        process_env['EXEC_WRAPPER_ROLLBAR_RETRY_DELAY_SECONDS'] = str(self.rollbar_retry_delay)
        process_env['EXEC_WRAPPER_PROCESS_EXECUTION_ID'] = str(self.id)
        process_env['EXEC_WRAPPER_PROCESS_TYPE_ID'] = str(self.process_type_id)
        process_env['EXEC_WRAPPER_PROCESS_NAME'] = str(self.process_name)
        process_env['EXEC_WRAPPER_PROCESS_CRON_SCHEDULE'] = str(self.process_cron_schedule)
        process_env['EXEC_WRAPPER_PROCESS_TIMEOUT_SECONDS'] = str(self.process_timeout)
        process_env['EXEC_WRAPPER_PROCESS_TERMINATION_GRACE_PERIOD_SECONDS'] = str(
            self.process_termination_grace_period_seconds)

        encoded_max_concurrency = self.process_max_concurrency

        if self.process_max_concurrency is None:
            encoded_max_concurrency = -1

        process_env['EXEC_WRAPPER_PROCESS_MAX_CONCURRENCY'] = str(encoded_max_concurrency)

        return process_env

    def _send_update(self, status=None,
        failed_attempts=None, timed_out_attempts=None,
        exit_code=None, pid=None):

        if status is None:
            status = self.STATUS_RUNNING

        body = {
            'status': status
        }

        body.update(self.status_dict)

        if failed_attempts is not None:
            body['failed_attempts'] = failed_attempts

        if timed_out_attempts is not None:
            body['timed_out_attempts'] = timed_out_attempts

        if exit_code is not None:
            body['exit_code'] = exit_code

        if pid is not None:
            body['pid'] = pid

        url = f"{self.api_base_url}/v1/process_executions/{quote_plus(str(self.id))}"
        headers = self._make_headers()

        if status == self.STATUS_RUNNING:
            api_retries = self.api_retries
        else:
            api_retries = self.api_retries_for_final_update

        data = json.dumps(body).encode('utf-8')

        self._logger.debug(f"Sending update '{data}' ...")

        req = Request(url, data=data, headers=headers, method='PATCH')
        with self._send_api_request(req, api_retries=api_retries) as f:
            self._logger.debug("Update sent successfully.")
            self.last_update_sent_at = time.time()
            self.status_dict = {}

    def _send_api_request(self, req, api_retries=-1, is_process_creation_request=False):
        if self.api_server_retries_exhausted:
            self._logger.debug('Not sending API request because all retries are exhausted')
            return None

        if api_retries == -1:
            if is_process_creation_request:
                api_retries = self.api_retries_for_process_creation_conflict
            else:
                api_retries = self.api_retries

        attempt_count = 0

        if api_retries == math.inf:
            max_attempts = math.inf
        else:
            max_attempts = api_retries + 1

        rollbar_data = {
            'request': {
                'url': req.full_url,
                'method': req.method,
                'body': str(req.data)
            }
        }

        while (api_retries == math.inf) or (attempt_count < max_attempts):
            attempt_count += 1
            retry_delay = self.api_retry_delay

            self._logger.info(f"Sending API request (attempt {attempt_count}/{max_attempts}) ...")

            try:
                resp = urlopen(req, timeout=self.api_timeout)
                self.last_api_request_data = None
                return resp
            except HTTPError as http_error:
                status_code = http_error.code

                response_body = None

                try:
                    response_body = str(http_error.read())
                except:
                    self._logger.warning("Can't read error response body")

                rollbar_data.pop('error', None)
                rollbar_data['response'] = {
                    'status_code': status_code,
                    'body': response_body
                }

                if status_code in self._RETRYABLE_HTTP_STATUS_CODES:
                    error_message = f"Endpoint temporarily not available, status code = {status_code}"
                    self._logger.warning(error_message)
                    self._send_rollbar_error(error_message, rollbar_data)
                elif is_process_creation_request and self.is_concurrency_limited_service and (status_code == 409) and (
                        attempt_count < max_attempts):
                    # Wait one heartbeat interval before retrying, so that the existing
                    # process is considered dead.
                    retry_delay = self.api_heartbeat_interval
                    self._logger.info(f"Response code = 409 for concurrency limited service, will retry")
                else:
                    logging.critical(f"Response code = {status_code}, exiting.")

                    self.last_api_request_data = rollbar_data

                    if status_code == 409:
                        self.was_conflict = True

                    exit_code = self._RESPONSE_CODE_TO_EXIT_CODE.get(status_code) or self._EXIT_CODE_GENERIC_ERROR
                    self._exit_or_raise(exit_code)
            except URLError as url_error:
                error_message = f"URL error: {url_error}"
                self._logger.error(error_message)

                rollbar_data.pop('response', None)
                rollbar_data['error'] = {
                    'reason': str(url_error.reason)
                }

                self._send_rollbar_error(error_message, rollbar_data)

            self._logger.debug(f"Sleeping {retry_delay} seconds after request error ...")
            time.sleep(retry_delay)
            self._logger.debug('Done sleeping after request error.')

        self.api_server_retries_exhausted = True
        self._logger.error("Exhausted all retries, giving up.")
        self._exit_or_raise(self._EXIT_CODE_GENERIC_ERROR)

    def _exit_or_raise(self, exit_code):
        if self._embedded_mode:
            raise RuntimeError(f"Raising an error in library mode, exit code {exit_code}")
        else:
            if self.called_exit:
                raise RuntimeError(
                    f"exit() called already; raising exception instead of exiting with exit code {exit_code}")
            else:
                self.called_exit = True
                exit(exit_code)

    def _send_rollbar_error(self, message, data=None):
        if not self.rollbar_access_token:
            self._logger.warning(f"Not sending '{message}' to Rollbar since no access token found")
            return False

        if self.rollbar_retries_exhausted:
            self._logger.debug(f"Not sending '{message}' to Rollbar since all retries are exhausted")
            return False

        payload = {
            'access_token': self.rollbar_access_token,
            'data': {
                'environment': self.deployment,
                'body': {
                    'message': {
                        'body': f"[execution_wrapper]: '{message}'",
                        'process_name': self.process_name,
                        'project': self.project,
                        'program_version': self.program_version,
                        'wrapper_version': self.wrapper_version,
                        'process_cron_schedule': self.process_cron_schedule,
                        'process_type_id': self.process_type_id,
                        'was_conflict': self.was_conflict,
                        'process_execution_id': self.id,
                        'api_server_retries_exhausted': self.api_server_retries_exhausted,
                        'data': data
                    }
                },
                'level': 'error',
                'server': {
                    'host': self.hostname
                    # Could put code version here
                }
            }
        }

        request_body = json.dumps(payload).encode('utf-8')
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        req = Request('https://api.rollbar.com/api/1/item/', data=request_body, headers=headers, method='POST')

        attempt_count = 0
        max_attempts = self.rollbar_retries + 1

        while attempt_count < max_attempts:
            attempt_count += 1

            self._logger.info(f"Sending Rollbar request (attempt {attempt_count}/{max_attempts}) ...")

            try:
                with urlopen(req, timeout=self.rollbar_timeout) as f:
                    response_body = f.read().decode('utf-8')
                    self._logger.debug(f"Got Rollbar response '{response_body}'")

                    response_dict = json.loads(response_body)
                    uuid = response_dict['result']['uuid']

                    self._logger.debug(f"Rollbar request returned UUID {uuid}.")

                    return True
            except HTTPError as http_error:
                status_code = http_error.code
                logging.critical(f"Rollbar response code = {status_code}, giving up.")
                return False
            except URLError as url_error:
                self._logger.error(f"Rollbar URL error: {url_error}")

                self._logger.debug('Sleeping after Rollbar request error ...')
                time.sleep(self.rollbar_retry_delay)
                self._logger.debug('Done sleeping after Rollbar request error.')

        self.rollbar_retries_exhausted = True
        self._logger.error("Exhausted all retries, giving up.")
        return False


if __name__ == '__main__':
    main_parser = ExecWrapper.make_arg_parser(require_command=True)
    main_args = main_parser.parse_args()

    main_process_type_id = main_args.process_type_id or os.environ.get('EXEC_WRAPPER_PROCESS_TYPE_ID')
    main_args.process_type_id = main_process_type_id

    log_level = (main_args.log_level or os.environ.get('EXEC_WRAPPER_LOG_LEVEL', _DEFAULT_LOG_LEVEL)).upper()
    numeric_log_level = getattr(logging, log_level, None)
    if not isinstance(numeric_log_level, int):
        logging.warning(f"Invalid log level: {log_level}, defaulting to {_DEFAULT_LOG_LEVEL}")
        numeric_log_level = getattr(logging, _DEFAULT_LOG_LEVEL, None)

    logging.basicConfig(level=numeric_log_level,
                        format=f"EXEC_WRAPPER {main_process_type_id}: %(asctime)s %(levelname)s: %(message)s")

    ExecWrapper(args=main_args, embedded_mode=False).run()
