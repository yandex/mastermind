import functools
import itertools
import json
import logging
import random
import socket
import time
import urllib

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.httpclient import HTTPRequest, AsyncHTTPClient, HTTPError
# TODO: Replace with AsyncHttpClient?
from tornado.simple_httpclient import SimpleAsyncHTTPClient

import errors
import helpers
from mastermind_core.config import config
import timed_queue
import storage


logger = logging.getLogger('mm.minions')

MINIONS_CFG = config.get('minions', {})


class MinionsMonitor(object):

    STATE_URL_TPL = 'http://{host}:{port}/rsync/list/?finish_ts_gte={finish_ts_gte}'
    START_URL_TPL = 'http://{host}:{port}/rsync/start/'
    TERMINATE_URL_TPL = 'http://{host}:{port}/command/terminate/'

    MAKE_IOLOOP = 'make_ioloop'
    MAKE_HTTP_CLIENT = 'make_http_client'
    STATE_FETCH = 'state_fetch'

    CREATE_GROUP_URL_TPL = 'http://{host}:{port}/command/create_group/'
    REMOVE_GROUP_URL_TPL = 'http://{host}:{port}/command/remove_group/'
    DNET_CLIENT_CMD_URL_TPL = 'http://{host}:{port}/command/dnet_client/'

    def __init__(self, meta_db):

        self._hosts = []

        self.__tq = timed_queue.TimedQueue()

        self.minion_headers = ({'X-Auth': MINIONS_CFG['authkey']}
                               if MINIONS_CFG.get('authkey') else
                               None)
        self.minion_port = MINIONS_CFG.get('port', 8081)
        self.http_client = None
        self.finish_ts_per_host = {}

        # set an independent IOLoop instance in timed queue thread
        self.__tq.add_task_in(
            self.MAKE_IOLOOP,
            0,
            self._make_tq_thread_ioloop)

        self.__tq.add_task_in(
            self.MAKE_HTTP_CLIENT,
            0,
            self._make_http_client)

        self.__tq.add_task_in(
            self.STATE_FETCH,
            5,
            self._fetch_states)

        db_name = config.get('metadata', {}).get('minions', {}).get('db', '')
        if not db_name:
            raise RuntimeError('Minions monitor requires minions metadb to be set up')

        self.commands = meta_db[db_name]['commands']

    def _start_tq(self):
        self.__tq.start()

    def _fetch_stored_unfinished_commands(self):
        commands = {}
        for cmd in self.commands.find({'exit_code': None}):
            commands[cmd['uid']] = cmd
        return commands

    def _fetch_states(self):
        logger.info('Fetching minion states task started')
        try:
            hosts = storage.hosts.keys()
            random.shuffle(hosts)

            urls = []
            current_ts = int(time.time())

            for host in hosts:
                # at first fetch tasks finished in last 24 hours or not finished at all
                last_host_min_finish_ts = self.finish_ts_per_host.get(
                    host,
                    int(time.time()) - 24 * 60 * 60
                )

                url = self.STATE_URL_TPL.format(host=host.addr,
                                                port=self.minion_port,
                                                finish_ts_gte=last_host_min_finish_ts)
                urls.append(url)

            logger.debug('Starting async http batch, {} hosts'.format(len(urls)))
            responses = self._perform_http_requests_sync(urls)
            logger.debug('Finished async http batch, {} hosts'.format(len(urls)))
            logger.info('Responses from minions: {}'.format(responses))

            logger.debug('Fetching stored minion commands state from mongo')
            stored_commands = self._fetch_stored_unfinished_commands()
            logger.debug('Finished fetching stored minion commands state from mongo')

            for host, response in itertools.izip(hosts, responses):

                try:
                    data = self._get_response(host, response)
                except HTTPError as e:
                    logger.error('Minion tasks fetch error: {0}'.format(e))
                    continue
                try:
                    self._process_state(host.addr, data, stored_commands)
                except errors.MinionApiError:
                    continue

                self.finish_ts_per_host[host] = current_ts

            logger.info('Finished fetching minion states task')
        except errors.NotReadyError as e:
            logger.warn('Failed to sync minions state: minions history is not fetched')
        except Exception as e:
            logger.exception('Failed to sync minions state')
        finally:
            self.__tq.add_task_in(
                self.STATE_FETCH,
                MINIONS_CFG.get('commands_fetch_period', 120),
                self._fetch_states)

    def _get_response(self, host, response):
        if response.error:
            error = response.error
            if isinstance(error, socket.error):
                code = error.errno
            elif isinstance(error, HTTPError):
                code = error.code
            else:
                raise TypeError(
                    'Unexpected http error type "{}": {}'.format(
                        type(error).__name__,
                        error
                    )
                )
            if code == 599:
                error_msg = ('Failed to connect to minion '
                             'on host {0} ({1})'.format(host, error.message))
            else:
                error_msg = ('Minion http error on host {0}, '
                             'code {1} ({2})'.format(host, code, error.message))
            logger.error(error_msg)
            raise error

        return response.body

    def _unwrap_response(self, response, host):
        if response['status'] != 'success':
            logger.warn('Host: {0}, minion returned error: {1}'.format(
                host, response['error']))
            raise errors.MinionApiError('Minion error: {0}'.format(response['error']))
        return response['response']

    def _process_state(self, addr, response, stored_commands, force_update=False):

        response_data = self._unwrap_response(json.loads(response), addr)

        hostname = storage.hosts[addr].hostname_or_not
        logger.debug(
            'Received {} minion task states from host {} ({})'.format(
                len(response_data),
                hostname,
                addr
            )
        )
        for uid, state in response_data.iteritems():
            state['uid'] = uid
            state['host'] = addr
            state['hostname'] = hostname

            if 'group' in state:
                group_id = int(state['group'])
                if group_id in storage.groups:
                    couple = storage.groups[group_id].couple
                    if couple:
                        state['couple'] = str(couple)

            # stdout and stderr output should not be saved to a db if present
            if 'output' in state:
                del state['output']
            if 'error_output' in state:
                del state['error_output']

        for uid, state in response_data.iteritems():

            update_stored_command = (
                force_update or (
                    uid in stored_commands and (
                        # minion command is finished
                        state['exit_code'] is not None or
                        # progress value changed noticeably
                        abs(state['progress'] - stored_commands[uid]['progress']) >= 0.01
                    )
                )
            )

            logger.debug(
                'Checking minion command "{}": update required: {}'.format(
                    state['uid'],
                    update_stored_command
                )
            )

            if update_stored_command:
                self._update_stored_command(uid, state)

        return response_data

    def _update_stored_command(self, uid, state):

        res = self.commands.update(
            spec={'uid': uid},
            document=state,
            upsert=True,
        )

        if 'ok' not in res or res['ok'] != 1:
            logger.error(
                'Failed to save minion command "{}": {}'.format(
                    uid,
                    res,
                )
            )
            raise ValueError(
                'Failed to save minion command "{}"'.format(
                    uid
                )
            )

    def _make_tq_thread_ioloop(self):
        logger.debug('Minion states, creating thread ioloop')
        io_loop = IOLoop()
        io_loop.make_current()

    def _make_http_client(self):
        logger.debug('Minion states, creating http client')
        # TODO: set max_clients in config
        self.http_client = AsyncHTTPClient(max_clients=100)

    @helpers.handler_wne
    def set_minion_hosts(self, hosts):
        self._hosts = hosts

    def _perform_http_requests_sync(self, urls):
        return IOLoop.current().run_sync(functools.partial(self._perform_http_requests, urls))

    @gen.coroutine
    def _perform_http_requests(self, urls):
        results = []
        for url in urls:
            request = HTTPRequest(
                url,
                method='GET',
                headers=self.minion_headers,
                # TODO: fix timeout settings
                connect_timeout=MINIONS_CFG.get('commands_fetch_timeout', 15),
                request_timeout=MINIONS_CFG.get('commands_fetch_timeout', 15),
                follow_redirects=False,
                allow_ipv6=True,
                use_gzip=True,
            )
            result = self.http_client.fetch(request, raise_error=False)
            results.append(result)
        ret = yield results
        raise gen.Return(ret)

    @helpers.concurrent_handler
    def get_command(self, request):
        try:
            uid = request[0]
            return self._get_command(uid)
        except ValueError:
            raise ValueError('Unknown command uid {0}'.format(uid))

    def _get_command(self, uid):
        command = self.commands.find_one(
            spec_or_id={'uid': uid},
            fields={'_id': False}
        )
        if not command:
            raise ValueError('Command "{}" is not found'.format(uid))
        return command

    @helpers.concurrent_handler
    def execute_cmd(self, request):
        try:
            host, command, params = request[0:3]
        except ValueError:
            raise ValueError('Invalid parameters')

        if host not in storage.hosts:
            logger.debug('Host was not found: {0}, {1}'.format(host, type(host)))
            raise ValueError('Host {0} is not present in cluster'.format(host))

        return self.execute(host, command, params)

    def execute(self, host, command, params):
        url = self.START_URL_TPL.format(host=self._wrap_host(host), port=self.minion_port)
        data = self._update_query_parameters(
            dst={'command': command},
            src=params,
        )

        io_loop = IOLoop()
        client = SimpleAsyncHTTPClient(io_loop)
        response = io_loop.run_sync(
            functools.partial(
                client.fetch,
                url,
                method='POST',
                headers=self.minion_headers,
                body=urllib.urlencode(data, doseq=True),
                request_timeout=MINIONS_CFG.get('request_timeout', 5.0),
                allow_ipv6=True,
                use_gzip=True,
            )
        )

        return self._process_state(
            host,
            self._get_response(host, response),
            stored_commands={},
            force_update=True,
        )

    @staticmethod
    def _wrap_host(addr):
        '''
        Wrap host ip address with square brackets if ipv6
        '''
        if not addr.startswith('[') and ':' in addr:
            return '[' + addr + ']'
        return addr

    def _update_query_parameters(self, dst, src):
        params_rename = {
            'success_codes': 'success_code'
        }

        for k, v in src.iteritems():
            if k == 'command':
                raise ValueError('Parameter "command" is not accepted as command parameter')

            if isinstance(v, (list, tuple)):
                for val in v:
                    self._check_param(k, val)
            else:
                self._check_param(k, v)
            dst[params_rename.get(k, k)] = v
        return dst

    @staticmethod
    def _check_param(key, val):
        if not isinstance(val, (int, long, basestring)):
            message = (
                'Invalid parameter {key} of type {type}, '
                'only strings and numbers are accepted'.format(
                    key=key,
                    type=type(val).__name__,
                )
            )
            logger.warn(message)
            raise ValueError(message)

    @helpers.concurrent_handler
    def terminate_cmd(self, request):
        try:
            host, uid = request[0:2]
        except ValueError:
            raise ValueError('Invalid parameters')

        if host not in storage.hosts:
            raise ValueError('Host "{}" was not found'.format(host))

        return self._terminate_cmd(host, uid)

    def _terminate_cmd(self, host, uid):
        url = self.TERMINATE_URL_TPL.format(host=self._wrap_host(host), port=self.minion_port)
        data = {'cmd_uid': uid}

        io_loop = IOLoop()
        client = SimpleAsyncHTTPClient(io_loop)
        response = io_loop.run_sync(
            functools.partial(
                client.fetch,
                url,
                method='POST',
                headers=self.minion_headers,
                body=urllib.urlencode(data),
                request_timeout=MINIONS_CFG.get('request_timeout', 5.0),
                allow_ipv6=True,
                use_gzip=True,
            )
        )

        return self._process_state(
            host,
            self._get_response(host, response),
            stored_commands={},
            force_update=True,
        )

    def create_group(self, host, params, files):
        url = self.CREATE_GROUP_URL_TPL.format(host=self._wrap_host(host), port=self.minion_port)
        data = self._update_query_parameters(
            dst={'command': 'create_group'},
            src=params,
        )
        commands_states = self._perform_request(
            host=host,
            url=url,
            params=data,
            files=files,
        )

        # a single command execution should return a list with a single command
        return commands_states.values()[0]

    def remove_group(self, host, params):
        url = self.REMOVE_GROUP_URL_TPL.format(host=self._wrap_host(host), port=self.minion_port)
        data = self._update_query_parameters(
            dst={'command': 'remove_group'},
            src=params,
        )
        commands_states = self._perform_request(
            host=host,
            url=url,
            params=data,
        )

        # a single command execution should return a list with a single command
        return commands_states.values()[0]

    def dnet_client_cmd(self, host, params, files=None):
        url = self.DNET_CLIENT_CMD_URL_TPL.format(
            host=self._wrap_host(host),
            port=self.minion_port,
        )
        data = self._update_query_parameters(
            dst={'command': 'dnet_client_cmd'},
            src=params,
        )
        commands_states = self._perform_request(
            host=host,
            url=url,
            params=data,
            files=files,
        )

        # a single command execution should return a list with a single command
        return commands_states.values()[0]
