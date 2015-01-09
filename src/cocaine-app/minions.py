import datetime
import json
import logging
import random
import threading
import time
import traceback
import urllib

import elliptics
import msgpack
from tornado.httpclient import AsyncHTTPClient, HTTPClient, HTTPError
from tornado.ioloop import IOLoop

from config import config
import errors
import keys
import timed_queue
import storage


logger = logging.getLogger('mm.minions')


class Minions(object):

    STATE_FETCH = 'state_fetch'
    STATE_FETCH_ACTIVE = 'state_fetch_active'
    HISTORY_FETCH = 'history_fetch'
    HISTORY_ENTRY_FETCH = 'history_entry_%s_fetch'

    MAKE_IOLOOP = 'make_ioloop'
    STATE_URL_TPL = 'http://{host}:{port}/rsync/list/'
    START_URL_TPL = 'http://{host}:{port}/rsync/start/'

    def __init__(self, node):
        self.meta_session = node.meta_session.clone()

        self.commands = {}
        self.history = {}
        self.active_hosts = []

        self.history_ready = False
        self.ready = False

        self.__tq = timed_queue.TimedQueue()

        self.__tq.add_task_in(self.MAKE_IOLOOP, 0,
            self._make_tq_thread_ioloop)

        self.__tq.add_task_in(self.HISTORY_FETCH,
            5, self._fetch_history)

        self.__tq.add_task_in(self.STATE_FETCH,
            5, self._fetch_states)

        self.minion_headers = ({'X-Auth': config['minions']['authkey']}
                               if config.get('minions', {}).get('authkey') else
                               None)
        self.minion_port = config.get('minions', {}).get('port', 8081)

        self.__commands_lock = threading.Lock()
        self.__active_hosts_lock = threading.Lock()

    def _start_tq(self):
        self.__tq.start()

    def _make_tq_thread_ioloop(self):
        logger.debug('Minion states, creating thread ioloop')
        io_loop = IOLoop()
        io_loop.make_current()

    def _fetch_states(self, active_hosts=False):
        logger.info('Fetching minion states task started')
        try:
            if not self.history_ready:
                raise errors.NotReadyError

            states = {}

            if not active_hosts:
                hosts = storage.hosts.keys()
            else:
                if not self.active_hosts:
                    return
                with self.__active_hosts_lock:
                    hosts = self.active_hosts
                    self.active_hosts = []

            random.shuffle(hosts)

            for host in hosts:
                url = self.STATE_URL_TPL.format(host=host.addr,
                                                port=self.minion_port)
                states[url] = host
            logger.debug('Starting async batch')
            responses = AsyncHTTPBatch(states.keys(),
                headers=self.minion_headers,
                timeout=config.get('minion', {}).get('commands_fetch_timeout', 15)).get()

            successfull_hosts = 0
            for url, response in responses.iteritems():
                host = states[url]

                try:
                    data = self._get_response(host, response)
                except ValueError as e:
                    logger.error('Minion tasks fetch error')
                    logger.debug(e)
                    continue
                try:
                    self._process_state(host.addr, data)
                except errors.MinionApiError:
                    continue

                successfull_hosts += 1

            if not self.ready and not active_hosts:
                # toggle ready state only if acitve_hosts == False (all minions are traversed)
                self.ready = successfull_hosts >= len(states)
                if not self.ready:
                    logger.warn('Failed to sync minions state: '
                        'received responses from {0}/{1} minions'.format(
                            successfull_hosts, len(states)))

            logger.info('Finished fetching minion states task')
        except errors.NotReadyError as e:
            logger.warn('Failed to sync minions state: '
                'minions history is not fetched')
        except Exception as e:
            logger.error('Failed to sync minions state: %s\n%s' %
                         (e, traceback.format_exc()))
        finally:
            self.__tq.add_task_in(self.STATE_FETCH,
                config.get('minions', {}).get('commands_fetch_period', 120),
                self._fetch_states)

    def _process_state(self, addr, response):

        response_data = self._get_wrapped_response(json.loads(response), addr)

        hostname = storage.hosts[addr].hostname
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

        with self.__commands_lock:
            self.commands.update(response_data)

        for uid, state in response_data.iteritems():
            if (self.history.get(uid) is None or
                int(self.history[uid]) != int(state['progress'])):

                logger.debug('Adding new task {0}'.format(self.HISTORY_ENTRY_FETCH % uid))
                self.__tq.add_task_in(self.HISTORY_ENTRY_FETCH % uid,
                    1, self._history_entry_update, state)

        for uid, state in response_data.iteritems():
            if state['progress'] < 1.0:
                with self.__active_hosts_lock:
                    self.active_hosts.append(storage.hosts[addr])

                try:
                    self.__tq.add_task_in(self.STATE_FETCH_ACTIVE,
                        config.get('minions', {}).get('active_fetch_period', 5),
                        self._fetch_states,
                        active_hosts=True)
                except Exception:
                    # task already exists
                    pass

                break

        return response_data

    def _get_response(self, host, response):
        if isinstance(response, HTTPError):
            error = response
        else:
            error = response.error
        if error:
            code = error.code
            if code == 599:
                error_msg = ('Failed to connect to minion '
                             'on host {0} ({1})'.format(host, error.message))
            else:
                error_msg = ('Minion http error on host {0}, '
                             'code {1} ({2})'.format(host, code, error.message))
            raise ValueError(error_msg)

        return response.body

    def _get_wrapped_response(self, response, host):
        if response['status'] != 'success':
            logger.warn('Host: {0}, minion returned error: {1}'.format(
                host, response['error']))
            raise errors.MinionApiError('Minion error: {0}'.format(response['error']))
        return response['response']

    def get_command(self, request):
        try:
            uid = request[0]
        except ValueError:
            raise ValueError('Invalid parameters')
        if not uid in self.commands:
            raise ValueError('Unknown minion command id {0}'.format(uid))
        return self.commands[uid]

    def get_commands(self, request):
        return sorted(self.commands.itervalues(), key=lambda c: c['start_ts'])

    def execute_cmd(self, request):
        try:
            host, command, params = request[0:3]
        except ValueError:
            raise ValueError('Invalid parameters')

        if not host in storage.hosts:
            logger.debug('Host was not found: {0}, {1}'.format(host, type(host)))
            raise ValueError('Host {0} is not present in cluster'.format(host))

        url = self.START_URL_TPL.format(host=host, port=self.minion_port)
        data = {'command': command}
        for k, v in params.iteritems():
            if k == 'command':
                raise ValueError('Parameter "command" is not accepted as command parameter')
            if not isinstance(v, basestring):
                logger.warn('Failed parameter: %s' % (v,))
                raise ValueError('Only strings are accepted as command parameters')
            data[k] = v

        try:
            response = HTTPClient().fetch(url, method='POST',
                                               headers=self.minion_headers,
                                               body=urllib.urlencode(data),
                                               request_timeout=5.0,
                                               use_gzip=True)
        except HTTPError as e:
            response = e

        data = self._process_state(host, self._get_response(host, response))
        return data

    # history processing

    def _fetch_history(self):
        try:
            logger.info('Fetching minion commands history')
            now = datetime.datetime.now()

            for entry in self._history_entries(now):
                if not entry['uid'] in self.history:
                    logger.debug('Fetched history entry for command {0}'.format(entry['uid']))
                self.history[entry['uid']] = entry['progress']

            self.history_ready = True
            logger.info('Finished fetching minion commands history')
        except Exception as e:
            logger.info('Failed to fetch minion history')
            logger.exception(e)
        finally:
            self.__tq.add_task_in(self.HISTORY_FETCH,
                config.get('minions', {}).get('history_fetch_period', 120),
                self._fetch_history)

    def _history_entries(self, dt):
        key = keys.MINION_HISTORY_KEY % dt.strftime('%Y-%m')
        idxs = self.meta_session.find_all_indexes([key])
        for idx in idxs:
            data = idx.indexes[0].data
            entry = self._unserialize(data)
            yield entry

    def _history_entry_update(self, state):
        try:
            uid = state['uid']
            logger.debug('Started updating minion history entry '
                         'for command {0}'.format(uid))
            update_history_entry = False
            try:
                eid = self.meta_session.transform(keys.MINION_HISTORY_ENTRY_KEY % uid.encode('utf-8'))
                # set to read_latest when it raises NotFoundError on non-existent keys
                r = self.meta_session.read_data(eid).get()[0]
                history_state = self._unserialize(r.data)
                self.history[uid] = history_state['progress']
                # updating if process is finished
                if int(self.history[uid]) != int(state['progress']):
                    update_history_entry = True
            except elliptics.NotFoundError:
                logger.debug('History state is not found')
                update_history_entry = True

            if update_history_entry:
                logger.info('Updating minion history entry for command {0}'.format(uid))
                start = datetime.datetime.fromtimestamp(state['start_ts'])
                key = keys.MINION_HISTORY_KEY % start.strftime('%Y-%m')
                self.meta_session.update_indexes(eid, [key], [self._serialize(state)])
                self.history[uid] = state['progress']
            else:
                logger.debug('Update for minion history entry '
                             'of command {0} is not required'.format(uid))
        except Exception as e:
            logger.error('Failed to update minion history entry for '
                         'uid {0}: {1}\n{2}'.format(uid, str(e), traceback.format_exc()))

    @staticmethod
    def _unserialize(data):
        return msgpack.unpackb(data)

    @staticmethod
    def _serialize(data):
        return msgpack.packb(data)

    def minion_history_log(self, request):
        try:
            year, month = [int(_) for _ in request[0:2]]
        except ValueError:
            raise ValueError('Invalid parameters')

        dt = datetime.datetime(year, month, 1)

        entries = self._history_entries(dt)
        return sorted(entries, key=lambda e: e['start_ts'])


class AsyncHTTPBatch(object):

    def __init__(self, urls, headers=None, timeout=3):
        self.left = len(urls)
        self.urls = urls
        self.timeout = timeout
        self.headers = headers
        self.ioloop = IOLoop.current()
        logger.debug('Minion states, ioloop fetched: {0}'.format(self.ioloop))
        self.responses = {}

    def get(self, emergency_timeout=None):
        logger.debug('Minion states, creating async http clients')
        if not self.urls:
            logger.warn('Empty url list for async http batch')
            return self.responses
        max_clients = len(self.urls)
        [AsyncHTTPClient(max_clients=max_clients).fetch(url, callback=self._process,
            request_timeout=self.timeout, headers=self.headers) for url in self.urls]
        logger.debug('Minion states, starting ioloop')
        self.ioloop.start()
        return self.responses

    def _process(self, response):
        logger.debug('Minion states, received response from {0} '
                     '({1:.4f}s)'.format(response.request.url, response.request_time))
        self.responses[response.request.url] = response
        self.left -= 1
        if not self.left:
            logger.debug('Minion states, stopping loop')
            self.ioloop.stop()

    def _emergency_halt(self):
        logger.warn('Minion states, emergency exit. '
                    'Unprocessed requests: {0}'.format(self.left))
        self.ioloop.stop()

