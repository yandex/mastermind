#!/usr/bin/env python
# coding=utf-8
"""
This utility monitors file system with the occurrence of some event (create file, modify file,
remove file, etc) can run a certain script.

In this case, the script deploy mastermind and related applications in cocaine-runtime.

To ensure that a deploy script doesn't run so often debounce period sets.
Events are logged relative to the current directory.

To specify script path or debounce time see the help information by typing:
./auto_deploy.py --help
"""

from __future__ import unicode_literals

import argparse
import logging
import subprocess
import threading
import time
from threading import Timer

from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

logging.basicConfig(level=logging.DEBUG)


def debounce(wait):
    def decorator(fn):
        def debounced(*args, **kwargs):
            def call_it():
                fn(*args, **kwargs)

            try:
                debounced.timer.cancel()
            except AttributeError:
                pass
            debounced.timer = Timer(wait, call_it)
            debounced.timer.start()

        return debounced

    return decorator


class DeployEventHandler(PatternMatchingEventHandler):
    DEFAULT_UPLOAD_SCRIPT = 'cocaine-app/upload.sh'
    DEFAULT_DEBOUNCE_TIME = 1.5
    _lock = threading.Lock()

    def __init__(self, debounce_time=DEFAULT_DEBOUNCE_TIME, upload_script=DEFAULT_UPLOAD_SCRIPT,
                 *args, **kwargs):
        self.upload_script = upload_script
        self.debounced_deploy = debounce(debounce_time)(self._deploy)
        super(DeployEventHandler, self).__init__(*args, **kwargs)

    def _deploy(self):
        with self._lock:
            subprocess.call(self.upload_script, shell=True)

    def on_any_event(self, event):
        self.debounced_deploy()


def parse_args(args=None):
    parser = argparse.ArgumentParser(description='Script to auto deploy mastermind code')
    parser.add_argument(
        '--upload_script',
        default=DeployEventHandler.DEFAULT_UPLOAD_SCRIPT,
        help='Path to upload script'
    )
    parser.add_argument(
        '--debounce',
        default=DeployEventHandler.DEFAULT_DEBOUNCE_TIME,
        type=float,
        help='Debounce time'
    )
    return parser.parse_args(args)


def main():
    args = parse_args()
    event_handler = DeployEventHandler(
        debounce_time=args.debounce,
        upload_script=args.upload_script,
        patterns=('*.py', '*.manifest', '*.profile')
    )
    observer = Observer()
    observer.schedule(event_handler, '.', recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == '__main__':
    main()
