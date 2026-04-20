import datetime
import logging
import os
import pathlib
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
import sys
import threading
import traceback

import requests


FMT = '%(asctime)s %(name)s %(process)d %(thread)d %(levelname)s %(message)s'
ENV_ALERT_URL = 'FEISHU_ALERT_URL'


def get_logger(name, level, dirpath_logs, logfile_basename):
    dirpath_logs = (
        pathlib.Path(dirpath_logs) if dirpath_logs is not None else None
    )
    logging.setLoggerClass(AlertLogger)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.init_alert()
    if not logger.handlers and dirpath_logs:
        handler = HybridRotatingFileHandler(logfile_basename, dirpath_logs)
        handler.setFormatter(logging.Formatter(FMT))
        logger.addHandler(handler)
    logger.info('logger.handlers=%s', logger.handlers)
    return logger


def get_project_root(start_path=__file__, max_levels=10):
    current_path = pathlib.Path(start_path).resolve()
    for _ in range(max_levels):
        # for dev
        pyproject_path = current_path / "pyproject.toml"
        if pyproject_path.exists() and pyproject_path.is_file():
            return current_path
        # for prod
        if current_path == current_path.parent:
            return pathlib.Path.cwd()
        current_path = current_path.parent
    # for prod
    return pathlib.Path.cwd()


def setup_mp_logging(log_queue, basename, dirpath_logs):
    handler = HybridRotatingFileHandler(basename, dirpath_logs)
    handler.setFormatter(logging.Formatter(FMT))
    listener = QueueListener(log_queue, handler)
    listener.start()
    root_logger = logging.getLogger()
    if not any(isinstance(h, QueueHandler) for h in root_logger.handlers):
        root_logger.addHandler(QueueHandler(log_queue))


def init_worker_logging(log_queue):
    for thread in threading.enumerate():
        if isinstance(thread, QueueListener):
            thread.stop()
            thread.join(timeout=0.5)
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        root_logger.removeHandler(handler)
    queue_handler = QueueHandler(log_queue)
    root_logger.addHandler(queue_handler)
    root_logger.propagate = True


class AlertLogger(logging.Logger):
    def __init__(self, logger_name):
        super().__init__(logger_name)
        self.alertbot_url = None

    def init_alert(self):
        url = os.environ.get(ENV_ALERT_URL)
        if url:
            url = url.strip()
            if url.startswith(('http://', 'https://')):
                self.alertbot_url = url
                self.debug('Alert URL loaded from env %s', ENV_ALERT_URL)
            else:
                self.warning('Invalid alert URL format, ignored: %s', url)
        if not self.alertbot_url:
            self.warning('alertbot_url not found')

    def alert(self, msg, *args, exc_info=True):
        if args:
            msg = msg % args
        if exc_info and sys.exc_info()[0] is not None:
            msg += '\n' + ''.join(traceback.format_exception(*sys.exc_info()))
        self.error('[Alert] %s', msg)
        if self.alertbot_url:
            try:
                requests.post(
                    url=self.alertbot_url,
                    json={'msg_type': 'text', 'content': {'text': msg}},
                    timeout=3.0
                )
            except Exception as e:
                str_e = type(e).__name__ + ': ' + str(e)
                self.error('Error in sending alert: %s', str_e)
        else:
            self.warning('Alert mechanism not configured, alert msg: %s', msg)


class HybridRotatingFileHandler(RotatingFileHandler):
    def __init__(
        self, basename, dirpath_logs,
        max_bytes=100*1024*1024, backup_count=30
    ):
        self.basename = basename
        self.current_date = datetime.datetime.now().strftime('%Y%m%d')
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.dirpath_logs = pathlib.Path(dirpath_logs)
        self.dirpath_logs.mkdir(exist_ok=True, parents=True)
        super().__init__(
            filename=self._get_fpath_log(),
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )

    def _get_fpath_log(self):
        return self.dirpath_logs / (
            f'{self.basename}.{self.current_date}.log' if self.basename else
            f'{self.current_date}.log'
        )

    def shouldRollover(self, record):
        current_date = datetime.datetime.now().strftime("%Y%m%d")
        date_changed = current_date != self.current_date
        size_exceeded = (
            self.stream is not None and
            self.stream.tell() >= self.maxBytes
        )
        return date_changed or size_exceeded

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
        current_date = datetime.datetime.now().strftime("%Y%m%d")
        if current_date != self.current_date:
            self.current_date = current_date
            self.baseFilename = self._get_fpath_log()
        else:
            if self.backup_count > 0:
                for i in range(self.backup_count-1, 0, -1):
                    sfn = self.baseFilename.with_suffix(f'.{i}.log')
                    dfn = self.baseFilename.with_suffix(f'.{i+1}.log')
                    if sfn.exists():
                        sfn.replace(dfn)
                dfn = self.baseFilename.with_suffix('.1.log')
                if self.baseFilename.exists():
                    self.baseFilename.replace(dfn)
        self.stream = self._open()
