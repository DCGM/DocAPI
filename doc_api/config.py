import os
import logging
import time

class UTCFormatter(logging.Formatter):
    converter = time.gmtime


TRUE_VALUES = {"true", "1"}


class Config:
    def __init__(self):
        self.APP_URL_ROOT = os.getenv("APP_URL_ROOT", "/")
        self.BASE_DIR = os.getenv("BASE_DIR", "./metakat_data")
        self.ADMIN_SERVER_NAME = os.getenv("ADMIN_SERVER_NAME", "MetaKat")
        self.SERVER_NAME = os.getenv("SERVER_NAME", "MetaKat")

        self.SOFTWARE_CREATOR = os.getenv("SOFTWARE_CREATOR", "MetaKat")
        self.SOFTWARE_VERSION = os.getenv("SOFTWARE_VERSION", "1.0")

        self.ADMIN_KEY = os.getenv("ADMIN_KEY", "adminkey")
        self.HMAC_SECRET = os.getenv("HMAC_SECRET", "hmacsecret")

        # server | worker
        self.MODE = os.getenv("MODE", 'server')

        self.ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "username")
        self.ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "pass")

        self.DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:pass@localhost:5432/metakat_db")
        self.UPDATE_TRIGGERS = os.getenv("UPDATE_TRIGGERS", str(False)).lower() in TRUE_VALUES


        self.IMAGE_UPLOADED_DIR = os.getenv("IMAGE_UPLOADED_DIR", os.path.join(self.BASE_DIR, "server", "image_uploaded"))
        self.WORKER_DIR = os.getenv("WORKER_DIR", os.path.join(self.BASE_DIR, "worker"))

        # how often workers check DB, time in sec
        self.WORKERS_DB_FETCH_INTERVAL = os.getenv("WORKERS_DB_FETCH_INTERVAL", "5")

        self.WORKING_DIR = os.getenv("WORKING_DIR", f'/tmp/metakat.{self.MODE}')

        self.PRODUCTION = os.getenv("PRODUCTION", False)

        # EMAILS and NOTIFICATIONS configuration
        ################################################################################################################

        # Internal mailing setting for metakat.internal_mail_logger
        self.INTERNAL_MAIL_SERVER = os.getenv("INTERNAL_MAIL_SERVER", None)
        self.INTERNAL_MAIL_PORT = os.getenv("INTERNAL_MAIL_PORT", 25)
        self.INTERNAL_MAIL_SENDER_NAME = os.getenv("INTERNAL_MAIL_SENDER_NAME", "MetaKat")
        self.INTERNAL_MAIL_SENDER_MAIL = os.getenv("INTERNAL_MAIL_SENDER_MAIL", None)
        self.INTERNAL_MAIL_PASSWORD = os.getenv("INTERNAL_MAIL_PASSWORD", None)
        if os.getenv("INTERNAL_MAIL_RECEIVER_MAILS") is not None:
            self.INTERNAL_MAIL_RECEIVER_MAILS = [e.strip() for e in
                                                 os.getenv("INTERNAL_MAIL_RECEIVER_MAILS").split(',')]
        else:
            self.INTERNAL_MAIL_RECEIVER_MAILS = ['user@mail.server.cz']
        self.INTERNAL_MAIL_FLOOD_LEVEL = int(os.getenv("INTERNAL_MAIL_FLOOD_LEVEL", 10))

        # External mailing setting for metakat.external_mail_logger
        self.EXTERNAL_MAIL_SERVER = os.getenv("EXTERNAL_MAIL_SERVER", None)
        self.EXTERNAL_MAIL_PORT = os.getenv("EXTERNAL_MAIL_PORT", 25)
        self.EXTERNAL_MAIL_SENDER_NAME = os.getenv("EXTERNAL_MAIL_SENDER_NAME", "MetaKat")
        self.EXTERNAL_MAIL_SENDER_MAIL = os.getenv("EXTERNAL_MAIL_SENDER_MAIL", None)
        self.EXTERNAL_MAIL_PASSWORD = os.getenv("EXTERNAL_MAIL_PASSWORD", None)
        self.EXTERNAL_MAIL_FLOOD_LEVEL = int(os.getenv("EXTERNAL_MAIL_FLOOD_LEVEL", 0))

        # LOGGING configuration
        ################################################################################################################
        self.LOGGING_CONSOLE_LEVEL = os.getenv("LOGGING_CONSOLE_LEVEL", logging.INFO)
        self.LOGGING_FILE_LEVEL = os.getenv("LOGGING_FILE_LEVEL", logging.INFO)
        self.LOGGING_DIR = os.getenv("LOGGING_DIR", os.path.join(self.BASE_DIR, self.MODE, "logs"))
        self.LOGGING_CONFIG = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'base': {
                    '()': UTCFormatter,
                    'format': '%(asctime)s : %(name)s : %(levelname)s : %(message)s'
                }
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': self.LOGGING_CONSOLE_LEVEL,
                    'formatter': 'base',
                    'stream': 'ext://sys.stdout'
                },
                'file_log': {
                    'class': 'logging.handlers.TimedRotatingFileHandler',
                    'level': self.LOGGING_FILE_LEVEL,
                    'when': 'midnight',
                    'utc': True,
                    'formatter': 'base',
                    'filename': os.path.join(self.LOGGING_DIR, f'{self.MODE}.log')
                }
            },
            'loggers': {
                'root': {
                    'level': 'DEBUG',
                    'handlers': [
                        'console',
                        'file_log',
                    ]
                },
                'metakat.exception_logger': {
                    'level': 'DEBUG',
                    'handlers': [
                        'file_log'
                    ]
                },
                'multipart.multipart': {
                    'level': 'INFO'
                }
            }
        }
        ################################################################################################################

    def create_dirs(self):
        os.makedirs(self.IMAGE_UPLOADED_DIR, exist_ok=True)
        os.makedirs(self.WORKER_DIR, exist_ok=True)
        os.makedirs(self.WORKING_DIR, exist_ok=True)
        os.makedirs(self.LOGGING_DIR, exist_ok=True)


config = Config()
config.create_dirs()


