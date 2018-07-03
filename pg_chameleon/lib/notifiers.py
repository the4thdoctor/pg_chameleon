import os
import sys
import rollbar
from emails import Message


class email_notifier(object):
    def __init__(self, attributes, level, logger):
        self.levels = {
            "critical": 1,
            "error": 2,
            "warning": 3,
            "info": 5
        }
        self.alert_subject = attributes["subject"]
        self.notify_level = self.levels[level]
        self.logger = logger
        self.mail_from = attributes["from"]

        smtp_pars = attributes["smtp"].split(":")

        if len(smtp_pars) == 1:
            smtp_server = smtp_pars[0]
            smtp_port = 25
        else:
            smtp_server = smtp_pars[0]
            smtp_port = smtp_pars[1]

        if "credentials" in attributes:
            creds_pars = attributes["credentials"].split("/")
            if len(creds_pars) < 2:
                raise ValueError("Config error in Notifier section - provide credentials: user/pass")
            login = creds_pars[0]
            password = creds_pars[1]
        else:
            login = ""
            password = ""
        ssl = attributes["ssl"] if "ssl" in attributes else False
        self.receivers = attributes["to"]
        self.mail_conf = {'host': smtp_server, 'port': smtp_port, 'user': login, 'password': password, 'ssl': ssl}

    def send_message(self, message, level):
        """
            The method sends an email. If it fails it just logs an error
            without causing the process to crash.
        """
        try:
            notification_level = self.levels[level]
            if notification_level <= self.notify_level:
                try:
                    m = Message(mail_from=(self.mail_from), subject=self.alert_subject, text=message)
                    m.send(to=self.receivers, smtp=self.mail_conf)
                except:
                    self.logger.error("Could not send the alert email.")
        except:
            self.logger.error("Wrong notification level specified.")



class rollbar_notifier(object):
    """
        This class is used to send messages to rollbar whether the key and environment variables are set
    """

    def __init__(self, rollbar_key, rollbar_env, rollbar_level, logger):
        """
            Class constructor.
        """
        self.levels = {
            "critical": 1,
            "error": 2,
            "warning": 3,
            "info": 5
        }
        self.rollbar_level = self.levels[rollbar_level]
        self.logger = logger
        self.notifier = rollbar
        if rollbar_key != '' and rollbar_env != '':
            self.notifier.init(rollbar_key, rollbar_env)
        else:
            self.notifier = None

    def send_message(self, message, level):
        """
            The method sends a message to rollbar. If it fails it just logs an error
            without causing the process to crash.
        """
        exc_info = sys.exc_info()
        try:
            notification_level = self.levels[level]
            if notification_level <= self.rollbar_level:
                try:
                    self.notifier.report_message(message, level)
                    if exc_info[0]:
                        self.notifier.report_exc_info(exc_info)
                except:
                    self.logger.error("Could not send the message to rollbar.")
        except:
            self.logger.error("Wrong rollbar level specified.")