import os
import logging
from logging.handlers import TimedRotatingFileHandler
from config import LOGGER_PATH 
# 
FORMAT = '[%(asctime)-15s][%(filename)s:%(lineno)d][%(levelname)s] %(message)s'
loggers = {}

if not os.path.exists("logger"):
    os.mkdir("logger")
 
def setup_logger(name, log_file, level=logging.DEBUG):
    """
    This function sets up a logger with the given name and log file.

    Parameters
    ----------
    name : str
        The name of the logger.
    log_file : str
        The path to the log file.
    level : int
        The logging level.

    Returns
    -------
    logger: The logger object.
    """
    if loggers.get(name):
        return loggers.get(name)
   
    formatter = logging.Formatter(FORMAT)

    # handler = RotatingFileHandler(log_file, mode='a', when='midnight', maxBytes=50*1024*1024,
    #                                  backupCount=10, encoding=None, delay=0)
    handler = TimedRotatingFileHandler(log_file, when='midnight', backupCount=5)
    handler.setFormatter(formatter)
    handler.setLevel(level)
    logger2 = logging.getLogger(name)
    logger2.setLevel(level)
    logger2.addHandler(handler)
    loggers[name] = logger2
    return logger2

def setup_logger_global(name, log_file, level=logging.DEBUG):
    """
    Setup a global logger with the given name and log file.

    Parameters:
        name (str): The name of the logger.
        log_file (str): The path to the log file.
        level (int, optional): The logging level. Defaults to logging.DEBUG.

    Returns:
        logger: The logger object.
    """
    return setup_logger(name, LOGGER_PATH +log_file, level)
