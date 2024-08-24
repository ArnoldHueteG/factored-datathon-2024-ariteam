import logging

def setup_logger(name):
    # Set the root logger to WARNING level
    logging.getLogger().setLevel(logging.WARNING)

    # Create a logger for the given name
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Create a console handler and set its level to DEBUG
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # Create a formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add formatter to ch
    ch.setFormatter(formatter)

    # Add ch to logger
    logger.addHandler(ch)

    return logger