"""
Centralized logging configuration for the pipeline.

- Standard format
- Consistent log levels
- Easy to extend later (file handlers, JSON logs, etc.)
"""

import logging


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
