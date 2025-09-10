# LoggerManager

`LoggerManager` sets up a logging system that logs messages to a file, with options to add console and rotating file handlers.

## Class Overview

### Attributes

- `log_dir (str)`: Directory where log files will be saved.
- `log_level (int)`: Logging level.
- `log_filename (str)`: Generated log filename based on the current datetime.
- `logger (logging.Logger)`: The logger instance for the class.

### Methods

- `__init__(self, log_dir='logs', log_level=logging.DEBUG)`: Initializes the LoggerManager with the specified log directory and log level.
- `generate_log_filename(self)`: Generates a log filename based on the current datetime.
- `setup_logging(self)`: Sets up the logging configuration, ensuring the log directory exists and configuring the root logger to log messages to a file.
- `get_logger(self, name=None)`: Retrieves a logger by name or returns the default logger.
- `add_console_handler(self)`: Adds a console handler to the root logger to output log messages to the console.
- `add_rotating_file_handler(self, max_bytes=10485760, backup_count=5)`: Adds a rotating file handler to the root logger to manage log file sizes.
- `add_handler(self, handler, format_str)`: Adds a specified handler to the root logger with the given format.
- `get_log_filename(self)`: Retrieves the generated log filename.

## Usage

```python
from logger_manager import LoggerManager

# Initialize the LoggerManager
logger_manager = LoggerManager()

# Setup logging
logger_manager.setup_logging()

# Get the logger
logger = logger_manager.get_logger()

# Log a message
logger.info('This is an info message')
```

## Example Output

```
2021-10-01 12:00:00,000 - INFO - This is an info message
```

## Notes

- The log directory will be created if it does not exist.
- By default, log messages are logged to a file with the name `log_<current_datetime>.log`.
- The log level can be set to control which messages are logged (e.g., `logging.DEBUG`, `logging.INFO`, `logging.WARNING`, `logging.ERROR`, `logging.CRITICAL`).
- The log filename can be retrieved using the `get_log_filename` method.
- Additional handlers can be added to the root logger to output log messages to the console or manage log file sizes.
- The logger can be retrieved by name to create separate loggers for different modules or classes.

## References

- [Python Logging Documentation](https://docs.python.org/3/library/logging.html)
- [Python Logging Cookbook](https://docs.python.org/3/howto/logging-cookbook.html)
