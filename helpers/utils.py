import os
import random
import re
import time
import logging
from functools import wraps
from datetime import timedelta
from typing import List, Dict, Any
from .logger_manager import LoggerManager
from jinja2 import Environment, FileSystemLoader

def setup_logger(name: str) -> logging.Logger:
    """
    Sets up and returns a logger with the specified name.

    Args:
        name (str): The name of the logger.

    Returns:
        logging.Logger: Configured logger instance.
    """
    # Initialize the logger manager
    logger_manager = LoggerManager()

    # Get the logger with the specified name
    logger = logger_manager.get_logger(name)
    return logger

def timed(func):
    """
    Decorator for logging the execution time of a function.

    This decorator measures the time it takes for the decorated function to run and logs the duration.

    Args:
        func (function): The function to be decorated.

    Returns:
        function: A wrapped version of the original function that logs execution time.

    Example:
        @timed
        def process_data():
            # Function logic
            pass
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Record start time
        result = func(*args, **kwargs)  # Execute the function
        elapsed_time = time.time() - start_time  # Calculate elapsed time
        logging.info(f"{func.__name__} took {elapsed_time:.2f} seconds")  # Log execution time
        return result  # Return the result of the function
    return wrapper

def retry(max_retries=3, delay=1, backoff=2, max_delay=None, jitter=0.5, exceptions=(Exception,), logger=None, on_failure=None):
    """
    Decorator for retrying a function with exponential backoff and optional jitter.

    Args:
        max_retries (int): Maximum number of retry attempts. Default is 3.
        delay (int or float): Initial delay between retries in seconds. Default is 1.
        backoff (int or float): Factor by which the delay is multiplied after each retry. Default is 2.
        max_delay (int or float): Maximum delay between retries in seconds. If None, no limit. Default is None.
        jitter (int or float): Random jitter added to delay (to avoid retry synchronization). Default is 0.5.
        exceptions (tuple): Tuple of exception classes to catch and retry on. Default is (Exception,).
        logger (logging.Logger): Logger instance for logging. Default is None, which will use the root logger.
        on_failure (callable): Optional callback function executed after final retry failure. Default is None.

    Returns:
        function: A wrapped version of the original function with retry logic.

    Raises:
        Exception: Reraise the last exception encountered if max_retries is exceeded.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            current_delay = delay
            log = logger or logging  # Use provided logger or root logger

            while attempt < max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    log.error(f"Attempt {attempt} failed for {func.__name__} with args={args}, kwargs={kwargs}. Error: {e}")

                    if attempt >= max_retries:
                        log.error(f"Max retries exceeded for function {func.__name__}")
                        if on_failure:
                            on_failure(e, *args, **kwargs)  # Call failure handler
                        raise

                    # Add random jitter to avoid synchronized retries
                    jitter_value = random.uniform(0, jitter)
                    sleep_time = current_delay + jitter_value

                    # Cap the sleep time if max_delay is specified
                    if max_delay:
                        sleep_time = min(sleep_time, max_delay)

                    log.info(f"Retrying in {sleep_time:.2f} seconds (attempt {attempt}/{max_retries})...")
                    time.sleep(sleep_time)
                    current_delay *= backoff  # Exponentially increase the delay

        return wrapper
    return decorator

def is_valid_email(email: str) -> bool:
    """
    Validate an email address using regex.

    Args:
        email (str): The email address to validate.

    Returns:
        bool: True if the email address is valid, False otherwise.
    """
    # Regex for validating an email address
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def is_image_file(filepath: str) -> bool:
    """
    Check if a file is an image based on its extension.

    Args:
        filepath (str): The path of the file to check.

    Returns:
        bool: True if the file is an image, False otherwise.
    """
    # Define a set of common image file extensions
    image_extensions = {'.png', '.jpg', '.jpeg', '.gif', '.bmp', '.tiff', '.webp'}

    # Extract the file extension and check if it is in the set
    _, ext = os.path.splitext(filepath)
    return ext.lower() in image_extensions

def remove_keys(list_of_dicts: List[Dict[str, Any]]) -> List[List[Any]]:
    """
    Convert a list of dictionaries into a list of lists containing values from each dictionary.

    Each inner list contains the values from a single dictionary, with keys excluded.

    Args:
        list_of_dicts (List[Dict[str, Any]]): A list where each element is a dictionary.

    Returns:
        List[List[Any]]: A list of lists, where each inner list contains the values from a dictionary.
    """
    # Convert each dictionary's values to a list and collect them in a list
    list_of_lists = [list(dictionary.values()) for dictionary in list_of_dicts]

    return list_of_lists

def get_keys(dicts: List[Dict[str, Any]]) -> List[str]:
    """
    Extracts unique keys from a list of dictionaries while maintaining the original order of their first appearance.

    Args:
        dicts (List[Dict[str, Any]]): A list of dictionaries from which to extract keys.

    Returns:
        List[str]: A list of unique keys in the order they first appeared in the input dictionaries.
    """
    # Initialize an empty set to keep track of seen keys
    seen_keys = set()

    # Initialize an empty list to maintain the order of keys
    ordered_keys = []

    # Iterate over each dictionary in the list
    for dictionary in dicts:
        # Iterate over each key in the current dictionary
        for key in dictionary:
            # Use the set to check for uniqueness
            if key not in seen_keys:
                seen_keys.add(key)
                ordered_keys.append(key)

    # Return the list of unique keys in the order they were first encountered
    return ordered_keys

def date_range(start_date, end_date):
    """
    Generates a list of dates from start_date to end_date (inclusive).

    Args:
        start_date (datetime): The starting date.
        end_date (datetime): The ending date.

    Returns:
        List[datetime]: A list of datetime objects between the start and end dates.
    """
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)


# Function to generate the HTML alert
def generate_alert(alert_type, alert_title, alert_message, file_names=None, alert_link=None):
    """
    Generate an HTML alert message using a Jinja template.

    Args:
        alert_type (str): The type of alert (e.g., 'warning', 'danger', 'info', 'success').
        alert_title (str): The title of the alert message.
        alert_message (str): The main content of the alert message.

    Returns:
        str: An HTML string representing the alert message.
    """

    # Set the color based on the type of alert
    if alert_type == 'success':
        alert_color = '#28a745'  # Green to success
    elif alert_type == 'warning':
        alert_color = '#ffc107'  # Yellow for warning
    elif alert_type == 'danger':
        alert_color = '#dc3545'  # Red for error
    else:
        alert_color = '#333333'  # Standard color if the type is unknown

    # Load the Jinja template for the alert
    template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('alert_template.html')

    # Render the template with the provided data
    html_output = template.render(
        title='Alerta de Processos ETL',
        alert_type=alert_type,
        alert_title=alert_title,
        alert_message=alert_message,
        file_names=file_names,
        alert_link=alert_link,
        alert_color=alert_color
    )

    return html_output
