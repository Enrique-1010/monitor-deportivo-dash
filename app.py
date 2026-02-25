import logging

# Set up logging
logging.basicConfig(level=logging.INFO)


def check_user_session(session):
    if 'user_id' not in session:
        logging.warning("User is not logged in.")
        return False
    logging.info("User session is valid.")
    return True


def error_alert(error_message):
    logging.error(f'An error occurred: {error_message}')
    # You can add more functionality here such as notifying admins or systems


def on_data_received(data):
    try:
        # Process the received data
        logging.info("Data received: %s", data)
        # ... Your logic here
    except Exception as e:
        error_alert(str(e))


def main_callback(session, data):
    if not check_user_session(session):
        error_alert("User session invalid in main_callback.")
        return
    on_data_received(data)

# Additional restructuring and improvement
# ... Add more organized callbacks and processing functions