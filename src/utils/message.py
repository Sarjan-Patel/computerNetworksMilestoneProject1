import json

def create_message(command, parameters, sender=None):
    """Create a JSON message"""
    message = {
        "command": command,
        "parameters": parameters
    }
    if sender:
        message["sender"] = sender
    return json.dumps(message)

def parse_message(message_str):
    """Parse JSON message"""
    try:
        return json.loads(message_str)
    except json.JSONDecodeError:
        return None

def create_response(status, message=None, data=None):
    """Create JSON response"""
    response = {"status": status}
    if message:
        response["message"] = message
    if data:
        response["data"] = data
    return json.dumps(response)

def encode_block(block_data):
    """Convert binary data to base64 for JSON"""
    import base64
    return base64.b64encode(block_data).decode('utf-8')

def decode_block(encoded_data):
    """Convert base64 back to binary data"""
    import base64
    return base64.b64decode(encoded_data.encode('utf-8'))