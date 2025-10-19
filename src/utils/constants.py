# Port range for Group 35
# Calculated as [18500, 18999] based on group number

GROUP_NUMBER = 35
PORT_RANGE_START = 18500
PORT_RANGE_END = 18999

# Ports we can use
AVAILABLE_PORTS = list(range(PORT_RANGE_START, PORT_RANGE_END + 1))

# Message constants
MAX_MESSAGE_SIZE = 8192  # Big enough for encoded block data
SOCKET_TIMEOUT = 30