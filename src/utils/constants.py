# Group 35 port calculation
# G = 35 (odd number)
# Formula: [(ceil(G/2) × 1000) + 500, (ceil(G/2) × 1000) + 999]
# ceil(35/2) = ceil(17.5) = 18
# Range: [18000 + 500, 18000 + 999] = [18500, 18999]

GROUP_NUMBER = 35
PORT_RANGE_START = 18500
PORT_RANGE_END = 18999

# Available ports for this group
AVAILABLE_PORTS = list(range(PORT_RANGE_START, PORT_RANGE_END + 1))

# Message constants
MAX_MESSAGE_SIZE = 1024
SOCKET_TIMEOUT = 30