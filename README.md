# Distributed Storage System (DSS) Project

## Group Information
- **Group Number**: 35
- **Port Range**: 18500-18999 (assigned based on group number formula)

## Project Structure
```
src/
├── manager.py          # Manager process (single-threaded)
├── disk.py            # Disk process (multi-threaded)
├── user.py            # User process (multi-threaded)
└── utils/
    ├── __init__.py
    ├── constants.py    # Port range and system constants
    └── message.py      # Message protocol utilities
```

## Usage

### Manager
```bash
python src/manager.py <port>
```

### Disk
```bash
python src/disk.py <name> <manager_ip> <manager_port> <m_port> <c_port>
```

### User
```bash
python src/user.py <name> <manager_ip> <manager_port> <m_port> <c_port>
```

## Port Assignment for Group 35
- Port range: 18500-18999 (500 ports total)
- Formula used: [(ceil(35/2) × 1000) + 500, (ceil(35/2) × 1000) + 999] = [18500, 18999]

## Current Implementation Status
- Basic project structure
- Port calculation for group 35
- UDP socket setup for all programs
- Basic message protocol (JSON format)
- Multi-threading framework for disk and user processes
- Command implementations (next phase)