#!/usr/bin/env python3
import sys
import socket
from utils.constants import MAX_MESSAGE_SIZE
from utils.message import parse_message, create_response

class Manager:
    def __init__(self, port):
        self.port = port
        self.sock = None
        self.registered_users = {}
        self.registered_disks = {}
        self.configured_dsses = {}
        
    def start(self):
        """Start the manager process"""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.bind(('', self.port))
            print(f"Manager listening on port {self.port}")
            
            while True:
                data, addr = self.sock.recvfrom(MAX_MESSAGE_SIZE)
                message = parse_message(data.decode())
                if message:
                    response = self.handle_message(message, addr)
                    self.sock.sendto(response.encode(), addr)
                    
        except KeyboardInterrupt:
            print("\nManager shutting down...")
        finally:
            if self.sock:
                self.sock.close()
    
    def handle_message(self, message, addr):
        """Handle incoming messages"""
        command = message.get("command")
        print(f"Received command: {command} from {addr}")
        
        if command == "register-user":
            return self.register_user(message.get("parameters", {}))
        elif command == "register-disk":
            return self.register_disk(message.get("parameters", {}))
        elif command == "configure-dss":
            return self.configure_dss(message.get("parameters", {}))
        elif command == "deregister-user":
            return self.deregister_user(message.get("parameters", {}))
        elif command == "deregister-disk":
            return self.deregister_disk(message.get("parameters", {}))
        else:
            return create_response("FAILURE", "Unknown command")
    
    def register_user(self, params):
        """Handle user registration"""
        user_name = params.get("user_name")
        ipv4_addr = params.get("ipv4_addr")
        m_port = params.get("m_port")
        c_port = params.get("c_port")
        
        # Validate parameters
        if not all([user_name, ipv4_addr, m_port, c_port]):
            return create_response("FAILURE", "Missing required parameters")
        
        if not isinstance(user_name, str) or len(user_name) > 15 or not user_name.isalnum():
            return create_response("FAILURE", "Invalid user name")
        
        # Check for duplicate user name
        if user_name in self.registered_users:
            return create_response("FAILURE", "User name already registered")
        
        # Check for port conflicts
        for existing_user in self.registered_users.values():
            if existing_user["m_port"] == m_port or existing_user["c_port"] == c_port:
                return create_response("FAILURE", "Port already in use")
        
        for existing_disk in self.registered_disks.values():
            if existing_disk["m_port"] == m_port or existing_disk["c_port"] == c_port:
                return create_response("FAILURE", "Port already in use")
        
        # Register user
        self.registered_users[user_name] = {
            "user_name": user_name,
            "ipv4_addr": ipv4_addr,
            "m_port": m_port,
            "c_port": c_port
        }
        
        print(f"User {user_name} registered successfully")
        return create_response("SUCCESS")
    
    def register_disk(self, params):
        """Handle disk registration"""
        disk_name = params.get("disk_name")
        ipv4_addr = params.get("ipv4_addr")
        m_port = params.get("m_port")
        c_port = params.get("c_port")
        
        # Validate parameters
        if not all([disk_name, ipv4_addr, m_port, c_port]):
            return create_response("FAILURE", "Missing required parameters")
        
        if not isinstance(disk_name, str) or len(disk_name) > 15 or not disk_name.isalnum():
            return create_response("FAILURE", "Invalid disk name")
        
        # Check for duplicate disk name
        if disk_name in self.registered_disks:
            return create_response("FAILURE", "Disk name already registered")
        
        # Check for port conflicts
        for existing_user in self.registered_users.values():
            if existing_user["m_port"] == m_port or existing_user["c_port"] == c_port:
                return create_response("FAILURE", "Port already in use")
        
        for existing_disk in self.registered_disks.values():
            if existing_disk["m_port"] == m_port or existing_disk["c_port"] == c_port:
                return create_response("FAILURE", "Port already in use")
        
        # Register disk with Free state
        self.registered_disks[disk_name] = {
            "disk_name": disk_name,
            "ipv4_addr": ipv4_addr,
            "m_port": m_port,
            "c_port": c_port,
            "state": "Free"
        }
        
        print(f"Disk {disk_name} registered successfully with state Free")
        return create_response("SUCCESS")
    
    def configure_dss(self, params):
        """Handle DSS configuration"""
        dss_name = params.get("dss_name")
        n = params.get("n")
        striping_unit = params.get("striping_unit")
        
        # Validate parameters
        if not all([dss_name, n, striping_unit]):
            return create_response("FAILURE", "Missing required parameters")
        
        if not isinstance(dss_name, str) or len(dss_name) > 15 or not dss_name.isalnum():
            return create_response("FAILURE", "Invalid DSS name")
        
        if not isinstance(n, int) or n < 3:
            return create_response("FAILURE", "n must be >= 3")
        
        # Check if striping_unit is power of 2 between 128 bytes and 1 MB
        if not isinstance(striping_unit, int) or striping_unit < 128 or striping_unit > 1048576:
            return create_response("FAILURE", "Invalid striping unit size")
        
        if striping_unit & (striping_unit - 1) != 0:
            return create_response("FAILURE", "Striping unit must be power of 2")
        
        # Check if DSS name already exists
        if dss_name in self.configured_dsses:
            return create_response("FAILURE", "DSS name already exists")
        
        # Count free disks
        free_disks = [disk_name for disk_name, disk_info in self.registered_disks.items() 
                     if disk_info["state"] == "Free"]
        
        if len(free_disks) < n:
            return create_response("FAILURE", "Insufficient free disks")
        
        # Select n disks randomly and update their state
        import random
        selected_disks = random.sample(free_disks, n)
        
        for disk_name in selected_disks:
            self.registered_disks[disk_name]["state"] = "InDSS"
            self.registered_disks[disk_name]["dss_name"] = dss_name
        
        # Configure DSS
        self.configured_dsses[dss_name] = {
            "dss_name": dss_name,
            "n": n,
            "striping_unit": striping_unit,
            "disks": selected_disks,
            "files": {}
        }
        
        print(f"DSS {dss_name} configured with {n} disks: {selected_disks}")
        return create_response("SUCCESS")
    
    def deregister_user(self, params):
        """Handle user deregistration"""
        user_name = params.get("user_name")
        
        if not user_name:
            return create_response("FAILURE", "Missing user name")
        
        if user_name not in self.registered_users:
            return create_response("FAILURE", "User not found")
        
        # Remove user
        del self.registered_users[user_name]
        print(f"User {user_name} deregistered successfully")
        return create_response("SUCCESS")
    
    def deregister_disk(self, params):
        """Handle disk deregistration"""
        disk_name = params.get("disk_name")
        
        if not disk_name:
            return create_response("FAILURE", "Missing disk name")
        
        if disk_name not in self.registered_disks:
            return create_response("FAILURE", "Disk not found")
        
        if self.registered_disks[disk_name]["state"] != "Free":
            return create_response("FAILURE", "Disk is in use (InDSS state)")
        
        # Remove disk
        del self.registered_disks[disk_name]
        print(f"Disk {disk_name} deregistered successfully")
        return create_response("SUCCESS")

def main():
    if len(sys.argv) != 2:
        print("Usage: python manager.py <port>")
        sys.exit(1)
    
    try:
        port = int(sys.argv[1])
        manager = Manager(port)
        manager.start()
    except ValueError:
        print("Error: Port must be an integer")
        sys.exit(1)

if __name__ == "__main__":
    main()