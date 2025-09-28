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
        else:
            return create_response("FAILURE", "Unknown command")
    
    def register_user(self, params):
        """Handle user registration"""
        # Basic implementation - will be expanded
        return create_response("SUCCESS", "User registration placeholder")
    
    def register_disk(self, params):
        """Handle disk registration"""
        # Basic implementation - will be expanded
        return create_response("SUCCESS", "Disk registration placeholder")

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