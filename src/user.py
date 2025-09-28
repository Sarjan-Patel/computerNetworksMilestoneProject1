#!/usr/bin/env python3
import sys
import socket
import threading
from utils.constants import MAX_MESSAGE_SIZE
from utils.message import create_message, parse_message

class User:
    def __init__(self, name, manager_ip, manager_port, m_port, c_port):
        self.name = name
        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.m_port = m_port
        self.c_port = c_port
        self.m_sock = None
        self.c_sock = None
        self.running = True
        
    def start(self):
        """Start the user process"""
        try:
            # Create sockets
            self.m_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.c_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            
            # Bind to ports
            self.m_sock.bind(('', self.m_port))
            self.c_sock.bind(('', self.c_port))
            
            print(f"User {self.name} started on m_port={self.m_port}, c_port={self.c_port}")
            
            # Start threads
            m_thread = threading.Thread(target=self.management_handler)
            c_thread = threading.Thread(target=self.command_handler)
            
            m_thread.daemon = True
            c_thread.daemon = True
            
            m_thread.start()
            c_thread.start()
            
            # Register with manager
            self.register_with_manager()
            
            # Start command interface
            self.command_interface()
            
        except KeyboardInterrupt:
            print(f"\nUser {self.name} shutting down...")
            self.running = False
        finally:
            if self.m_sock:
                self.m_sock.close()
            if self.c_sock:
                self.c_sock.close()
    
    def management_handler(self):
        """Handle management port communications"""
        while self.running:
            try:
                self.m_sock.settimeout(1.0)
                data, addr = self.m_sock.recvfrom(MAX_MESSAGE_SIZE)
                print(f"Management message received from {addr}")
            except socket.timeout:
                continue
            except:
                break
    
    def command_handler(self):
        """Handle command port communications"""
        while self.running:
            try:
                self.c_sock.settimeout(1.0)
                data, addr = self.c_sock.recvfrom(MAX_MESSAGE_SIZE)
                print(f"Command message received from {addr}")
            except socket.timeout:
                continue
            except:
                break
    
    def register_with_manager(self):
        """Register this user with the manager"""
        message = create_message("register-user", {
            "user_name": self.name,
            "ipv4_addr": "127.0.0.1",  # localhost for now
            "m_port": self.m_port,
            "c_port": self.c_port
        }, self.name)
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.sendto(message.encode(), (self.manager_ip, self.manager_port))
            response, _ = sock.recvfrom(MAX_MESSAGE_SIZE)
            print(f"Registration response: {response.decode()}")
        finally:
            sock.close()
    
    def send_to_manager(self, command, params):
        """Send command to manager and get response"""
        message = create_message(command, params, self.name)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.sendto(message.encode(), (self.manager_ip, self.manager_port))
            response, _ = sock.recvfrom(MAX_MESSAGE_SIZE)
            return parse_message(response.decode())
        finally:
            sock.close()
    
    def command_interface(self):
        """Command line interface for user commands"""
        print(f"User {self.name} ready. Available commands:")
        print("  configure-dss <dss_name> <n> <striping_unit>")
        print("  deregister-user")
        print("  quit")
        
        while self.running:
            try:
                command = input(f"{self.name}> ").strip()
                if command.lower() == 'quit':
                    break
                elif command.startswith("configure-dss"):
                    self.handle_configure_dss(command)
                elif command == "deregister-user":
                    self.handle_deregister_user()
                elif command:
                    print(f"Unknown command: {command}")
            except (EOFError, KeyboardInterrupt):
                break
    
    def handle_configure_dss(self, command):
        """Handle configure-dss command"""
        parts = command.split()
        if len(parts) != 4:
            print("Usage: configure-dss <dss_name> <n> <striping_unit>")
            return
        
        try:
            dss_name = parts[1]
            n = int(parts[2])
            striping_unit = int(parts[3])
            
            response = self.send_to_manager("configure-dss", {
                "dss_name": dss_name,
                "n": n,
                "striping_unit": striping_unit
            })
            
            if response:
                print(f"DSS configuration: {response['status']}")
                if response.get("message"):
                    print(f"Message: {response['message']}")
            
        except ValueError:
            print("Error: n and striping_unit must be integers")
    
    def handle_deregister_user(self):
        """Handle user deregistration"""
        response = self.send_to_manager("deregister-user", {
            "user_name": self.name
        })
        
        if response:
            print(f"Deregistration: {response['status']}")
            if response.get("message"):
                print(f"Message: {response['message']}")
            
            if response["status"] == "SUCCESS":
                self.running = False

def main():
    if len(sys.argv) != 6:
        print("Usage: python user.py <name> <manager_ip> <manager_port> <m_port> <c_port>")
        sys.exit(1)
    
    try:
        name = sys.argv[1]
        manager_ip = sys.argv[2]
        manager_port = int(sys.argv[3])
        m_port = int(sys.argv[4])
        c_port = int(sys.argv[5])
        
        user = User(name, manager_ip, manager_port, m_port, c_port)
        user.start()
    except ValueError:
        print("Error: Ports must be integers")
        sys.exit(1)

if __name__ == "__main__":
    main()