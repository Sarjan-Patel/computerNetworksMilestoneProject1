#!/usr/bin/env python3
import sys
import socket
import threading
import time
from utils.constants import MAX_MESSAGE_SIZE
from utils.message import create_message, parse_message, create_response

class Disk:
    def __init__(self, name, manager_ip, manager_port, m_port, c_port):
        self.name = name
        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.m_port = m_port
        self.c_port = c_port
        self.m_sock = None
        self.c_sock = None
        self.running = True
        self.storage = {}  # {dss_name: {file_name: {stripe_num: (block_data, block_type)}}}
        self.dss_params = {}  # {dss_name: {n, striping_unit, files}}
        
    def start(self):
        """Start the disk process"""
        try:
            # Create sockets
            self.m_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.c_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            
            # Bind to ports
            self.m_sock.bind(('', self.m_port))
            self.c_sock.bind(('', self.c_port))
            
            print(f"Disk {self.name} started on m_port={self.m_port}, c_port={self.c_port}")
            
            # Start threads
            m_thread = threading.Thread(target=self.management_handler)
            c_thread = threading.Thread(target=self.command_handler)
            
            m_thread.daemon = True
            c_thread.daemon = True
            
            m_thread.start()
            c_thread.start()
            
            # Register with manager
            self.register_with_manager()
            
            print(f"Disk {self.name} is running and ready for commands...")
            
            # Keep the process alive
            try:
                while self.running:
                    time.sleep(1)
            except KeyboardInterrupt:
                print(f"\nDisk {self.name} shutting down...")
                self.running = False
            
        except KeyboardInterrupt:
            print(f"\nDisk {self.name} shutting down...")
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
            except Exception as e:
                print(f"Management handler error: {e}")
                if not self.running:
                    break
                continue
    
    def command_handler(self):
        """Handle command port communications"""
        while self.running:
            try:
                self.c_sock.settimeout(1.0)
                data, addr = self.c_sock.recvfrom(MAX_MESSAGE_SIZE)
                message = parse_message(data.decode())
                if message:
                    response = self.handle_command_message(message, addr)
                    if response:
                        self.c_sock.sendto(response.encode(), addr)
            except socket.timeout:
                continue
            except Exception as e:
                print(f"Command handler error: {e}")
                if not self.running:
                    break
                continue
    
    def register_with_manager(self):
        """Register this disk with the manager"""
        message = create_message("register-disk", {
            "disk_name": self.name,
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
    
    def deregister_with_manager(self):
        """Deregister this disk with the manager"""
        message = create_message("deregister-disk", {
            "disk_name": self.name
        }, self.name)
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.sendto(message.encode(), (self.manager_ip, self.manager_port))
            response, _ = sock.recvfrom(MAX_MESSAGE_SIZE)
            print(f"Deregistration response: {response.decode()}")
            response_data = parse_message(response.decode())
            if response_data and response_data["status"] == "SUCCESS":
                self.running = False
        finally:
            sock.close()
    
    def handle_command_message(self, message, addr):
        """Handle incoming command messages"""
        command = message.get("command")
        
        if command == "write-block":
            return self.handle_write_block(message.get("parameters", {}))
        elif command == "read-block":
            return self.handle_read_block(message.get("parameters", {}))
        elif command == "fail":
            return self.handle_fail(message.get("parameters", {}))
        elif command == "recovery-write":
            return self.handle_recovery_write(message.get("parameters", {}))
        else:
            return create_response("FAILURE", "Unknown command")
    
    def handle_write_block(self, params):
        """Handle write-block command"""
        file_name = params.get("file_name")
        dss_name = params.get("dss_name")
        stripe_num = params.get("stripe_num")
        block_type = params.get("block_type")
        block_data_encoded = params.get("block_data")
        
        if not all([file_name, dss_name, stripe_num is not None, block_type, block_data_encoded]):
            return create_response("FAILURE", "Missing required parameters")
        
        try:
            # Decode block data
            from utils.message import decode_block
            block_data = decode_block(block_data_encoded)
            
            # Initialize storage structure if needed
            if dss_name not in self.storage:
                self.storage[dss_name] = {}
            if file_name not in self.storage[dss_name]:
                self.storage[dss_name][file_name] = {}
            
            # Store block
            self.storage[dss_name][file_name][stripe_num] = (block_data, block_type)
            
            print(f"Disk {self.name}: Stored {block_type} block for {file_name} stripe {stripe_num}")
            return create_response("SUCCESS")
            
        except Exception as e:
            print(f"Error storing block: {e}")
            return create_response("FAILURE", str(e))
    
    def handle_read_block(self, params):
        """Handle read-block command"""
        file_name = params.get("file_name")
        dss_name = params.get("dss_name")
        stripe_num = params.get("stripe_num")
        
        if not all([file_name, dss_name, stripe_num is not None]):
            return create_response("FAILURE", "Missing required parameters")
        
        try:
            # Check if block exists
            if (dss_name not in self.storage or 
                file_name not in self.storage[dss_name] or 
                stripe_num not in self.storage[dss_name][file_name]):
                return create_response("FAILURE", "Block not found")
            
            # Get block data and type
            block_data, block_type = self.storage[dss_name][file_name][stripe_num]
            
            # Encode block data
            from utils.message import encode_block
            block_data_encoded = encode_block(block_data)
            
            print(f"Disk {self.name}: Read {block_type} block for {file_name} stripe {stripe_num}")
            return create_response("SUCCESS", data={
                "block_data": block_data_encoded,
                "block_type": block_type
            })
            
        except Exception as e:
            print(f"Error reading block: {e}")
            return create_response("FAILURE", str(e))
    
    def handle_fail(self, params):
        """Handle fail command - simulate disk failure"""
        dss_name = params.get("dss_name")
        
        if not dss_name:
            return create_response("FAILURE", "Missing DSS name")
        
        try:
            # Delete all contents for this DSS
            if dss_name in self.storage:
                del self.storage[dss_name]
            
            if dss_name in self.dss_params:
                del self.dss_params[dss_name]
            
            print(f"Disk {self.name}: Simulated failure for DSS {dss_name}")
            return create_response("SUCCESS")
            
        except Exception as e:
            print(f"Error during failure simulation: {e}")
            return create_response("FAILURE", str(e))
    
    def handle_recovery_write(self, params):
        """Handle recovery-write command - write recovered block"""
        file_name = params.get("file_name")
        dss_name = params.get("dss_name")
        stripe_num = params.get("stripe_num")
        block_type = params.get("block_type")
        block_data_encoded = params.get("block_data")
        
        if not all([file_name, dss_name, stripe_num is not None, block_type, block_data_encoded]):
            return create_response("FAILURE", "Missing required parameters")
        
        try:
            # Decode block data
            from utils.message import decode_block
            block_data = decode_block(block_data_encoded)
            
            # Initialize storage structure if needed
            if dss_name not in self.storage:
                self.storage[dss_name] = {}
            if file_name not in self.storage[dss_name]:
                self.storage[dss_name][file_name] = {}
            
            # Store recovered block
            self.storage[dss_name][file_name][stripe_num] = (block_data, block_type)
            
            print(f"Disk {self.name}: Recovered {block_type} block for {file_name} stripe {stripe_num}")
            return create_response("SUCCESS")
            
        except Exception as e:
            print(f"Error storing recovered block: {e}")
            return create_response("FAILURE", str(e))

def main():
    if len(sys.argv) != 6:
        print("Usage: python disk.py <name> <manager_ip> <manager_port> <m_port> <c_port>")
        sys.exit(1)
    
    try:
        name = sys.argv[1]
        manager_ip = sys.argv[2]
        manager_port = int(sys.argv[3])
        m_port = int(sys.argv[4])
        c_port = int(sys.argv[5])
        
        disk = Disk(name, manager_ip, manager_port, m_port, c_port)
        disk.start()
    except ValueError:
        print("Error: Ports must be integers")
        sys.exit(1)

if __name__ == "__main__":
    main()