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
        self.copy_in_progress = False
        self.reads_in_progress = []
        self.failure_in_progress = False
        self.decommission_in_progress = False
        
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
        elif command == "ls":
            return self.list_files(message.get("parameters", {}))
        elif command == "copy":
            return self.copy_file(message.get("parameters", {}))
        elif command == "copy-complete":
            return self.copy_complete(message.get("parameters", {}))
        elif command == "read":
            return self.read_file(message.get("parameters", {}))
        elif command == "read-complete":
            return self.read_complete(message.get("parameters", {}))
        elif command == "disk-failure":
            return self.disk_failure(message.get("parameters", {}))
        elif command == "recovery-complete":
            return self.recovery_complete(message.get("parameters", {}))
        elif command == "decommission-dss":
            return self.decommission_dss(message.get("parameters", {}))
        elif command == "decommission-complete":
            return self.decommission_complete(message.get("parameters", {}))
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
        user_name = params.get("user_name")
        
        # Validate parameters
        if not all([dss_name, n, striping_unit, user_name]):
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
            "files": {},
            "owner": user_name  # Track which user configured this DSS
        }
        
        print(f"DSS {dss_name} configured with {n} disks: {selected_disks}")
        return create_response("SUCCESS")
    
    def list_files(self, params):
        """Handle ls command - list files on DSSs accessible to the user"""
        user_name = params.get("user_name")
        
        if not user_name:
            return create_response("FAILURE", "Missing user name")
        
        if user_name not in self.registered_users:
            return create_response("FAILURE", "User not registered")
        
        # Check if any DSSs are configured
        if not self.configured_dsses:
            return create_response("FAILURE", "No DSSs configured")
        
        # Build response data with DSS and file information
        dss_info_list = []
        
        for dss_name, dss_data in self.configured_dsses.items():
            # Get DSS parameters
            n = dss_data["n"]
            striping_unit = dss_data["striping_unit"]
            disk_names = dss_data["disks"]
            
            # Get disk details in order
            disk_details = []
            for disk_name in disk_names:
                if disk_name in self.registered_disks:
                    disk_info = self.registered_disks[disk_name]
                    disk_details.append({
                        "disk_name": disk_name,
                        "ipv4_addr": disk_info["ipv4_addr"],
                        "c_port": disk_info["c_port"]
                    })
            
            # Get file information - show all files with their owners
            files_info = []
            for file_name, file_data in dss_data.get("files", {}).items():
                files_info.append({
                    "file_name": file_name,
                    "file_size": file_data["file_size"],
                    "owner": file_data["owner"]
                })
            
            # Add DSS information to list
            dss_info_list.append({
                "dss_name": dss_name,
                "n": n,
                "striping_unit": striping_unit,
                "disks": disk_details,
                "files": files_info
            })
        
        print(f"Listing files: {len(dss_info_list)} DSS(s) found")
        return create_response("SUCCESS", data={"dsses": dss_info_list})
    
    def copy_file(self, params):
        """Handle copy command - two-phase file copy with critical section"""
        # Check if any DSSs are configured
        if not self.configured_dsses:
            return create_response("FAILURE", "No DSSs configured")
        
        # Check if copy is already in progress (critical section)
        if self.copy_in_progress:
            # Check if copy has been stuck for too long (reset after 60 seconds)
            import time
            current_time = time.time()
            if not hasattr(self, 'copy_start_time'):
                self.copy_start_time = current_time
            elif current_time - self.copy_start_time > 60:
                print("Copy operation timed out, resetting copy_in_progress flag")
                self.copy_in_progress = False
                delattr(self, 'copy_start_time')
            else:
                return create_response("FAILURE", "Copy operation already in progress")
        
        # Get file parameters
        file_name = params.get("file_name")
        file_size = params.get("file_size")
        owner = params.get("owner")
        
        if not all([file_name, file_size, owner]):
            return create_response("FAILURE", "Missing required parameters: file_name, file_size, owner")
        
        # Select a DSS using round-robin selection across all configured DSSs
        dss_names = sorted(list(self.configured_dsses.keys()))  # Sort for consistent ordering
        
        # Initialize round-robin counter if not exists
        if not hasattr(self, 'dss_selection_index'):
            self.dss_selection_index = 0
        
        # Select DSS using round-robin
        selected_dss_name = dss_names[self.dss_selection_index % len(dss_names)]
        selected_dss = self.configured_dsses[selected_dss_name]
        
        # Update index for next selection (always increment, regardless of modulo)
        self.dss_selection_index = self.dss_selection_index + 1
        
        # Get DSS parameters
        n = selected_dss["n"]
        striping_unit = selected_dss["striping_unit"]
        disk_names = selected_dss["disks"]
        
        # Get disk details in order
        disk_details = []
        for disk_name in disk_names:
            if disk_name in self.registered_disks:
                disk_info = self.registered_disks[disk_name]
                disk_details.append({
                    "disk_name": disk_name,
                    "ipv4_addr": disk_info["ipv4_addr"],
                    "c_port": disk_info["c_port"]
                })
        
        # Set critical section flag
        self.copy_in_progress = True
        import time
        self.copy_start_time = time.time()
        
        # Return DSS parameters for Phase 1
        dss_params = {
            "dss_name": selected_dss_name,
            "n": n,
            "striping_unit": striping_unit,
            "disks": disk_details
        }
        
        print(f"Copy Phase 1: Selected DSS {selected_dss_name} for file {file_name}")
        return create_response("SUCCESS", data=dss_params)
    
    def copy_complete(self, params):
        """Handle copy-complete message - Phase 2 of copy operation"""
        # Check if copy is in progress
        if not self.copy_in_progress:
            return create_response("FAILURE", "No copy operation in progress")
        
        # Get file parameters
        file_name = params.get("file_name")
        file_size = params.get("file_size")
        owner = params.get("owner")
        dss_name = params.get("dss_name")
        
        if not all([file_name, file_size, owner, dss_name]):
            return create_response("FAILURE", "Missing required parameters")
        
        # Validate DSS exists
        if dss_name not in self.configured_dsses:
            self.copy_in_progress = False
            return create_response("FAILURE", "DSS not found")
        
        # Add file to DSS directory
        self.configured_dsses[dss_name]["files"][file_name] = {
            "file_name": file_name,
            "file_size": file_size,
            "owner": owner
        }
        
        # Clear critical section flag
        self.copy_in_progress = False
        
        print(f"Copy Phase 2: File {file_name} added to DSS {dss_name}")
        return create_response("SUCCESS")
    
    def read_file(self, params):
        """Handle read command - file reading with DSS parameters"""
        # Get parameters
        dss_name = params.get("dss_name")
        file_name = params.get("file_name")
        user_name = params.get("user_name")
        
        if not all([dss_name, file_name, user_name]):
            return create_response("FAILURE", "Missing required parameters: dss_name, file_name, user_name")
        
        # Check if DSS exists
        if dss_name not in self.configured_dsses:
            return create_response("FAILURE", "DSS not found")
        
        dss_data = self.configured_dsses[dss_name]
        
        # Check if file exists on DSS
        if file_name not in dss_data.get("files", {}):
            return create_response("FAILURE", "File not found on DSS")
        
        # Check if user is the owner
        file_info = dss_data["files"][file_name]
        if file_info["owner"] != user_name:
            return create_response("FAILURE", "User is not the owner of this file")
        
        # Get file size and DSS parameters
        file_size = file_info["file_size"]
        n = dss_data["n"]
        striping_unit = dss_data["striping_unit"]
        disk_names = dss_data["disks"]
        
        # Get disk details in order
        disk_details = []
        for disk_name in disk_names:
            if disk_name in self.registered_disks:
                disk_info = self.registered_disks[disk_name]
                disk_details.append({
                    "disk_name": disk_name,
                    "ipv4_addr": disk_info["ipv4_addr"],
                    "c_port": disk_info["c_port"]
                })
        
        # Add to reads in progress
        read_operation = {
            "dss_name": dss_name,
            "file_name": file_name,
            "user_name": user_name,
            "timestamp": __import__("time").time()
        }
        self.reads_in_progress.append(read_operation)
        
        # Return file size and DSS parameters
        dss_params = {
            "dss_name": dss_name,
            "file_size": file_size,
            "n": n,
            "striping_unit": striping_unit,
            "disks": disk_details
        }
        
        print(f"Read operation started: {file_name} from DSS {dss_name} by {user_name}")
        return create_response("SUCCESS", data=dss_params)
    
    def read_complete(self, params):
        """Handle read-complete message - cleanup read operation"""
        # Get parameters
        dss_name = params.get("dss_name")
        file_name = params.get("file_name")
        user_name = params.get("user_name")
        
        if not all([dss_name, file_name, user_name]):
            return create_response("FAILURE", "Missing required parameters")
        
        # Find and remove the read operation from in-progress list
        read_operation = None
        for op in self.reads_in_progress:
            if (op["dss_name"] == dss_name and 
                op["file_name"] == file_name and 
                op["user_name"] == user_name):
                read_operation = op
                break
        
        if not read_operation:
            return create_response("FAILURE", "Read operation not found in progress")
        
        # Remove from in-progress list
        self.reads_in_progress.remove(read_operation)
        
        print(f"Read operation completed: {file_name} from DSS {dss_name} by {user_name}")
        return create_response("SUCCESS")
    
    def disk_failure(self, params):
        """Handle disk-failure command - two-phase disk failure simulation"""
        # Get parameters
        dss_name = params.get("dss_name")
        
        if not dss_name:
            return create_response("FAILURE", "Missing required parameter: dss_name")
        
        # Check if DSS exists
        if dss_name not in self.configured_dsses:
            return create_response("FAILURE", "DSS not found")
        
        # Check if reads are in progress
        if self.reads_in_progress:
            return create_response("FAILURE", "Read operations in progress - cannot perform disk failure")
        
        # Check if failure is already in progress (critical section)
        if self.failure_in_progress:
            return create_response("FAILURE", "Disk failure operation already in progress")
        
        dss_data = self.configured_dsses[dss_name]
        
        # Get DSS parameters
        n = dss_data["n"]
        striping_unit = dss_data["striping_unit"]
        disk_names = dss_data["disks"]
        
        # Get disk details in order
        disk_details = []
        for disk_name in disk_names:
            if disk_name in self.registered_disks:
                disk_info = self.registered_disks[disk_name]
                disk_details.append({
                    "disk_name": disk_name,
                    "ipv4_addr": disk_info["ipv4_addr"],
                    "c_port": disk_info["c_port"]
                })
        
        # Set critical section flag
        self.failure_in_progress = True
        
        # Return DSS parameters for Phase 1
        dss_params = {
            "dss_name": dss_name,
            "n": n,
            "striping_unit": striping_unit,
            "disks": disk_details
        }
        
        print(f"Disk failure Phase 1: DSS {dss_name} parameters provided for failure simulation")
        return create_response("SUCCESS", data=dss_params)
    
    def recovery_complete(self, params):
        """Handle recovery-complete message - Phase 2 of disk failure operation"""
        # Get parameters
        dss_name = params.get("dss_name")
        
        if not dss_name:
            return create_response("FAILURE", "Missing required parameter: dss_name")
        
        # Check if failure is in progress
        if not self.failure_in_progress:
            return create_response("FAILURE", "No disk failure operation in progress")
        
        # Validate DSS exists
        if dss_name not in self.configured_dsses:
            self.failure_in_progress = False
            return create_response("FAILURE", "DSS not found")
        
        # Clear critical section flag
        self.failure_in_progress = False
        
        print(f"Disk failure Phase 2: DSS {dss_name} recovery completed")
        return create_response("SUCCESS")
    
    def decommission_dss(self, params):
        """Handle decommission-dss command - DSS decommissioning with critical section"""
        # Get parameters
        dss_name = params.get("dss_name")
        
        if not dss_name:
            return create_response("FAILURE", "Missing required parameter: dss_name")
        
        # Check if DSS exists
        if dss_name not in self.configured_dsses:
            return create_response("FAILURE", "DSS not found")
        
        # Check if decommission is already in progress (critical section)
        if self.decommission_in_progress:
            return create_response("FAILURE", "Decommission operation already in progress")
        
        dss_data = self.configured_dsses[dss_name]
        
        # Get DSS parameters
        n = dss_data["n"]
        striping_unit = dss_data["striping_unit"]
        disk_names = dss_data["disks"]
        
        # Get disk details in order
        disk_details = []
        for disk_name in disk_names:
            if disk_name in self.registered_disks:
                disk_info = self.registered_disks[disk_name]
                disk_details.append({
                    "disk_name": disk_name,
                    "ipv4_addr": disk_info["ipv4_addr"],
                    "c_port": disk_info["c_port"]
                })
        
        # Set critical section flag
        self.decommission_in_progress = True
        
        # Return DSS parameters for decommissioning
        dss_params = {
            "dss_name": dss_name,
            "n": n,
            "striping_unit": striping_unit,
            "disks": disk_details
        }
        
        print(f"Decommission Phase 1: DSS {dss_name} parameters provided for decommissioning")
        return create_response("SUCCESS", data=dss_params)
    
    def decommission_complete(self, params):
        """Handle decommission-complete message - Phase 2 of decommission operation"""
        # Get parameters
        dss_name = params.get("dss_name")
        
        if not dss_name:
            return create_response("FAILURE", "Missing required parameter: dss_name")
        
        # Check if decommission is in progress
        if not self.decommission_in_progress:
            return create_response("FAILURE", "No decommission operation in progress")
        
        # Validate DSS exists
        if dss_name not in self.configured_dsses:
            self.decommission_in_progress = False
            return create_response("FAILURE", "DSS not found")
        
        dss_data = self.configured_dsses[dss_name]
        disk_names = dss_data["disks"]
        
        # Update disk states from InDSS to Free
        for disk_name in disk_names:
            if disk_name in self.registered_disks:
                self.registered_disks[disk_name]["state"] = "Free"
                # Remove DSS-specific information
                if "dss_name" in self.registered_disks[disk_name]:
                    del self.registered_disks[disk_name]["dss_name"]
        
        # Remove DSS from configured DSSes
        del self.configured_dsses[dss_name]
        
        # Clear critical section flag
        self.decommission_in_progress = False
        
        print(f"Decommission Phase 2: DSS {dss_name} decommissioned, {len(disk_names)} disks returned to Free state")
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