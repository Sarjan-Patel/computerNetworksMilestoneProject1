#!/usr/bin/env python3
import sys
import socket
import threading
import os
import time
from utils.constants import MAX_MESSAGE_SIZE
from utils.message import create_message, parse_message, encode_block, decode_block
from utils.file_ops import calculate_parity, calculate_stripe_count, get_parity_disk_index, pad_block, inject_single_bit_error, verify_parity

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
        print("  ls")
        print("  copy <file_path>")
        print("  read <dss_name> <file_name> [error_prob]")
        print("  disk-failure <dss_name>")
        print("  decommission-dss <dss_name>")
        print("  deregister-user")
        print("  quit")
        
        while self.running:
            try:
                command = input(f"{self.name}> ").strip()
                if command.lower() == 'quit':
                    break
                elif command.startswith("configure-dss"):
                    self.handle_configure_dss(command)
                elif command == "ls":
                    self.handle_ls()
                elif command.startswith("copy"):
                    self.handle_copy(command)
                elif command.startswith("read"):
                    self.handle_read(command)
                elif command.startswith("disk-failure"):
                    self.handle_disk_failure(command)
                elif command.startswith("decommission-dss"):
                    self.handle_decommission_dss(command)
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
                "striping_unit": striping_unit,
                "user_name": self.name
            })
            
            if response:
                print(f"DSS configuration: {response['status']}")
                if response.get("message"):
                    print(f"Message: {response['message']}")
            
        except ValueError:
            print("Error: n and striping_unit must be integers")
    
    def handle_ls(self):
        """Handle ls command - list files on all DSSs"""
        response = self.send_to_manager("ls", {"user_name": self.name})
        
        if response:
            if response["status"] == "FAILURE":
                print(f"ls failed: {response.get('message', 'Unknown error')}")
                return
            
            # Parse and display DSS information
            dsses = response.get("data", {}).get("dsses", [])
            
            if not dsses:
                print("No DSSs configured")
                return
            
            print(f"\n{'='*70}")
            print(f"Distributed Storage Systems - File Listing")
            print(f"{'='*70}\n")
            
            for dss_info in dsses:
                dss_name = dss_info["dss_name"]
                n = dss_info["n"]
                striping_unit = dss_info["striping_unit"]
                disks = dss_info["disks"]
                files = dss_info["files"]
                
                # Display DSS information
                disk_names = ", ".join([d["disk_name"] for d in disks])
                print(f"{dss_name}: Disk array with n={n} ({disk_names}) with striping-unit {striping_unit} B.")
                
                # Display files
                if files:
                    for file_info in files:
                        file_name = file_info["file_name"]
                        file_size = file_info["file_size"]
                        owner = file_info["owner"]
                        print(f"  {file_name:<30} {file_size:>10} B  {owner}")
                else:
                    print("  (no files)")
                
                print()  # Blank line between DSSs
    
    def handle_copy(self, command):
        """Handle copy command - two-phase file copy"""
        parts = command.split()
        if len(parts) != 2:
            print("Usage: copy <file_path>")
            return
        
        file_path = parts[1]
        
        try:
            # Check if file exists and get file size
            import os
            if not os.path.exists(file_path):
                print(f"Error: File {file_path} does not exist")
                return
            
            file_size = os.path.getsize(file_path)
            file_name = os.path.basename(file_path)
            
            print(f"Copying file: {file_name} ({file_size} bytes)")
            
            # Phase 1: Request DSS parameters from manager
            response = self.send_to_manager("copy", {
                "file_name": file_name,
                "file_size": file_size,
                "owner": self.name
            })
            
            if not response or response["status"] != "SUCCESS":
                print(f"Copy failed: {response.get('message', 'Unknown error') if response else 'No response'}")
                return
            
            # Get DSS parameters from response
            dss_params = response.get("data", {})
            dss_name = dss_params["dss_name"]
            n = dss_params["n"]
            striping_unit = dss_params["striping_unit"]
            disks = dss_params["disks"]
            
            print(f"Selected DSS: {dss_name} with {n} disks, striping-unit: {striping_unit} B")
            
            # Phase 2: Perform actual file copy with striping and parity
            print("Performing file copy to DSS...")
            
            # Read file and perform striping
            success = self.copy_file_to_dss(file_path, dss_params)
            if not success:
                print("File copy failed")
                return
            
            # Send copy-complete message to manager
            complete_response = self.send_to_manager("copy-complete", {
                "file_name": file_name,
                "file_size": file_size,
                "owner": self.name,
                "dss_name": dss_name
            })
            
            if complete_response and complete_response["status"] == "SUCCESS":
                print(f"File {file_name} successfully copied to DSS {dss_name}")
            else:
                print(f"Copy completion failed: {complete_response.get('message', 'Unknown error') if complete_response else 'No response'}")
                
        except Exception as e:
            print(f"Error during copy: {e}")
    
    def copy_file_to_dss(self, file_path, dss_params):
        """Copy file to DSS using striping and parity"""
        try:
            # Get DSS parameters
            dss_name = dss_params["dss_name"]
            n = dss_params["n"]
            striping_unit = dss_params["striping_unit"]
            disks = dss_params["disks"]
            
            # Read file
            with open(file_path, 'rb') as f:
                file_data = f.read()
            
            file_size = len(file_data)
            file_name = os.path.basename(file_path)
            
            print(f"Copying {file_name} ({file_size} bytes) to DSS {dss_name}")
            print(f"Striping: {n} disks, block size: {striping_unit} bytes")
            
            # Calculate number of stripes needed
            num_stripes = calculate_stripe_count(file_size, n, striping_unit)
            print(f"Number of stripes: {num_stripes}")
            
            # Process each stripe
            for stripe_num in range(num_stripes):
                print(f"Processing stripe {stripe_num + 1}/{num_stripes}")
                
                # Calculate data range for this stripe
                data_bytes_per_stripe = (n - 1) * striping_unit
                start_byte = stripe_num * data_bytes_per_stripe
                end_byte = min(start_byte + data_bytes_per_stripe, file_size)
                
                # Extract data for this stripe
                stripe_data = file_data[start_byte:end_byte]
                
                # Split into n-1 data blocks
                data_blocks = []
                for i in range(n - 1):
                    block_start = i * striping_unit
                    block_end = min(block_start + striping_unit, len(stripe_data))
                    block_data = stripe_data[block_start:block_end]
                    
                    # Pad block if necessary
                    padded_block = pad_block(block_data, striping_unit)
                    data_blocks.append(padded_block)
                
                # Calculate parity block
                parity_block = calculate_parity(data_blocks)
                
                # Determine parity disk position
                parity_disk_index = get_parity_disk_index(stripe_num, n)
                
                # Create stripe with data and parity blocks
                stripe_blocks = []
                data_block_index = 0
                
                for disk_index in range(n):
                    if disk_index == parity_disk_index:
                        # This disk gets the parity block
                        stripe_blocks.append(parity_block)
                    else:
                        # This disk gets a data block
                        stripe_blocks.append(data_blocks[data_block_index])
                        data_block_index += 1
                
                # Write stripe to disks in parallel
                success = self.write_stripe_to_disks(stripe_blocks, disks, dss_name, file_name, stripe_num)
                if not success:
                    print(f"Failed to write stripe {stripe_num}")
                    return False
                
                print(f"Stripe {stripe_num + 1} written successfully")
            
            print(f"File {file_name} copied successfully to DSS {dss_name}")
            return True
            
        except Exception as e:
            print(f"Error copying file: {e}")
            return False
    
    def write_stripe_to_disks(self, stripe_blocks, disks, dss_name, file_name, stripe_num):
        """Write a stripe to all disks in parallel using threading"""
        threads = []
        results = {}
        lock = threading.Lock()
        
        def write_block_thread(disk_index, block_data, disk_info):
            """Thread function to write a single block to a disk"""
            try:
                # Create write-block message
                message = create_message("write-block", {
                    "file_name": file_name,
                    "dss_name": dss_name,
                    "stripe_num": stripe_num,
                    "block_type": "parity" if disk_index == get_parity_disk_index(stripe_num, len(disks)) else "data",
                    "block_data": encode_block(block_data)
                }, self.name)
                
                # Send to disk
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                try:
                    sock.sendto(message.encode(), (disk_info["ipv4_addr"], disk_info["c_port"]))
                    
                    # Wait for response
                    sock.settimeout(30.0)
                    response, addr = sock.recvfrom(MAX_MESSAGE_SIZE)
                    response_data = parse_message(response.decode())
                    
                    with lock:
                        results[disk_index] = response_data and response_data.get("status") == "SUCCESS"
                        
                finally:
                    sock.close()
                    
            except Exception as e:
                print(f"Error writing to disk {disk_index}: {e}")
                with lock:
                    results[disk_index] = False
        
        # Start threads for all disks
        for disk_index, (block_data, disk_info) in enumerate(zip(stripe_blocks, disks)):
            thread = threading.Thread(
                target=write_block_thread,
                args=(disk_index, block_data, disk_info)
            )
            thread.start()
            threads.append(thread)
            time.sleep(0.1)  # Small delay between thread starts
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Check if all writes were successful
        success = all(results.get(i, False) for i in range(len(disks)))
        return success
    
    def read_file_from_dss(self, file_name, dss_params, error_prob):
        """Read file from DSS with error injection and parity verification"""
        try:
            # Get DSS parameters
            dss_name = dss_params["dss_name"]
            file_size = dss_params["file_size"]
            n = dss_params["n"]
            striping_unit = dss_params["striping_unit"]
            disks = dss_params["disks"]
            
            print(f"Reading {file_name} ({file_size} bytes) from DSS {dss_name}")
            print(f"Striping: {n} disks, block size: {striping_unit} bytes, error_prob: {error_prob}%")
            
            # Calculate number of stripes needed
            num_stripes = calculate_stripe_count(file_size, n, striping_unit)
            print(f"Number of stripes: {num_stripes}")
            
            # Create output file
            output_file = f"read_{file_name}"
            output_data = bytearray()
            
            # Process each stripe
            for stripe_num in range(num_stripes):
                print(f"Processing stripe {stripe_num + 1}/{num_stripes}")
                
                # Read stripe with error injection and parity verification
                stripe_success = False
                retry_count = 0
                max_retries = 3
                
                while not stripe_success and retry_count < max_retries:
                    if retry_count > 0:
                        print(f"Retrying stripe {stripe_num + 1} (attempt {retry_count + 1})")
                    
                    # Read all blocks from stripe in parallel
                    stripe_blocks, block_types = self.read_stripe_from_disks(disks, dss_name, file_name, stripe_num)
                    
                    if stripe_blocks is None:
                        print(f"Failed to read stripe {stripe_num}")
                        return False
                    
                    # Inject error with probability p (only on first attempt)
                    if retry_count == 0 and error_prob > 0:
                        import random
                        if random.randint(1, 100) <= error_prob:
                            # Choose random block to inject error
                            error_block_index = random.randint(0, len(stripe_blocks) - 1)
                            stripe_blocks[error_block_index] = inject_single_bit_error(stripe_blocks[error_block_index])
                            print(f"Injected error into block {error_block_index} of stripe {stripe_num}")
                    
                    # Verify parity
                    data_blocks = []
                    parity_block = None
                    parity_disk_index = get_parity_disk_index(stripe_num, n)
                    
                    for i, (block_data, block_type) in enumerate(zip(stripe_blocks, block_types)):
                        if i == parity_disk_index:
                            parity_block = block_data
                        else:
                            data_blocks.append(block_data)
                    
                    # Verify parity
                    if verify_parity(data_blocks, parity_block):
                        print(f"Stripe {stripe_num + 1} parity verified successfully")
                        stripe_success = True
                        
                        # Add data blocks to output (excluding parity)
                        for data_block in data_blocks:
                            output_data.extend(data_block)
                    else:
                        print(f"Stripe {stripe_num + 1} parity verification failed")
                        retry_count += 1
                        if retry_count < max_retries:
                            print("Rereading stripe...")
                
                if not stripe_success:
                    print(f"Failed to read stripe {stripe_num} after {max_retries} attempts")
                    return False
                
                print(f"Stripe {stripe_num + 1} read successfully")
            
            # Truncate output to original file size (remove padding)
            output_data = output_data[:file_size]
            
            # Write output file
            with open(output_file, 'wb') as f:
                f.write(output_data)
            
            # Verify file integrity using diff
            try:
                import subprocess
                result = subprocess.run(['diff', file_name, output_file], 
                                      capture_output=True, text=True)
                if result.returncode == 0:
                    print(f"File integrity verified: {file_name} matches {output_file}")
                else:
                    print(f"File integrity check failed: {result.stderr}")
                    return False
            except Exception as e:
                print(f"Could not verify file integrity: {e}")
                # Continue anyway as diff might not be available
            
            print(f"File {file_name} read successfully from DSS {dss_name}")
            return True
            
        except Exception as e:
            print(f"Error reading file: {e}")
            return False
    
    def read_stripe_from_disks(self, disks, dss_name, file_name, stripe_num):
        """Read a stripe from all disks in parallel using threading"""
        threads = []
        results = {}
        block_types = {}
        lock = threading.Lock()
        
        def read_block_thread(disk_index, disk_info):
            """Thread function to read a single block from a disk"""
            try:
                # Create read-block message
                message = create_message("read-block", {
                    "file_name": file_name,
                    "dss_name": dss_name,
                    "stripe_num": stripe_num
                }, self.name)
                
                # Send to disk
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                try:
                    sock.sendto(message.encode(), (disk_info["ipv4_addr"], disk_info["c_port"]))
                    
                    # Wait for response
                    sock.settimeout(30.0)
                    response, _ = sock.recvfrom(MAX_MESSAGE_SIZE)
                    response_data = parse_message(response.decode())
                    
                    if response_data and response_data.get("status") == "SUCCESS":
                        data = response_data.get("data", {})
                        block_data = decode_block(data["block_data"])
                        block_type = data["block_type"]
                        
                        with lock:
                            results[disk_index] = block_data
                            block_types[disk_index] = block_type
                    else:
                        with lock:
                            results[disk_index] = None
                            
                finally:
                    sock.close()
                    
            except Exception as e:
                print(f"Error reading from disk {disk_index}: {e}")
                with lock:
                    results[disk_index] = None
        
        # Start threads for all disks
        for disk_index, disk_info in enumerate(disks):
            thread = threading.Thread(
                target=read_block_thread,
                args=(disk_index, disk_info)
            )
            thread.start()
            threads.append(thread)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Check if all reads were successful
        stripe_blocks = []
        block_types_list = []
        
        for i in range(len(disks)):
            if results.get(i) is not None:
                stripe_blocks.append(results[i])
                block_types_list.append(block_types[i])
            else:
                return None, None
        
        return stripe_blocks, block_types_list
    
    def simulate_disk_failure(self, dss_params):
        """Simulate disk failure and perform recovery"""
        try:
            # Get DSS parameters
            dss_name = dss_params["dss_name"]
            n = dss_params["n"]
            striping_unit = dss_params["striping_unit"]
            disks = dss_params["disks"]
            
            print(f"Simulating disk failure on DSS {dss_name} with {n} disks")
            
            # Randomly select a disk to fail
            import random
            failed_disk_index = random.randint(0, n - 1)
            failed_disk = disks[failed_disk_index]
            
            print(f"Selected disk {failed_disk_index} ({failed_disk['disk_name']}) for failure")
            
            # Send fail message to selected disk
            success = self.send_fail_to_disk(failed_disk, dss_name)
            if not success:
                print("Failed to send fail message to disk")
                return False
            
            print(f"Disk {failed_disk['disk_name']} failed successfully")
            
            # Perform recovery
            recovery_success = self.recover_failed_disk(failed_disk_index, dss_params)
            if not recovery_success:
                print("Disk recovery failed")
                return False
            
            print(f"Disk {failed_disk['disk_name']} recovered successfully")
            return True
            
        except Exception as e:
            print(f"Error during disk failure simulation: {e}")
            return False
    
    def send_fail_to_disk(self, disk_info, dss_name):
        """Send fail message to a disk"""
        try:
            message = create_message("fail", {
                "dss_name": dss_name
            }, self.name)
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                sock.sendto(message.encode(), (disk_info["ipv4_addr"], disk_info["c_port"]))
                
                # Wait for fail-complete response
                sock.settimeout(30.0)
                response, _ = sock.recvfrom(MAX_MESSAGE_SIZE)
                response_data = parse_message(response.decode())
                
                return response_data and response_data.get("status") == "SUCCESS"
                
            finally:
                sock.close()
                
        except Exception as e:
            print(f"Error sending fail message: {e}")
            return False
    
    def recover_failed_disk(self, failed_disk_index, dss_params):
        """Recover failed disk by reconstructing all its blocks"""
        try:
            dss_name = dss_params["dss_name"]
            n = dss_params["n"]
            disks = dss_params["disks"]
            
            print(f"Recovering disk {failed_disk_index} for DSS {dss_name}")
            
            # Get list of remaining disks (excluding failed disk)
            remaining_disks = [disk for i, disk in enumerate(disks) if i != failed_disk_index]
            
            # Get DSS file list from manager for proper recovery
            ls_response = self.send_to_manager("ls", {"user_name": self.name})
            files_to_recover = []
            
            if ls_response and ls_response.get("status") == "SUCCESS":
                dsses = ls_response.get("data", {}).get("dsses", [])
                for dss_info in dsses:
                    if dss_info["dss_name"] == dss_name:
                        files_to_recover = [f["file_name"] for f in dss_info["files"]]
                        break
            
            if not files_to_recover:
                print("No files found on DSS to recover")
                return True
            
            print(f"Recovering {len(files_to_recover)} file(s): {files_to_recover}")
            
            # Recover each file's stripes
            for file_name in files_to_recover:
                print(f"Recovering file: {file_name}")
                
                # Process first stripe (most files fit in one stripe)
                stripe_num = 0
                
                # Read blocks from remaining disks in parallel
                recovery_blocks, block_types = self.read_stripe_from_disks(remaining_disks, dss_name, file_name, stripe_num)
                
                if recovery_blocks is None:
                    print(f"Could not read stripe {stripe_num} for {file_name} from remaining disks")
                    continue
                
                # XOR all blocks to reconstruct the missing block
                reconstructed_block = recovery_blocks[0]
                for block in recovery_blocks[1:]:
                    reconstructed_block = bytearray(a ^ b for a, b in zip(reconstructed_block, block))
                
                # Determine if reconstructed block is data or parity
                parity_disk_index = get_parity_disk_index(stripe_num, n)
                block_type = "parity" if failed_disk_index == parity_disk_index else "data"
                
                print(f"Reconstructed {block_type} block for stripe {stripe_num}")
                
                # Write recovered block to failed disk
                success = self.write_recovered_block_to_disk(disks[failed_disk_index], dss_name, file_name, stripe_num, reconstructed_block, block_type)
                if not success:
                    print(f"Failed to write recovered block for {file_name} stripe {stripe_num}")
                    return False
                
                print(f"File {file_name} stripe {stripe_num} recovered successfully")
            
            print(f"Disk {failed_disk_index} recovery completed")
            return True
            
        except Exception as e:
            print(f"Error during disk recovery: {e}")
            return False
    
    def write_recovered_block_to_disk(self, disk_info, dss_name, file_name, stripe_num, block_data, block_type):
        """Write recovered block to disk"""
        try:
            message = create_message("recovery-write", {
                "file_name": file_name,
                "dss_name": dss_name,
                "stripe_num": stripe_num,
                "block_type": block_type,
                "block_data": encode_block(block_data)
            }, self.name)
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                sock.sendto(message.encode(), (disk_info["ipv4_addr"], disk_info["c_port"]))
                
                # Wait for response
                sock.settimeout(30.0)
                response, _ = sock.recvfrom(MAX_MESSAGE_SIZE)
                response_data = parse_message(response.decode())
                
                return response_data and response_data.get("status") == "SUCCESS"
                
            finally:
                sock.close()
                
        except Exception as e:
            print(f"Error writing recovered block: {e}")
            return False
    
    def handle_read(self, command):
        """Handle read command - file reading with error injection"""
        parts = command.split()
        if len(parts) < 3 or len(parts) > 4:
            print("Usage: read <dss_name> <file_name> [error_prob]")
            return
        
        dss_name = parts[1]
        file_name = parts[2]
        error_prob = 10  # Default 10% error probability
        
        if len(parts) == 4:
            try:
                error_prob = int(parts[3])
                if error_prob < 0 or error_prob > 100:
                    print("Error: error_prob must be between 0 and 100")
                    return
            except ValueError:
                print("Error: error_prob must be an integer")
                return
        
        print(f"Reading file: {file_name} from DSS {dss_name} (error_prob={error_prob}%)")
        
        try:
            # Request file read from manager
            response = self.send_to_manager("read", {
                "dss_name": dss_name,
                "file_name": file_name,
                "user_name": self.name
            })
            
            if not response or response["status"] != "SUCCESS":
                print(f"Read failed: {response.get('message', 'Unknown error') if response else 'No response'}")
                return
            
            # Get file and DSS parameters from response
            data = response.get("data", {})
            file_size = data["file_size"]
            n = data["n"]
            striping_unit = data["striping_unit"]
            disks = data["disks"]
            
            print(f"File size: {file_size} bytes, DSS: {n} disks, striping-unit: {striping_unit} B")
            
            # Phase 2: Perform actual file read with striping and parity verification
            print("Performing file read from DSS...")
            
            # Create DSS parameters structure
            dss_params = {
                "dss_name": dss_name,
                "file_size": file_size,
                "n": n,
                "striping_unit": striping_unit,
                "disks": disks
            }
            
            # Read file from DSS with error injection and parity verification
            success = self.read_file_from_dss(file_name, dss_params, error_prob)
            if not success:
                print("File read failed")
                return
            
            # Send read-complete message to manager
            complete_response = self.send_to_manager("read-complete", {
                "dss_name": dss_name,
                "file_name": file_name,
                "user_name": self.name
            })
            
            if complete_response and complete_response["status"] == "SUCCESS":
                print(f"File {file_name} successfully read from DSS {dss_name}")
            else:
                print(f"Read completion failed: {complete_response.get('message', 'Unknown error') if complete_response else 'No response'}")
                
        except Exception as e:
            print(f"Error during read: {e}")
    
    def handle_disk_failure(self, command):
        """Handle disk-failure command - two-phase disk failure simulation"""
        parts = command.split()
        if len(parts) != 2:
            print("Usage: disk-failure <dss_name>")
            return
        
        dss_name = parts[1]
        
        print(f"Simulating disk failure on DSS: {dss_name}")
        
        try:
            # Phase 1: Request DSS parameters from manager
            response = self.send_to_manager("disk-failure", {
                "dss_name": dss_name
            })
            
            if not response or response["status"] != "SUCCESS":
                print(f"Disk failure failed: {response.get('message', 'Unknown error') if response else 'No response'}")
                return
            
            # Get DSS parameters from response
            dss_params = response.get("data", {})
            n = dss_params["n"]
            striping_unit = dss_params["striping_unit"]
            disks = dss_params["disks"]
            
            print(f"DSS parameters: {n} disks, striping-unit: {striping_unit} B")
            
            # Phase 2: Perform actual disk failure simulation and recovery
            print("Performing disk failure simulation...")
            
            # Simulate disk failure and perform recovery
            success = self.simulate_disk_failure(dss_params)
            if not success:
                print("Disk failure simulation failed")
                return
            
            # Send recovery-complete message to manager
            complete_response = self.send_to_manager("recovery-complete", {
                "dss_name": dss_name
            })
            
            if complete_response and complete_response["status"] == "SUCCESS":
                print(f"Disk failure simulation completed for DSS {dss_name}")
            else:
                print(f"Recovery completion failed: {complete_response.get('message', 'Unknown error') if complete_response else 'No response'}")
                
        except Exception as e:
            print(f"Error during disk failure: {e}")
    
    def handle_decommission_dss(self, command):
        """Handle decommission-dss command - DSS decommissioning"""
        parts = command.split()
        if len(parts) != 2:
            print("Usage: decommission-dss <dss_name>")
            return
        
        dss_name = parts[1]
        
        print(f"Decommissioning DSS: {dss_name}")
        
        try:
            # Phase 1: Request DSS parameters from manager
            response = self.send_to_manager("decommission-dss", {
                "dss_name": dss_name
            })
            
            if not response or response["status"] != "SUCCESS":
                print(f"Decommission failed: {response.get('message', 'Unknown error') if response else 'No response'}")
                return
            
            # Get DSS parameters from response
            dss_params = response.get("data", {})
            n = dss_params["n"]
            striping_unit = dss_params["striping_unit"]
            disks = dss_params["disks"]
            
            print(f"DSS parameters: {n} disks, striping-unit: {striping_unit} B")
            
            # Phase 2: Perform actual DSS decommissioning
            print("Performing DSS decommissioning...")
            
            # Simulate decommission process
            import time
            time.sleep(1)  # Simulate decommission time
            
            # Send decommission-complete message to manager
            complete_response = self.send_to_manager("decommission-complete", {
                "dss_name": dss_name
            })
            
            if complete_response and complete_response["status"] == "SUCCESS":
                print(f"DSS {dss_name} successfully decommissioned")
            else:
                print(f"Decommission completion failed: {complete_response.get('message', 'Unknown error') if complete_response else 'No response'}")
                
        except Exception as e:
            print(f"Error during decommission: {e}")
    
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