#!/usr/bin/env python3
"""
File operations utilities for DSS file striping and parity calculation
"""

import math

def calculate_parity(data_blocks):
    """
    Calculate parity block by XORing all data blocks byte by byte
    
    Args:
        data_blocks: List of byte arrays (data blocks)
    
    Returns:
        bytearray: Parity block
    """
    if not data_blocks:
        return bytearray()
    
    # Start with first block
    parity = bytearray(data_blocks[0])
    
    # XOR with remaining blocks
    for block in data_blocks[1:]:
        for i in range(len(parity)):
            if i < len(block):
                parity[i] ^= block[i]
    
    return parity

def calculate_stripe_count(file_size, n, block_size):
    """
    Calculate number of stripes needed to store a file
    
    Args:
        file_size: Size of file in bytes
        n: Number of disks in DSS
        block_size: Size of each block in bytes
    
    Returns:
        int: Number of stripes needed
    """
    data_blocks_per_stripe = n - 1
    data_bytes_per_stripe = data_blocks_per_stripe * block_size
    return math.ceil(file_size / data_bytes_per_stripe)

def get_parity_disk_index(stripe_num, n):
    """
    Calculate which disk should store the parity block for a given stripe
    
    Args:
        stripe_num: Stripe number (0-based)
        n: Number of disks in DSS
    
    Returns:
        int: Disk index for parity block
    """
    return n - ((stripe_num % n) + 1)

def pad_block(data, block_size):
    """
    Pad block with NULL bytes to specified size
    
    Args:
        data: Data to pad
        block_size: Target block size
    
    Returns:
        bytearray: Padded block
    """
    if len(data) >= block_size:
        return bytearray(data[:block_size])
    
    # Pad with NULL bytes
    padded = bytearray(data)
    padded.extend(b'\x00' * (block_size - len(data)))
    return padded

def inject_single_bit_error(block_data):
    """
    Inject a single bit error into a block at random position
    
    Args:
        block_data: Block data as bytearray
    
    Returns:
        bytearray: Block with single bit error
    """
    import random
    
    if not block_data:
        return block_data
    
    # Choose random byte and bit position
    byte_index = random.randint(0, len(block_data) - 1)
    bit_position = random.randint(0, 7)
    
    # Create copy and flip the bit
    error_block = bytearray(block_data)
    error_block[byte_index] ^= (1 << bit_position)
    
    return error_block

def verify_parity(data_blocks, parity_block):
    """
    Verify parity by recomputing and comparing
    
    Args:
        data_blocks: List of data blocks
        parity_block: Stored parity block
    
    Returns:
        bool: True if parity matches, False otherwise
    """
    computed_parity = calculate_parity(data_blocks)
    return computed_parity == parity_block
