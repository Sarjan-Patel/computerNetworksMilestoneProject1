#!/usr/bin/env python3
"""
File operations for striping and parity
"""

import math

def calculate_parity(data_blocks):
    """
    Calculate parity by XORing all data blocks
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
    Calculate how many stripes we need for a file
    """
    data_blocks_per_stripe = n - 1
    data_bytes_per_stripe = data_blocks_per_stripe * block_size
    return math.ceil(file_size / data_bytes_per_stripe)

def get_parity_disk_index(stripe_num, n):
    """
    Figure out which disk gets the parity block
    """
    return n - ((stripe_num % n) + 1)

def pad_block(data, block_size):
    """
    Pad data to block size with zeros
    """
    if len(data) >= block_size:
        return bytearray(data[:block_size])
    
    # Pad with NULL bytes
    padded = bytearray(data)
    padded.extend(b'\x00' * (block_size - len(data)))
    return padded

def inject_single_bit_error(block_data):
    """
    Flip a random bit in the block
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
    Check if parity is correct
    """
    computed_parity = calculate_parity(data_blocks)
    return computed_parity == parity_block
