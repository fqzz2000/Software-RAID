#!/bin/python3
import os
import argparse
import random

def read_block(device_path, offset, size):
    fd = os.open(device_path, os.O_RDONLY)
    try:
        data = os.pread(fd, size, offset)
    finally:
        os.close(fd)
    return data

def write_block(device_path, offset, data):
    fd = os.open(device_path, os.O_WRONLY)
    try:
        os.pwrite(fd, data, offset) 
    finally:
        os.close(fd)
def test_block_readwrite(device_path, device_size, times=20, block_size=1024):
    # write data to random offset and read it back
    print ('----------testing block random read/write----------')

    for i in range(times):
        size = random.randint(1, block_size * (device_size // block_size))
        offset = random.randint(0, device_size-size-1)
        
        data = os.urandom(size)
        write_block(device_path, offset, data)
        os.sync()
        read_data = read_block(device_path, offset, size)
        if data != read_data:
            print('data mismatch at offset: {}'.format(offset))
            for byte in range(size):
                if data[byte] != read_data[byte]:
                    print('mismatch at byte: {}'.format(byte))
                    print('data: {}'.format(data[byte]))
                    print('read_data: {}'.format(read_data[byte]))
                    print("")
                    break
            print('[FAILED]test block random read/write failed')
            return False
    print('[SUCCESS]test block random read/write passed')
    return True

def test_block_first_write_to_image0 (device_path, image0_path, block_size=1024):
    print('----------testing block first write to image0----------')
   
    writeToImage0 = (b'\x00' * block_size)
    write_block(device_path, 0, writeToImage0)
    os.sync()
    if read_block(device_path, 0, block_size) != writeToImage0:
        print('test block first write to image0 failed: read write mismatch')
        return False
    readImg0 = read_block(image0_path, 0, block_size)
    if readImg0 != writeToImage0:
        print('readImg0: ', readImg0.hex())
        print('[FAILED]test block first write to image0 failed')
        return False
    print('[SUCCESS]test block first write to image0 passed')
    return True

def test_block_first_write_to_image1 (device_path, image1_path, block_size=1024):
    print('----------testing block first write to image1----------')
    writeToImage1 = (b'\xff' * block_size)
    write_block(device_path, block_size, writeToImage1)
    os.sync()
    if read_block(device_path, block_size, block_size) != writeToImage1:
        print('[FAILED]test block first write to image1 failed: read write mismatch')
        return False
    readImg1 = read_block(image1_path, 0, block_size)
    if readImg1 != writeToImage1:
        print('readImg1: ', readImg1.hex())
        print('[FAILED]test block first write to image1 failed')
        return False
    print('[SUCCESS]test block first write to image1 passed')
    return True


def test_block_write_with_image(device_path, image0_path, image1_path, block_size=1024, times=20):
    print('----------testing block write with image----------')
    image0_size = os.path.getsize(image0_path)
    image1_size = os.path.getsize(image1_path)
    device_size = image0_size + image1_size
    if times * block_size * 2 > device_size:
        print('[FAILED]test block write with image failed: not enough space')
        return False
    writeToImage0 = (b'\x00' * block_size)
    writeToImage1 = (b'\xff' * block_size)
    # binary all zero string to fill the block
    offset = 0
    for i in range(times):
        write_block(device_path, offset, writeToImage0 + writeToImage1)
        offset += block_size * 2
    os.sync()
    # test if image0 are all zero and image1 are all one
    readImg0 = read_block(image0_path, 0, block_size * times)
    readImg1 = read_block(image1_path, 0, block_size * times)
    if readImg0 != writeToImage0 * times or readImg1 != writeToImage1 * times:
        # print readImg0 in hex
        # print("readImg0: ", readImg0.hex())
        print("readImg1: ", readImg1.hex())
        print('[FAILED]test block write with image failed')
        return False
    print('[SUCCESS]test block write with image passed')
    return True
    




def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('device', help='block device path')
    parser.add_argument('image0', help='first image path')
    parser.add_argument('image1', help='second image path')
    parser.add_argument('--block-size', help='block size in bytes', default=1024, type=int)
    args = parser.parse_args()
    device = args.device
    image0 = args.image0
    image1 = args.image1
    block_size = args.block_size
    total_size = os.path.getsize(image0) + os.path.getsize(image1)
    total_size = total_size - total_size % block_size
    print('total size: {}'.format(total_size))
    test_block_readwrite(device, total_size, 100, block_size)
    # test_block_first_write_to_image0(device, image0, block_size)
    # test_block_first_write_to_image1(device, image1, block_size)
    # test_block_write_with_image(device, image0, image1, block_size, 100)


if __name__ == '__main__':
    main()




