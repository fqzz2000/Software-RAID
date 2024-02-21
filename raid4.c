/*
 * RAID1 example for BUSE
 * by Tyler Bletsch to ECE566, Duke University, Fall 2019
 *
 * Based on 'busexmp' by Adam Cozzette
 *
 * This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 */

#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <argp.h>
#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <unistd.h>

#include "buse.h"

#define UNUSED(x) (void)(x) // used to suppress "unused variable" warnings without turning off the feature entirely

int dev_fd[16];            // file descriptors for two underlying block devices that make up the RAID
int dev_fd_size;           // number of devices
int block_size;            // NOTE: other than truncating the resulting raid device, block_size is ignored in this program; it is asked for and set in order to make it easier to adapt this code to RAID0/4/5/6.
int fail_dev;              // index of the failed device
int parity_dev = -1;       // index of the parity device
uint64_t raid_device_size; // size of raid device in bytes
bool verbose = false;      // set to true by -v option for debug output
bool degraded = false;     // true if we're missing a device

int ok_dev = -1;      // index of dev_fd that has a valid drive (used in degraded mode to identify the non-missing drive (0 or 1))
int rebuild_dev = -1; // index of drive that is being added with '+' for RAID rebuilt

int last_read_dev = 0; // used to interleave reading between the two devices

static int xmp_read(void *buf, u_int32_t len, u_int64_t offset, void *userdata)
{
    UNUSED(userdata);
    if (verbose)
        fprintf(stderr, "R - %lu, %u\n", offset, len);

    long started = offset / block_size;
    long ended = (offset + len) / block_size;
    if (offset + len > raid_device_size)
    {
        fprintf(stderr, "Read request exceeds device size.\n");
        return -EIO;
    }
    long bytesRead = 0;

    for (long i = started; i < ended; i++)
    {
        int driveToRead = i % (dev_fd_size - 1);
        int blockToRead = i / (dev_fd_size - 1) * block_size;
        long rd;
        long bytesToRead = len - bytesRead > block_size ? block_size : len - bytesRead;
        if (degraded && driveToRead == fail_dev)
        {
            char readBuf[bytesToRead];
            memset(readBuf, 0, bytesToRead);
            // read from surviving drives
            memset((char *)buf + bytesRead, 0, bytesToRead);
            for (int j = 0; j < dev_fd_size; j++)
            {
                if (j != fail_dev)
                {
                    pread(dev_fd[j], readBuf, bytesToRead, blockToRead);
                    for (int k = 0; k < bytesToRead; k++)
                    {
                        ((char *)buf)[k + bytesRead] = ((char *)buf)[k + bytesRead] ^ readBuf[k];
                    }
                }
            }
            rd = bytesToRead;
        }
        else
        {
            rd = pread(dev_fd[driveToRead], (char *)buf + bytesRead, bytesToRead, blockToRead);
        }
        bytesRead += rd;
    }

    return 0;
}

static int xmp_write(const void *buf, u_int32_t len, u_int64_t offset, void *userdata)
{
    UNUSED(userdata);
    long started = offset / block_size;
    long ended = (offset + len) / block_size;
    if (offset + len > raid_device_size)
    {
        fprintf(stderr, "Write request exceeds device size.\n");
        return -EIO;
    }
    long bytesWritten = 0;
    if (verbose)
        fprintf(stderr, "W - %lu, %u\n", offset, len);

    for (long i = started; i < ended; i++)
    {
        int driveToWrite = i % (dev_fd_size - 1);
        int blockToWrite = i / (dev_fd_size - 1) * block_size;
        long bytesToWrite = len - bytesWritten > block_size ? block_size : len - bytesWritten;
        if (degraded && driveToWrite == fail_dev)
        {
            // only need to update the parity
            char parityBlock[bytesToWrite];
            memset(parityBlock, 0, bytesToWrite);
            for (int j = 0; j < dev_fd_size; j++)
            {
                if (j != fail_dev && j != parity_dev)
                {
                    char readBuf[bytesToWrite];
                    pread(dev_fd[j], readBuf, bytesToWrite, blockToWrite);
                    for (int k = 0; k < bytesToWrite; k++)
                    {
                        parityBlock[k] = parityBlock[k] ^ readBuf[k];
                    }
                }
            }
            // xor the newvalue with the parity
            for (int j = 0; j < bytesToWrite; j++)
            {
                parityBlock[j] = parityBlock[j] ^ ((char *)buf)[bytesWritten + j];
            }
            // write new parity
            long parityWr = pwrite(dev_fd[parity_dev], parityBlock, bytesToWrite, blockToWrite);
            bytesWritten += bytesToWrite;
        }
        else
        {
            // update parity first
            // get old value of the block to be updated
            char oldBlock[block_size];
            long oldbytes = pread(dev_fd[driveToWrite], oldBlock, block_size, blockToWrite);

            long wr = pwrite(dev_fd[driveToWrite], (char *)buf + bytesWritten, bytesToWrite, blockToWrite);

            // update parity
            // xor old value with new value
            char parityBlock[block_size];
            long oldParitybytes = pread(dev_fd[parity_dev], parityBlock, block_size, blockToWrite);
            for (int i = 0; i < bytesToWrite; i++)
            {
                parityBlock[i] = parityBlock[i] ^ oldBlock[i] ^ ((char *)buf)[i + bytesWritten];
            }
            // write new parity
            long parityWr = pwrite(dev_fd[parity_dev], parityBlock, block_size, blockToWrite);
            bytesWritten += wr;
        }
    }

    return 0;
}

static int xmp_flush(void *userdata)
{
    UNUSED(userdata);
    if (verbose)
        fprintf(stderr, "Received a flush request.\n");
    for (int i = 0; i < 2; i++)
    {
        if (dev_fd[i] != -1)
        {                     // handle degraded mode
            fsync(dev_fd[i]); // we use fsync to flush OS buffers to underlying devices
        }
    }
    return 0;
}

static void xmp_disc(void *userdata)
{
    UNUSED(userdata);
    if (verbose)
        fprintf(stderr, "Received a disconnect request.\n");
    // disconnect is a no-op for us
}

/*
// we'll disable trim support, you can add it back if you want it
static int xmp_trim(u_int64_t from, u_int32_t len, void *userdata) {
    UNUSED(userdata);
    if (verbose)
        fprintf(stderr, "T - %lu, %u\n", from, len);
    // trim is a no-op for us
    return 0;
}
*/

/* argument parsing using argp */

static struct argp_option options[] = {
    {"verbose", 'v', 0, 0, "Produce verbose output", 0},
    {"init", 'i', 0, 0, "Initialize the RAID", 0},
    {0},
};

struct arguments
{
    uint32_t block_size;
    char *device[16];
    char *raid_device;
    int verbose;
    int num_devices;
    bool need_init;
};

/* Parse a single option. */
static error_t parse_opt(int key, char *arg, struct argp_state *state)
{
    struct arguments *arguments = state->input;
    char *endptr;

    switch (key)
    {

    case 'v':
        arguments->verbose = 1;
        break;
    case 'i':
        arguments->need_init = true;
        break;

    case ARGP_KEY_ARG:
        switch (state->arg_num)
        {

        case 0:
            arguments->block_size = strtoul(arg, &endptr, 10);
            if (*endptr != '\0')
            {
                /* failed to parse integer */
                errx(EXIT_FAILURE, "SIZE must be an integer");
            }
            break;

        case 1:
            arguments->raid_device = arg;
            break;

        case 2:
            arguments->device[0] = arg;
            break;

        case 3:
            arguments->device[1] = arg;
            break;
        case 4:
            arguments->device[2] = arg;
            break;
        case 5:
            arguments->device[3] = arg;
            break;
        case 6:
            arguments->device[4] = arg;
            break;
        case 7:
            arguments->device[5] = arg;
            break;
        case 8:
            arguments->device[6] = arg;
            break;
        case 9:
            arguments->device[7] = arg;
            break;
        case 10:
            arguments->device[8] = arg;
            break;
        case 11:
            arguments->device[9] = arg;
            break;
        case 12:
            arguments->device[10] = arg;
            break;
        case 13:
            arguments->device[11] = arg;
            break;
        case 14:
            arguments->device[12] = arg;
            break;
        case 15:
            arguments->device[13] = arg;
            break;
        case 16:
            arguments->device[14] = arg;
            break;
        case 17:
            arguments->device[15] = arg;
            break;

        default:
            /* Too many arguments. */
            return ARGP_ERR_UNKNOWN;
        }
        break;

    case ARGP_KEY_END:
        if (state->arg_num < 5)
        {
            warnx("not enough arguments");
            argp_usage(state);
        }
        else
        {
            arguments->num_devices = state->arg_num - 2;
        }

        break;

    default:
        return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

static struct argp argp = {
    .options = options,
    .parser = parse_opt,
    .args_doc = "BLOCKSIZE RAIDDEVICE DEVICE1 DEVICE2",
    .doc = "BUSE implementation of RAID4 for up to 16 devices.\n"
           "`BLOCKSIZE` is an integer number of bytes. "
           "\n\n"
           "`RAIDDEVICE` is a path to an NBD block device, for example \"/dev/nbd0\"."
           "\n\n"
           "`DEVICE*` is a path to underlying block devices. Normal files can be used too. A `DEVICE` may be specified as \"MISSING\" to run in degraded mode. "
           "\n\n"
           "If you prepend '+' to a DEVICE, you are re-adding it as a replacement to the RAID, and we will rebuild the array. "
           "This is synchronous; the rebuild will have to finish before the RAID is started. "};

void printProgressBar(uint64_t current, uint64_t total)
{
    const int barWidth = 50;
    float progress = (float)current / total;
    int pos = barWidth * progress;

    printf("[");
    for (int i = 0; i < barWidth; ++i)
    {
        if (i < pos)
            printf("=");
        else if (i == pos)
            printf(">");
        else
            printf(" ");
    }
    printf("] %.2f%%\r", progress * 100);
    fflush(stdout);
}

static int do_raid_rebuild()
{
    // rebuild the rebuild_dev
    // compute the original data from the other drives based on the parity
    // write the data to the rebuild_dev
    char buf[block_size];
    char readBuf[block_size];
    fprintf(stdout, "Rebuilding...\n");
    memset(buf, 0, block_size);

    for (uint64_t cursor = 0; cursor < raid_device_size / block_size; cursor += block_size)
    {
        for (int i = 0; i < dev_fd_size; i++)
        {
            if (i != rebuild_dev)
            {
                pread(dev_fd[i], readBuf, block_size, cursor);
                for (int j = 0; j < block_size; j++)
                {
                    buf[j] = buf[j] ^ readBuf[j];
                }
            }
        }
        pwrite(dev_fd[rebuild_dev], buf, block_size, cursor);
        printProgressBar(cursor + block_size, raid_device_size / block_size);
    }
    printf("\n"); // Print a new line after the progress bar is complete

    return 0;
}

int main(int argc, char *argv[])
{
    struct arguments arguments = {
        .verbose = 0,
    };
    argp_parse(&argp, argc, argv, 0, 0, &arguments);

    struct buse_operations bop = {
        .read = xmp_read,
        .write = xmp_write,
        .disc = xmp_disc,
        .flush = xmp_flush,
        // .trim = xmp_trim, // we'll disable trim support, you can add it back if you want it
    };
    for (int i = 0; i < arguments.num_devices; i++)
    {
        fprintf(stderr, "Device %d: %s\n", i, arguments.device[i]);
    }
    verbose = arguments.verbose;
    block_size = arguments.block_size;
    dev_fd_size = arguments.num_devices;

    raid_device_size = 0; // will be detected from the drives available
    fail_dev = -1;
    parity_dev = dev_fd_size - 1; // last device is the parity device
    bool rebuild_needed = false;  // will be set to true if a drive is MISSING
    for (int i = 0; i < dev_fd_size; i++)
    {
        char *dev_path = arguments.device[i];
        if (strcmp(dev_path, "MISSING") == 0)
        {
            if (degraded)
            {
                fprintf(stderr, "ERROR: Can't have multiple MISSING drives. Aborting.\n");
                exit(1);
            }
            degraded = true;
            fail_dev = i;
            dev_fd[i] = -1;
            fprintf(stderr, "DEGRADED: Device number %d is missing!\n", i);
        }
        else
        {
            if (dev_path[0] == '+')
            { // RAID rebuild mode!!
                if (rebuild_needed)
                {
                    // multiple +drives detected
                    fprintf(stderr, "ERROR: Multiple '+' drives specified. Can only recover one drive at a time.\n");
                    exit(1);
                }
                dev_path++; // shave off the '+' for the subsequent logic
                rebuild_dev = i;
                rebuild_needed = true;
            }

            dev_fd[i] = open(dev_path, O_RDWR);
            if (dev_fd[i] < 0)
            {
                perror(dev_path);
                exit(1);
            }
            uint64_t size = lseek(dev_fd[i], 0, SEEK_END); // used to find device size by seeking to end
            fprintf(stderr, "Got device '%s', size %ld bytes.\n", dev_path, size);
            if (raid_device_size == 0 || size < raid_device_size)
            {
                raid_device_size = size * (dev_fd_size - 1); // we'll use the smallest device size as the RAID size
            }
        }
    }

    raid_device_size = raid_device_size / block_size * block_size; // divide+mult to truncate to block size
    bop.size = raid_device_size;                                   // tell BUSE how big our block device is
    bop.blksize = block_size;                                      // tell BUSE our block size
    bop.size_blocks = raid_device_size / block_size;               // tell BUSE our block count
    if (rebuild_needed)
    {
        if (degraded)
        {
            fprintf(stderr, "ERROR: Can't rebuild from a missing device (i.e., you can't combine MISSING and '+').\n");
            exit(1);
        }
        fprintf(stderr, "Doing RAID rebuild...\n");
        if (do_raid_rebuild() != 0)
        {
            // error on rebuild
            fprintf(stderr, "Rebuild failed, aborting.\n");
            exit(1);
        }
    }
    fprintf(stderr, "RAID device resulting size: %ld.\n", bop.size);
    if (degraded)
    {
        fprintf(stderr, "RAID is running in degraded mode.\n");
    }
    if (arguments.need_init)
    {
        if (degraded)
        {
            fprintf(stderr, "ERROR: Can't initialize a RAID with a missing device. Aborting.\n");
            exit(1);
        }
        fprintf(stderr, "Initializing RAID parity...\n");
        // write all files to zero
        char zero[block_size];
        memset(zero, 0, block_size);

        // Function to print the progress bar

        for (uint64_t cursor = 0; cursor < raid_device_size / block_size; cursor += block_size)
        {
            for (int i = 0; i < dev_fd_size; i++)
            {
                int r = pwrite(dev_fd[i], zero, block_size, cursor);
                if (r < 0)
                {
                    perror("init_write");
                    return -1;
                }
                else if (r != block_size)
                {
                    fprintf(stderr, "init_write: short write (%d bytes), offset=%zu\n", r, cursor);
                    return 1;
                }
            }

            // Print the progress bar
            printProgressBar(cursor + block_size, raid_device_size / block_size);
        }
        printf("\n"); // Print a new line after the progress bar is complete
    }
    return buse_main(arguments.raid_device, &bop, NULL);
}
