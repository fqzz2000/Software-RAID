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

int dev_fd[2];             // file descriptors for two underlying block devices that make up the RAID
int block_size;            // NOTE: other than truncating the resulting raid device, block_size is ignored in this program; it is asked for and set in order to make it easier to adapt this code to RAID0/4/5/6.
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

    // raid 0 read
    long started = offset / block_size;
    long ended = (offset + len) / block_size;
    long offsetInBlock = offset % block_size;
    if (verbose)
        fprintf(stderr, "started: %ld, ended: %ld\n", started, ended);
    if (offset + len > raid_device_size)
    {
        fprintf(stderr, "Read request exceeds device size.\n");
        return -EIO;
    }
    long bytesRead = 0;
    for (long i = started; i < ended; i++)
    {
        // if i % 2 == 0, read from dev_fd[0], else read from dev_fd[1]
        long bytesToRead = len - bytesRead > block_size ? block_size : len - bytesRead;
        if (i % 2 == 0)
        {

            long rd = pread(dev_fd[0], (char *)buf + bytesRead, bytesToRead, i / 2 * block_size);
            bytesRead += rd;
        }
        else
        {
            long rd = pread(dev_fd[1], (char *)buf + bytesRead, bytesToRead, i / 2 * block_size);
            bytesRead += rd;
        }
    }

    return 0;
}

static int xmp_write(const void *buf, u_int32_t len, u_int64_t offset, void *userdata)
{
    UNUSED(userdata);
    if (verbose)
        fprintf(stderr, "W - %lu, %u\n", offset, len);

    // raid 0 write
    long started = offset / block_size;
    long ended = (offset + len) / block_size;
    long offsetInBlock = offset % block_size;
    if (offset + len > raid_device_size)
    {
        fprintf(stderr, "Write request exceeds device size.\n");
        return -EIO;
    }

    long bytesWrite = 0;
    for (long i = started; i < ended; i++)
    {
        // if i % 2 == 0, write to dev_fd[0], else write to dev_fd[1]
        long bytesToWrite = len - bytesWrite > block_size ? block_size : len - bytesWrite;
        if (i % 2 == 0)
        {
            long wd = pwrite(dev_fd[0], (char *)buf + bytesWrite, bytesToWrite, i / 2 * block_size);
            bytesWrite += wd;
        }
        else
        {
            long wd = pwrite(dev_fd[1], (char *)buf + bytesWrite, bytesToWrite, i / 2 * block_size);
            bytesWrite += wd;
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
    {0},
};

struct arguments
{
    uint32_t block_size;
    char *device[2];
    char *raid_device;
    int verbose;
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

        default:
            /* Too many arguments. */
            return ARGP_ERR_UNKNOWN;
        }
        break;

    case ARGP_KEY_END:
        if (state->arg_num < 3)
        {
            warnx("not enough arguments");
            argp_usage(state);
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
    .doc = "BUSE implementation of RAID0 for two devices.\n"
           "`BLOCKSIZE` is an integer number of bytes. "
           "\n\n"
           "`RAIDDEVICE` is a path to an NBD block device, for example \"/dev/nbd0\"."
           "\n\n"
           "`DEVICE*` is a path to underlying block devices. Normal files can be used too."};

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

    verbose = arguments.verbose;
    block_size = arguments.block_size;

    raid_device_size = 0; // will be detected from the drives available
    ok_dev = -1;
    dev_fd[0] = open(arguments.device[0], O_RDWR);
    dev_fd[1] = open(arguments.device[1], O_RDWR);
    long fd0_size = lseek(dev_fd[0], 0, SEEK_END);
    long fd1_size = lseek(dev_fd[1], 0, SEEK_END);
    raid_device_size = 2 * (fd0_size < fd1_size ? fd0_size : fd1_size); // RAID0 size is the smaller of the two drives

    raid_device_size = raid_device_size / block_size * block_size; // divide+mult to truncate to block size
    bop.size = raid_device_size;                                   // tell BUSE how big our block device is
    bop.blksize = block_size;                                      // tell BUSE our block size
    bop.size_blocks = raid_device_size / block_size;               // tell BUSE our block count
    fprintf(stderr, "RAID device resulting size: %ld.\n", bop.size);

    return buse_main(arguments.raid_device, &bop, NULL);
}
