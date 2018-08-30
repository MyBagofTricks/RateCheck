#!/usr/bin/python3
""" Scans a folder recursively for mp3s and outputs their encoding details to a .csv"""
import os
import csv
import time
import asyncio
from argparse import ArgumentParser
import logging

from mutagen.mp3 import MP3
from mutagen import MutagenError

logging.basicConfig(format='[%(asctime)s] %(message)s')
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

CSV_QUEUE = asyncio.Queue()


def parse_args():
    """Sets up and parses command line args. Return directory to scan, number of
    processes, and verbosity"""
    parser = ArgumentParser(
            description="Searches a folder for mp3s and creates a csv file, listing their bitrate")
    parser.add_argument(
            "directory_path", help="Full path to scan. Use quotes if directory has spaces")
    parser.add_argument(
            "-p", "--processes", type=int, help="Number of processes to use. Default = 3",
            default=3)
    parser.add_argument(
        "-v", "--verbose", help="Verbose log output", action="store_true"
        )
    args = parser.parse_args()
    return args.directory_path, args.processes, args.verbose

async def path_generator(base_directory):
    """Crawls a base_directory(str) and yield mp3 files found"""
    for root, _, files in os.walk(base_directory):
        for filename in files:
            await asyncio.sleep(0)
            path = os.path.abspath(os.path.join(root, filename))
            if path[-3:] == 'mp3':
                yield path

async def bitcheck(path):
    """Scans mp3 using path(str), and Return the bitrate"""
    try:
        bitrate = MP3(path).info.bitrate / 1000
    except MutagenError as err:
        LOGGER.error(f"Error when scanning file! [{path}] {err}")
        bitrate = 0
    return bitrate

async def task_scheduler(base_directory):
    """Coroutine that schedules scanning of mp3s in base_directory(str),
    adds results to global queue
    """
    LOGGER.info(f"Scanning {base_directory}...")
    async for path in path_generator(base_directory):
        bitrate = await bitcheck(path)
        await CSV_QUEUE.put((path, bitrate))    

async def csv_worker(future, f_name='output.csv'):
    """Coroutine that pulls items from a global queue (CSV_QUEUE), and adds to a csv file"""
    with open(f_name, 'w', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=['path', 'bitrate'],
            delimiter=';', lineterminator='\n'
        )
        writer.writeheader()
        total_written = 0
        while True:
            try:
                path, bitrate = await asyncio.wait_for(CSV_QUEUE.get(), 1)
            except asyncio.TimeoutError as err:
                return total_written
            else:
                writer.writerow({'path': path, 'bitrate': bitrate})
                CSV_QUEUE.task_done()
                total_written+=1
                if total_written % 1000 == 0:
                    LOGGER.debug(f"{total_written} items written to csv")
   
async def queue_printer():
    """Intermittently prints the number of items in queue to be added to the csv file"""
    while True:
        await asyncio.sleep(1)
        LOGGER.debug(f"{CSV_QUEUE.qsize()} items in queue")
        if CSV_QUEUE.empty():
            break


if __name__ == '__main__':
    
    base_directory, processes, verbose = parse_args()
    if verbose:
        LOGGER.setLevel(logging.DEBUG)
    LOGGER.info("This scan may take a while, depending on the number of mp3s, and hardware")

    t1 = time.time()
    loop = asyncio.get_event_loop()
    scanner = asyncio.ensure_future(task_scheduler(base_directory))
    csv_writer = asyncio.ensure_future(csv_worker(scanner))
    printer = asyncio.ensure_future(queue_printer())
    futures = asyncio.gather(scanner, csv_writer, printer)
    results = loop.run_until_complete(futures)
    loop.close()

    processed = results[1]
    total_time = time.time()-t1
    rate = processed / total_time
    LOGGER.info(f"Processed {processed} and wrote to csv in {total_time:.2f} seconds")
    LOGGER.info(f"Total speed: {rate:.2f} per second")
