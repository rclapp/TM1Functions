import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from TM1py.Services import TM1Service
import pandas as pd
import csv
import configparser
import logging
import sys
import os.path

# create logger
logger = logging.getLogger('ExecuteParallelTI')
logger.setLevel(logging.DEBUG)

# create file handler which logs even debug messages
fh = logging.FileHandler('ExecuteParallelTI.log')
fh.setLevel(logging.INFO)

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

class TICommand():

    def __init__(self, process, parameters, key):
        self.process = process
        self.parameters = parameters
        self.key = key



def create_ti_instructions(csv_path):
    """
    :param csv_path: Takes in a path to a local CSV file in the format:
    ProcessName, ParamName1, ParamValue1, ParamNameN, ParamValueN
    :return: a list of TICommands to be run
    """
    ticommands = []
    with open(csv_path) as tifile:
        reader = csv.reader(tifile, delimiter=',')
        key = 1
        for row in reader:
            paramlist = []
            params = {}
            process = row[0]
            cols = len(row)
            #params["Parameters"] = {}
            for i in range(1, cols, 2):
                paramvals = {}
                paramvals["Name"] = row[i]
                paramvals["Value"] = row[i+1]
                paramlist.append(paramvals)
            params["Parameters"] = paramlist
            command = TICommand(process, params, key)
            ticommands.append(command)
            key += 1
    return ticommands


def execute_ti(tm1, process, parameters, key):
    try:
        logger.info("Row {} {}: Executing for {} on {}".format(key, process, parameters, tm1.server.get_server_name()))
        response = tm1.processes.execute(process, parameters)
        return "Row {} {}: Process Completed Successfully for {} on {}".format(key, process, parameters,
                                                                        tm1.server.get_server_name())
    except Exception as e:
        logger.error("Process Completed With Errors")
        return "Row {} {}: Produced Errors for {} on {} | {}".format(key, process, parameters,
                                                             tm1.server.get_server_name(), e._response)


async def execute_parallel_ti(tm1, commands, max_threads):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers = max_threads) as executor:
        futures = [loop.run_in_executor(executor, execute_ti, tm1, i.process, i.parameters, i.key) for i in commands]
        while futures:
            done, futures = await asyncio.wait(futures, loop=loop, return_when=asyncio.FIRST_COMPLETED)
            for f in done:
                await f
                logger.info(f.result())


def main():
    try:
        file_path = sys.argv[1]
    except:
        logger.fatal("No File Specified")

    if not os.path.isfile(file_path):
        logger.fatal("File not found, exiting")

    default_max_threads = 5
    try:
        max_threads = int(sys.argv[2])
    except:
        max_threads = default_maxthreads
        logger.warning("Max Parallel Threads Not Specified, Default: {}".format(default_max_threads))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    config = configparser.ConfigParser()
    config.read('config.ini')
    logger.info("Reading Config File")
    tis = create_ti_instructions(file_path)
    row_count = sum(1 for row in tis)
    logger.info("Reading file {}, {} lines read".format(file_path, row_count))
    logger.info("Maximum Parallel Threads: {}".format(max_threads))

    try:
        
        with TM1Service(**config['tm1srv01']) as tm1:
            logger.info("Connecting to: {}".format(tm1.server.get_server_name()))
            logger.info("Starting Processes found in: {}".format(file_path))
            start_time = time.clock()
            result = loop.run_until_complete(execute_parallel_ti(tm1, tis, max_threads))
            tm1.logout()
            logger.info("Logging Out")
            end_time = time.clock()
            elapsed_time = end_time - start_time
            logger.info("Total Time To Complete {} Processes: {}". Format(row_count, elapsed_time))
            loop.close()

    except Exception as e:
        logging.fatal("Unable to Connect to TM1")
        logging.fatal("Connection Error: {}". format(e))



if __name__ == '__main__':
    main()