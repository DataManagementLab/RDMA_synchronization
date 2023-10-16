#!/usr/bin/env python3

import argparse
import logging
import sys

from distexprunner.experiment_server import ExperimentServer


__author__ = 'mjasny'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Distributed Experiment Runner Server')
    parser.add_argument('-v', '--verbose', action="count", default=0, help='-v WARN -vv INFO -vvv DEBUG')
    parser.add_argument('-ip', '--ip', default='0.0.0.0', help='Listening ip')
    parser.add_argument('-p', '--port', default=20000, help='Listening port')
    parser.add_argument('-rf', '--run-forever', default=False, action='store_true', help='Disable auto termination of server')
    parser.add_argument('-mi', '--max-idle', default=3600, type=int, help='Maximum idle time before auto termination (in seconds). Default 1 hour.')
    parser.add_argument('-o', '--log', type=str, help='Log into file')
    args = parser.parse_args()

    logging_handlers = [logging.StreamHandler()]
    if args.log:
        logging_handlers.append(logging.FileHandler(filename=args.log, mode='w'))


    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s', # [%(filename)s:%(lineno)d]: 
        datefmt='%Y-%m-%d %H:%M:%S',
        level=max(4 - args.verbose, 0) * 10,
        handlers=logging_handlers
    )


    server = ExperimentServer(
        ip=args.ip,
        port=args.port,
        max_idle=0 if args.run_forever else args.max_idle
    )
    server.start()