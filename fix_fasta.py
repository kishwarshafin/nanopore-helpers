import os
from argparse import ArgumentParser
from argparse import ArgumentDefaultsHelpFormatter
from collections import defaultdict
import shutil
import time
import os
import sys
import concurrent.futures
from datetime import datetime
"""
THIS SCRIPT FIXES FASTA FILES THAT DOESN'T HAVE '>' BEFORE READ NAME
"""


def check_if_line_is_only_sequence(line):
    line_list = list(line)
    for elm in line_list:
        if elm == 'A' or elm == 'C' or elm == 'G' or elm == 'T' or elm == 'N':
            continue
        else:
            return False

    return True


def fix_fasta(input_fasta):
    with open(input_fasta, "r") as ins:
        array = []
        for line in ins:
            line = line.rstrip()
            if check_if_line_is_only_sequence(line) is False:
                print(">"+line)
            else:
                print(line)


if __name__ == '__main__':
    '''
    Processes arguments and performs tasks.
    '''
    parser = ArgumentParser(
        formatter_class=ArgumentDefaultsHelpFormatter,
        add_help=False
    )
    parser.add_argument(
        "-i",
        "--input_fasta",
        type=str,
        required=True,
        help="Path to the input fasta file."
    )
    FLAGS, unparsed = parser.parse_known_args()
    fix_fasta(FLAGS.input_fasta)
