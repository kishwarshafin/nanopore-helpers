import os
from argparse import ArgumentParser
from argparse import ArgumentDefaultsHelpFormatter
from collections import defaultdict
import glob
from shutil import copyfile
from pathlib import Path
"""
USE THIS SCRIPT TO EXTRACT ALL READS FROM A POOL OF READS FOR A SPECIFIC SPECIES.
REQUIRES:
- A LIST OF READ IDS FOR EXTRACTION
- A SUMMARY FILE CONTAINING READ_ID -> FILE_NAME MAPPING
- A PATH TO THE SIGNAL FILES
"""


def get_all_signal_file_names(read_id_file, summary_file):
    summary_dictionary = defaultdict()
    with open(summary_file) as summary_file_pointer:
        for cnt, line in enumerate(summary_file_pointer):
            line = line.rstrip().split('\t')
            file_name = line[0]
            read_id = line[1]
            summary_dictionary[read_id] = file_name

    signal_files = []
    with open(read_id_file) as read_file_pointer:
        for cnt, line in enumerate(read_file_pointer):
            read_id = line.rstrip()
            if read_id in summary_dictionary.keys():
                signal_files.append(summary_dictionary[read_id])
            else:
                print("WARNING: ", read_id, " NOT PRESENT IN SUMMARY FILE")
    print("TOTAL " + str(len(signal_files)) + " READS FOUND\n")
    return signal_files


def get_absolute_filepath(extract_file_names, signal_directory, output_directory):
    if signal_directory[-1] != "/":
        signal_directory += "/"

    # process the output directory
    if output_directory[-1] != "/":
        output_directory += "/"
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    count = 0
    for root, dirs, files in os.walk(signal_directory):
        for file_name in files:
            if file_name in extract_file_names:
                print("COPYING " + file_name + " TO " + output_directory)
                src_file = os.path.join(root, file_name)
                copyfile(src_file, output_directory+file_name)
                count = count + 1

    print("TOTAL " + count + " FAST5 FILES COPIED TO " + output_directory)


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
        "--read_ids",
        type=str,
        required=True,
        help="Path to the input directory."
    )
    parser.add_argument(
        "-s",
        "--summary_file",
        type=str,
        required=True,
        help="Path to the output directory."
    )
    parser.add_argument(
        "-d",
        "--signal_directory",
        type=str,
        required=False,
        help="Path to the signal directory."
    )
    parser.add_argument(
        "-o",
        "--output_directory",
        type=str,
        required=False,
        help="Path to output directory."
    )
    FLAGS, unparsed = parser.parse_known_args()
    signal_file_names = get_all_signal_file_names(FLAGS.read_ids, FLAGS.summary_file)
    get_absolute_filepath(signal_file_names, FLAGS.signal_directory, FLAGS.output_directory)
