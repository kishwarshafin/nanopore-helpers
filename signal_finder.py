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
                sys.stderr.write("WARNING: ", read_id, " NOT PRESENT IN SUMMARY FILE\n")
                sys.stderr.flush()

    sys.stderr.write("TOTAL " + str(len(signal_files)) + " READS FOUND\n")
    sys.stderr.flush()
    return signal_files


def move_files(signal_directory, output_directory, all_extract_file_names, thread_id, total_threads):
    extract_file_names = [r for i, r in enumerate(all_extract_file_names) if i % total_threads == thread_id]
    thread_prefix = "[THREAD " + "{:02d}".format(thread_id) + "]"
    sys.stderr.write(thread_prefix + " GOT: " + str(len(extract_file_names)) + " FILES\n")
    sys.stderr.flush()

    if signal_directory[-1] != "/":
        signal_directory += "/"

    # process the output directory
    if output_directory[-1] != "/":
        output_directory += "/"
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    count = 0
    total = len(extract_file_names)
    start_time = time.time()
    for root, dirs, files in os.walk(signal_directory):
        for file_name in files:
            if file_name in extract_file_names:
                src_file = os.path.join(root, file_name)
                if os.path.isfile(output_directory+file_name):
                    sys.stderr.write("FILE ALREADY EXISTS IN DESTINATION: " + file_name + "\n")
                    sys.stderr.flush()
                else:
                    shutil.copy(src_file, output_directory+file_name)

                count = count + 1
                if count % 1000 == 0:
                    percent_complete = int((100 * count) / total)
                    time_now = time.time()
                    mins = int((time_now - start_time) / 60)
                    secs = int((time_now - start_time)) % 60
                    sys.stderr.write("[" + datetime.now().strftime('%m-%d-%Y %H:%M:%S') + "]"
                                     + " INFO: " + str(thread_id) + " " + str(count) + "/" + str(total)
                                     + " COMPLETE (" + str(percent_complete) + "%)"
                                     + " [ELAPSED TIME: " + str(mins) + " Min " + str(secs) + " Sec]\n")
                    sys.stderr.flush()

    sys.stderr.write("TOTAL " + str(count) + "/" + str(total) + " FAST5 FILES COPIED TO " + output_directory + "\n")
    sys.stderr.flush()


def parallel_method_master(all_extract_file_names, signal_directory, output_directory, total_threads):
    start_time = time.time()
    with concurrent.futures.ProcessPoolExecutor(max_workers=total_threads) as executor:
        futures = [executor.submit(move_files, signal_directory, output_directory,
                                   all_extract_file_names, thread_id, total_threads)
                   for thread_id in range(0, total_threads)]

        for fut in concurrent.futures.as_completed(futures):
            if fut.exception() is None:
                # get the results
                thread_id = fut.result()
                sys.stderr.write("[" + datetime.now().strftime('%m-%d-%Y %H:%M:%S') + "] "
                                 + "THREAD " + str(thread_id) + " FINISHED SUCCESSFULLY.\n")
            else:
                sys.stderr.write("ERROR: " + str(fut.exception()) + "\n")
            fut._result = None  # python issue 27144

    end_time = time.time()
    mins = int((end_time - start_time) / 60)
    secs = int((end_time - start_time)) % 60
    sys.stderr.write("ELAPSED TIME: " + str(mins) + " Min " + str(secs) + " Sec\n")


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
    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        required=False,
        default=1,
        help="Number of threads to use."
    )
    FLAGS, unparsed = parser.parse_known_args()
    signal_file_names = get_all_signal_file_names(FLAGS.read_ids, FLAGS.summary_file)
    parallel_method_master(signal_file_names, FLAGS.signal_directory, FLAGS.output_directory, FLAGS.threads)
