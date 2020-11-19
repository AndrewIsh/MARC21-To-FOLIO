import argparse
from io import FileIO
from os.path import isfile, join
from posix import listdir
import queue
import threading
import time
import copy
import uuid

from pymarc.reader import MARCReader
from pymarc.record import Record


class Worker:
    def __init__(self, args):

        self.start = time.time()

        self.processed = 0
        self.migration_report = {}
        self.files = [
            join(args.source_folder, f)
            for f in listdir(args.source_folder)
            if isfile(join(args.source_folder, f))
        ]
        print(f"Files to process: {len(self.files)}")

    def work(self):
        filex = open("xml.txt", "w+")
        files = open("srs.json", "w+")
        for file_name in self.files:
            with open(file_name, "rb") as marc_file:
                reader = MARCReader(marc_file, "rb", permissive=True)
                marc_record: Record
                for marc_record in reader:
                    if marc_record:
                        folio_record = {"id": str(uuid.uuid4())}
                        filex.write(f"{marc_record.as_marc()}\n")
                        # files.write(str(uuid.uuid4()))
                        # filex.write(str(uuid.uuid4()))
                        files.write(f"{marc_record.as_json()}\n")
                        if marc_record:
                            for f in marc_record:
                                self.add_to_migration_report(
                                    "Present marc fields", f.tag
                                )
                            for p in folio_record:
                                self.add_to_migration_report("Present folio fields", p)
                        self.processed += 1
                        if self.processed % 1000 == 0:
                            elapsed = self.processed / (time.time() - self.start)
                            elapsed_formatted = "{0:.4g}".format(elapsed)
                            print(
                                f"{elapsed_formatted} records/sec.\t\t{self.processed:,} records processed"
                            )

        filex.close()
        files.close()
        self.write_migration_report()

    def do_something(self):
        data = str(uuid.uuid4())
        print("MAIN DONE!")

    def write_migration_report(self):
        for a in self.migration_report:
            print(f"   \n")
            print(f"## {a} - {len(self.migration_report[a])} things   \n")
            print(f"Measure | Count   \n")
            print(f"--- | ---:   \n")
            b = self.migration_report[a]
            sortedlist = [(k, b[k]) for k in sorted(b, key=as_str)]
            for b in sortedlist:
                print(f"{b[0]} | {b[1]}   \n")

    def add_to_migration_report(self, header, measure_to_add):
        if header not in self.migration_report:
            self.migration_report[header] = {}
        if measure_to_add not in self.migration_report[header]:
            self.migration_report[header][measure_to_add] = 1
        else:
            self.migration_report[header][measure_to_add] += 1


def parse_args():
    """Parse CLI Arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("source_folder", help="path to marc records folder")
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    worker = Worker(args)
    worker.work()


if __name__ == "__main__":
    main()


def as_str(s):
    try:
        return str(s), ""
    except ValueError:
        return "", s
