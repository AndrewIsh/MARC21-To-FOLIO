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


class MigrationReporter(threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.exitFlag = 0
        self.processed = 0
        self.queueLock = threading.Lock()
        self.name = name
        self.q = q
        self.migration_report = {}
        self.stats = {}

    def run(self):
        print("Starting " + self.name)
        self.report(self.name, self.q)
        print("Exiting " + self.name)

    def write_migration_report(self, report_file):
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

    def report(self, threadName, q):
        while not self.exitFlag:
            self.queueLock.acquire()
            if not q.empty():
                data = q.get()
                self.queueLock.release()
                """if data[0]:
                    for f in data[0]:
                        self.add_to_migration_report("Present marc fields", f.tag)
                for p in data[1]:
                    self.add_to_migration_report("Present folio fields", p)"""
                self.processed += 1
                if self.processed % 10000 == 0:
                    print(f"{self.processed} processed in {self.name}")
            else:
                self.queueLock.release()


class MARCXMLWriter(threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.exitFlag = 0
        self.processed = 0
        self.queueLock = threading.Lock()
        self.name = name
        self.q = q
        self.file = open("xml.txt", "w+")

    def run(self):
        print("Starting " + self.name)
        self.process_x_data(self.name, self.q)  # self.file)
        print("Exiting " + self.name)
        self.file.close()

    def process_x_data(self, threadName, q):  # file):
        while not self.exitFlag:
            self.queueLock.acquire()
            if not q.empty():
                data = q.get()
                self.queueLock.release()
                # self.file.write(f"{data.as_marc()}\n")
                self.processed += 1
                if self.processed % 10000 == 0:
                    print(f"{self.processed} processed in {self.name}")
            else:
                self.queueLock.release()


class SRSWriter(threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.queueLock = threading.Lock()
        self.name = name
        self.processed = 0
        self.q = q
        self.exitFlag = 0
        self.file = open("xml.txt", "w+")

    def run(self):
        print("Starting " + self.name)
        self.process_s_data(self.name, self.q, self.file)
        print("Exiting " + self.name)
        self.file.close()

    def process_s_data(self, threadName, q, file):
        while not self.exitFlag:
            self.queueLock.acquire()
            if not q.empty():
                data = q.get()
                self.queueLock.release()
                # if data:
                # file.write(f"{data.as_json()}\n")
                self.processed += 1
                if self.processed % 10000 == 0:
                    print(f"{self.processed} processed in {self.name}")
            else:
                self.queueLock.release()


class Worker:
    def __init__(self, args):
        self.srsQueue = queue.Queue(1000)
        self.xmlQueue = queue.Queue(1000)
        self.migration_report_queue = queue.Queue(1000)
        self.start = time.time()

        self.x_writer = MARCXMLWriter(1, "MARC XML Writer", self.xmlQueue)
        self.x_writer2 = MARCXMLWriter(5, "MARC XML Writer", self.xmlQueue)

        self.srs_writer = SRSWriter(2, "SRS Writer", self.srsQueue)
        self.srs_writer2 = SRSWriter(5, "SRS Writer", self.srsQueue)
        self.migration_reporter = MigrationReporter(
            6, "Migration reporter", self.migration_report_queue
        )
        self.migration_reporter2 = MigrationReporter(
            3, "Migration reporter", self.migration_report_queue
        )

        self.threads = [
            self.x_writer,
            self.srs_writer,
            self.migration_reporter,
            self.x_writer2,
            self.srs_writer2,
            self.migration_reporter2,
        ]
        # self.threads = [self.srs_writer, self.migration_reporter]
        for t in self.threads:
            t.start()
        # self.srs_writer.start()
        # self.migration_reporter.start()
        self.processed = 0
        self.files = [
            join(args.source_folder, f)
            for f in listdir(args.source_folder)
            if isfile(join(args.source_folder, f))
        ]
        print(f"Files to process: {len(self.files)}")

    def work(self):
        for file_name in self.files:
            with open(file_name, "rb") as marc_file:
                reader = MARCReader(marc_file, "rb", permissive=True)
                marc_record: Record
                for marc_record in reader:
                    folio_record = {"id": str(uuid.uuid4())}
                    self.migration_report_queue.put(
                        (copy.deepcopy(marc_record), folio_record)
                    )
                    self.srsQueue.put(copy.deepcopy(marc_record))
                    self.xmlQueue.put(copy.deepcopy(marc_record))
                    self.processed += 1
                    if self.processed % 1000 == 0:
                        elapsed = self.processed / (time.time() - self.start)
                        elapsed_formatted = "{0:.4g}".format(elapsed)
                        print(
                            f"{elapsed_formatted} records/sec.\t\t{self.processed:,} records processed"
                        )

        print("LOADED QUEUE")
        while (
            not self.srsQueue.empty()
            and not self.xmlQueue.empty()
            and not self.migration_report_queue.empty()
        ):
            pass
        # Notify threads it's time to exit
        for t in self.threads:
            t.exitFlag = 1

        # Wait for all threads to complete
        for t in self.threads:
            t.join()
        print("Exiting Main Thread")

    def do_something(self):
        data = str(uuid.uuid4())
        print("MAIN DONE!")


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
