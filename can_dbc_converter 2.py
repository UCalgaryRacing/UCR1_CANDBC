import multiprocessing.process
import cantools
import csv
import os
import shutil
import multiprocessing
import time
import itertools
col = -1


class CanFrame:
    def __init__(self, timestamp, channel, can_id, flags, dlc, data) -> None:
        self.timestamp = timestamp
        self.channel = channel
        self.can_id = can_id
        self.flags = flags
        self.dlc = dlc
        self.data = data  # each element is one byte

    def __str__(self) -> str:
        return f"""
            Timestamp: {self.timestamp}
            Channel: {self.channel}
            CAN ID: {self.can_id}
            Flags: {self.flags}
            DLC: {self.dlc}
            Data: {self.data}
        """


def sort_key(header):
    if header.startswith("Cell") and header[4:].isdigit():
        return (0, int(header[4:]))  # Sort numerically for Cell#
    else:
        return (1, header)  # Sort alphabetically for others

dbc_path = "UCR-01.dbc"
db = cantools.database.load_file(dbc_path)

def parse_file(file_name, input_folder, output_path, database):
    output_csv_path = ""
    if file_name.endswith(".csv"):
        csv_path = os.path.join(input_folder, file_name)
        print(f"Processing file: {csv_path}")

        if not os.path.exists(output_path):
            os.makedirs(output_path)

        output_csv_path = os.path.join(
            f"{output_path}", f"decoded_{file_name}"
        )
        print(output_csv_path)

        with open(output_csv_path, mode="w", newline="") as output_csv_file:
            csv_writer = csv.writer(output_csv_file)

            header_set = set()

            with open(csv_path, newline="") as csv_file:
                can_data = csv.reader(csv_file)
                for i, row in enumerate(can_data):
                    if i >= 8:
                        row = [item for item in row if item != ""]
                        try:
                            data = bytes([(int(i, 16)) for i in row[5:col]])
                        except Exception as e:
                            print(f"Error converting data: {e}")
                            continue
                        can_id = int(row[2])

                        if can_id:
                            try:
                                frame = CanFrame(
                                    row[0], row[1], can_id, row[3], row[4], data
                                )
                                msg = database.decode_message(can_id, data)
                                header_set.update(msg.keys())

                            except Exception as e:
                                continue
                            except IndexError as e:
                                continue

            sorted_headers = sorted(header_set, key=sort_key)

            header_row = [
                "Timestamp",
                "Channel",
                "CAN ID",
                "Flags",
                "DLC",
            ] + sorted_headers
            csv_writer.writerow(header_row)

            with open(csv_path, newline="") as csv_file:
                can_data = csv.reader(csv_file)
                for i, row in enumerate(can_data):
                    if i >= 8:
                        row = [item for item in row if item != ""]
                        try:
                            data = bytes([(int(i, 16)) for i in row[5:col]])
                        except Exception as e:
                            print(f"Error converting data: {e}")
                            continue
                        can_id = int(row[2])

                        if can_id:
                            try:
                                frame = CanFrame(
                                    row[0], row[1], can_id, row[3], row[4], data
                                )
                                msg = database.decode_message(can_id, data)

                                row_data = [
                                    frame.timestamp,
                                    frame.channel,
                                    frame.can_id,
                                    frame.flags,
                                    frame.dlc,
                                ]

                                row_data += [
                                    msg.get(key, "") for key in sorted_headers
                                ]
                                csv_writer.writerow(row_data)

                            except Exception as e:
                                continue
                            except IndexError as e:
                                continue
    print(f"Decoded data has been exported to {output_csv_path}")

if __name__ == '__main__':
    user_dbc_path = input("Enter dbc path. If using default leave blank: ")
    if user_dbc_path != '':
        db = cantools.database.load_file(user_dbc_path)
        print("Updated dbc")
    
    
    input_path = input("Enter input data path: ")
    output_path = input("Enter output data path: ")
    start = time.time()
    files = os.listdir(input_path)
    thing = list(zip(files, [input_path for s in range(len(files))], [output_path for s in range(len(files))], [db for s in range(len(files))]))

    processes = multiprocessing.Pool(processes=(multiprocessing.cpu_count() - 1))
    processes.starmap(parse_file, thing)

    end = time.time()

    print("It took", end - start, "seconds")
        
        

