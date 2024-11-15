import csv
import os
# import cudf.pandas
# cudf.pandas.install()
# %load_ext cudf.pandas
import pandas as pd
import multiprocessing
import platform
import time
import cantools
import cudf
col = -2


class CanFrame (object):
    """Class representing a CAN frame"""

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
    """Function for sorting the headers"""
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

        plain_data_path = os.path.join(
            f"{output_path}", f"Regular Data"
        )
        if not os.path.exists(plain_data_path):
            os.makedirs(plain_data_path, exist_ok=True)

        output_csv_path = os.path.join(
            f"{plain_data_path}", f"regular_{file_name}"
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
    dataFrame = pd.read_csv(output_csv_path)
    dataFrame = dataFrame.interpolate(method='linear', limit_direction='forward', axis=0)
    filled_data_path = os.path.join(
        f"{output_path}", f"Linear Interpolation"
    )
    if not os.path.exists(filled_data_path):
        os.makedirs(filled_data_path, exist_ok=True)
    output_csv_path = os.path.join(
        f"{filled_data_path}", f"linear_{file_name}"
    )
    try:
        dataFrame.to_csv(output_csv_path, sep=',')
    except Exception as e:
        print("Unable to export {output_csv_path}")
    print(f"Interpolated decoded data has been exported to {output_csv_path}")


if __name__ == '__main__':
    print(platform.platform())
    # user_dbc_path = input("Enter dbc path. If using default leave blank: ")
    # if user_dbc_path != '':
    #     db = cantools.database.load_file(user_dbc_path)
    #     print("Updated dbc")
    
    # input_path = r"K:\UCalgary Racing\UCR-01\5. Testing Data\2024-11-11 Lot 10 Aero Test and Drive Day 4\Raw Data\11-11\skidpad"
    # output_path = r"K:\UCalgary Racing\UCR-01\5. Testing Data\2024-11-11 Lot 10 Aero Test and Drive Day 4\Decoded Data\Skid Pad"
    # input_path = input("Enter input data path: ")
    # output_path = input("Enter output data path: ")


    path = os.path.join("/mnt/k/UCalgary Racing/UCR-01/5. Testing Data/2024-11-10 Lot 10 Drive Day 3/Decoded Data/Endurance/Regular Data/", "regular_11-10_008.csv")
    # path = "/mnt/k/UCalgary Racing/UCR-01/5. Testing Data/2024-11-11 Lot 10 Aero Test and Drive Day 4/Decoded Data/Auto Cross/Regular Data/regular_11-10_008.csv"

    # PANDAS Rounded
    data_Frame = pd.read_csv(path)
    start = time.time()
    data_Frame = data_Frame.interpolate(method='linear', limit_direction='forward', axis=0)
    data_Frame[data_Frame.columns.difference([
        "gnss_lat", "gnss_long", "gnss_height", "north_vel", "east_vel", "up_vel", "roll", 
        "pitch", "azimuth", "lat", "long", "hgt", "lat_std_dev", "long_std_dev", "height_std_dev", "z_accel"
        "neg_y_accel", "x_accel", "z_gyro","neg_y_gyro", "x_gyro"])].round(4)
    end = time.time()
    print("It took", end - start, "seconds with just pandas")
    start = time.time()
    data_Frame.to_csv('pandas.csv')
    end = time.time()
    print("It took", end - start, "seconds to write with pandas")
    print(len(data_Frame))

    # # PANDAS FEATHER
    # start = time.time()
    # data_Frame.to_feather('pandas.feather')
    # end = time.time()
    # print("It took", end - start, "seconds to write with pandas feather")
    # print(len(data_Frame))

    # start = time.time()
    # # dir_path = os.path.abspath(input_path)
    # # all_files = ( os.path.join(basedir, filename) for basedir, dirs, files in os.walk(dir_path) for filename in files   )
    # # sorted_files = sorted(all_files, key=os.path.getsize, reverse=True)
    # # thing = list(zip(sorted_files, [input_path for s in range(len(sorted_files))], [output_path for s in range(len(sorted_files))], [db for s in range(len(sorted_files))]))
    
    
    # # files = os.listdir(input_path)
    # # thing = list(zip(files, [input_path for s in range(len(files))], [output_path for s in range(len(files))], [db for s in range(len(files))]))

    # # processes = multiprocessing.Pool(processes=(multiprocessing.cpu_count() - 1))
    # # processes.starmap(parse_file, thing)

    # # path = "K:/UCalgary Racing/UCR-01/5. Testing Data/2024-11-11 Lot 10 Aero Test and Drive Day 4/Raw Data/11-11/autocross/11-11_025.csv"

    # # CUDF NORMAL
    # start = time.time()
    # data_Frame = cudf.read_csv(path)
    # data_Frame = data_Frame.interpolate(method='linear', limit_direction='forward', axis=0)
    # end = time.time()
    # print("It took", end - start, "seconds to interpolate with cudf")
    # start = time.time()
    # data_Frame.to_csv('cudf.csv')
    # end = time.time()
    # print("It took", end - start, "seconds to write with cudf")
    # print(len(data_Frame))

    # CUDF Rounded
    start = time.time()
    data_Frame = cudf.read_csv(path)
    data_Frame = data_Frame.interpolate(method='linear', limit_direction='forward', axis=0)
    data_Frame[data_Frame.columns.difference([
        "gnss_lat", "gnss_long", "gnss_height", "north_vel", "east_vel", "up_vel", "roll", 
        "pitch", "azimuth", "lat", "long", "hgt", "lat_std_dev", "long_std_dev", "height_std_dev", "z_accel"
        "neg_y_accel", "x_accel", "z_gyro","neg_y_gyro", "x_gyro"])].round(4)
    end = time.time()
    print("It took", end - start, "seconds to interpolate with cudf")
    start = time.time()
    data_Frame.to_csv('cudf.csv')
    end = time.time()
    print("It took", end - start, "seconds to write with cudf")
    print(len(data_Frame))


    # start = time.time()
    # data_Frame.to_feather('cudf.feather')
    # end = time.time()
    # print("It took", end - start, "seconds to write with cudf feather")
    # print(len(data_Frame))
