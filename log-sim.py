from datetime import datetime

import time
import os
import sys
import re
import socket
import argparse
import configparser

version = "2.00"

str_bool = ['No', 'Yes']

# NOTES:
# - \033[F = Moves the cursor up one line
# - \033[E = Moves the cursor down one line and places it at the beginning of that line
# - \033[K = Clear line to the end of the line

def _make_gen(reader):
    b = reader(1024 * 1024)
    while b:
        yield b
        b = reader(1024*1024)

def rawgencount(filename):
    f = open(filename, 'rb')
    f_gen = _make_gen(f.raw.read)
    return sum(buf.count(b'\n') for buf in f_gen)

def find_dt_pos(text):
    pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}'

    matches = re.finditer(pattern, text)
    pos = [(match.start(), match.end()) for match in matches]

    return pos

def main():

    print("-------------------------")
    print("log-parser Simulator " + version)
    print("-------------------------")
    print("")

    print("Environment:")
    if sys.platform == 'linux':
        print("- OS: Ubuntu")
    elif sys.platform == 'win32':
        print("- OS: Windows")
    else:
        print("- OS: unknown")
        sys.exit()

    print("")

    # Command line arguments

    parser = argparse.ArgumentParser(description='log-parser-sim')
    parser.add_argument('-m', '--mode', help='Network mode', required=True, choices=['cu', 'du', 'core'])
    parser.add_argument('-p', '--port', help='TCP listening port (Default=5010)', required=False, type=int, default=5010)
    parser.add_argument('-s', '--sync', help='1=Synchronized mode (Default=1)', required=False, type=int, choices=[0, 1], default=1)
    parser.add_argument('-d', '--del-res', help='1=Delete result files (Default=0)', required=False, type=int, choices=[0, 1], default=0)

    args = vars(parser.parse_args())

    net_mode = args['mode']
    sync_mode = args['sync'] == 1
    del_res = args['del_res'] == 1
    tcp_port = args['port']

    print("Options:")
    print("- Network mode:", net_mode)
    print("- Synchronized mode:", str_bool[sync_mode])
    print("- TCP listening port:", tcp_port)
    print("- Delete result files:", str_bool[del_res])
    print("")

    user = os.getlogin()

    # Configuration file

    if sys.platform == 'linux':
        ini_path = r'/home/' + user + r'/config/srs-log/'
        out_log_path = r'/home/' + user + '/logs/Logs/'
    else:
        ini_path = r'C:/Zona/Profesional/O-RAN/Org/Config srsRAN/Asus Gaming - UE - B210/home · ue · config/srs-log/'
        out_log_path = r'./-Logs-/Logs/'

    ini_file_base = 'config-parser-sim-' + net_mode + '.ini'

    ini_file = os.path.join(ini_path, ini_file_base)

    print("- Ini file path: " + ini_path)
    print("- Ini file name: " + ini_file_base)
    print("")

    print("INI file:")

    config = configparser.ConfigParser()
    config.read(ini_file)

    if config.has_option('config', 'tcp_port') and config['config']['tcp_port']:
        try:
            tcp_port = int(config['config']['tcp_port'])
            print("- TCP listening port: " + str(tcp_port))
        except ValueError:
            print("WARNING: non-integer value in 'tcp_port' parameter. Parameter ignored.")

    if config.has_option('config', 'inp_log_path') and config['config']['inp_log_path']:
        inp_log_path = config['config']['inp_log_path']
    else:
        inp_log_path = ''

    if config.has_option('config','inp_log_file') and config['config']['inp_log_file']:
        inp_log_file = config['config']['inp_log_file']
        inp_log_path_file = os.path.join(inp_log_path, inp_log_file)
    else:
        print("ERROR: input log file name not defined")
        sys.exit()

    if config.has_option('config','out_log_file') and config['config']['out_log_file']:
        out_log_file = config['config']['out_log_file']
        out_log_file_full = out_log_path + out_log_file
    else:
        print("ERROR: output log file name not defined")
        sys.exit()

    print("- Input log file path: " + inp_log_path)
    print("- Input log file name: " + inp_log_file)
    print("- Output log file path: " + out_log_path)
    print("- Output file: " + out_log_file)
    print("")

    key_int = False

    comm_dt_log_ref = None

    ic = 0  # Iterations counter

    # Open the output file for writing
    with open(out_log_file_full, 'w', encoding='utf-8') as out_file:

        try:

            while True:

                if sync_mode:
                    print("Waiting for 'sim' command from controller to establish a time reference and start the simulation ...")

                    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    server_socket.bind(('0.0.0.0', tcp_port))
                    server_socket.listen(1)

                    print(f"- Server listening on 0.0.0.0:{tcp_port}")

                    # Wait for command from controller to set log date-time reference

                    conn, addr = server_socket.accept()

                    print(f"- Connection established with {addr}")

                    with conn:
                        data = conn.recv(1024)

                        if not data:
                            print(f"\nNo data received. Connection with {addr} closed.")
                            server_socket.close()
                            sys.exit()

                        command = data.decode().strip()

                        if 'sim' in command.lower():
                            print("- Rx command '" + command + "'")
                            conn.sendall(b"Rx 'sim' command\n")

                            pos_dt = command.find("dt=")

                            comm_dt_log_iso = command[pos_dt + 3:pos_dt + 29]
                            comm_dt_log_ref = datetime.fromisoformat(comm_dt_log_iso)

                            pos_path = command.find("path=")
                            pos_end = command.find("<END>")
                            comm_log_path = command[pos_path + 5:pos_end]

                            print("· Path to log file:", comm_log_path)
                            print("· Reference log date-time (ISO):", comm_dt_log_iso)
                            print("· Reference log date-time (Datetime):", comm_dt_log_ref)

                        else:
                            print(f"\nUnknown command received. Connection with {addr} closed.")
                            server_socket.close()
                            sys.exit()

                    server_socket.close()

                    print("")

                inp_log_path_file_full = os.path.join(comm_log_path, inp_log_path_file)
                len_input = rawgencount(inp_log_path_file_full)
                print("- Input log file name: " + inp_log_path_file_full + " (" + str(len_input) + " lines)")
                print("")

                out_file.write("log-parser Simulator " + version + '\n')
                out_file.write("Current datetime: " + datetime.now().isoformat() + '\n')
                out_file.write("Input log file name: " + inp_log_path_file_full + '\n')
                out_file.write("Output log file name: " + out_log_file_full + '\n')
                out_file.write("\n")

                out_file.flush()

                dt_ref_log = None
                dt_ref_now = None

                ic+=1  # Iterations counter

                print("Iteration:", ic)
                # Open the input file for reading
                with open(inp_log_path_file_full, 'r', encoding='utf-8') as inp_file:

                    lc = 0  # Lines counter

                    for line in inp_file:

                        pos_dt = find_dt_pos(line)

                        if pos_dt:

                            # Extract date-time (ISO 8601) from log line
                            dt_iso = line[pos_dt[0][0]:pos_dt[0][1]]
                            dt_log = datetime.fromisoformat(dt_iso)

                            # Initialize date-time references
                            if not dt_ref_log:
                                if sync_mode:
                                    dt_ref_log = comm_dt_log_ref
                                else:
                                    dt_ref_log = dt_log

                                dt_ref_now = datetime.now()

                                print("Reference log date-time:", dt_ref_log)
                                print("Reference now date-time:", dt_ref_now)

                            t_start = time.time()

                            # Synchronize lines generation with log time-stamps
                            while True:
                                dt_now = datetime.now()

                                t_current = time.time()

                                log_ellapsed = dt_log - dt_ref_log
                                ellapsed_now = dt_now - dt_ref_now

                                if t_current - t_start > 1:

                                    dt_sim = dt_ref_log + ellapsed_now

                                    sys.stdout.write(f'Line: {lc}          \n')
                                    sys.stdout.write(f'- log ellapsed = {log_ellapsed}          \n')
                                    sys.stdout.write(f'- now ellapsed = {ellapsed_now}          \n')
                                    sys.stdout.write(f'- now dt = {dt_now}          \n')
                                    sys.stdout.write(f'- sim dt = {dt_sim}          \n')
                                    sys.stdout.write(f'- log dt = {dt_log}          ')

                                    sys.stdout.write("\033[F\033[F\033[F\033[F\033[F")
                                    sys.stdout.flush()

                                    t_start = time.time()

                                if ellapsed_now >= log_ellapsed:
                                    break
                                else:
                                    time.sleep(0.01)

                        lc+=1  # Line counter

                        out_file.write(line)
                        out_file.flush()  # Ensure the line is written immediately

                ellaped_log = dt_log - dt_ref_log
                ellapsed_now = dt_now - dt_ref_now

                print("Final log date-time:", dt_log, "       ")
                print("Final now date-time:", dt_now, "       ")
                print("Ellapsed log date-time:", ellaped_log, "       ")
                print("Ellapsed now date-time:", ellapsed_now, "       ")
                print("                                            ")

        except KeyboardInterrupt:
            sys.stdout.write("\033[E\033[E\033[E\033[E\033[E\033[E")
            print("")
            print("")
            key_int = True
        finally:
            out_file.close()
            if del_res:
                print("Deleting output file ...")
                os.remove(out_log_file_full)
            if key_int:
                sys.stdout.write('\x1b[2K\n')
                print("... script gracefully stopped")
            else:
                print("... script finished")

if __name__ == "__main__":
    main()