from datetime import datetime

import time
import os
import sys


def find_dt_pos(text):
    pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}'

    matches = re.finditer(pattern, text)
    pos = [(match.start(), match.end()) for match in matches]

    return pos

def main():

    print("-------------------------")
    print("Live-file simulator ")
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
    
    inp_data_file = 'data_source.txt'
    out_data_file = 'data_live.txt'

    print("- Source data file name: " + inp_data_file)
    print("- Lve data file name: " + out_data_file)
    print("")

    key_int = False

    ic = 0  # Iterations counter

    # Open the output file for writing
    with open(out_data_file, 'w', encoding='utf-8') as out_file:

        try:

            while True:

                out_file.flush()

                ic += 1  # Iterations counter

                print("Iteration:", ic)
                # Open the input file for reading
                with open(inp_data_file, 'r', encoding='utf-8') as inp_file:

                    for lc, line in enumerate(inp_file):

                        lc += 1  # Line counter

                        print(f"Line #{lc} read: {line.strip()}")

                        time.sleep(1)

                        out_file.write(line)
                        out_file.flush()  # Ensure the line is written immediately

        except KeyboardInterrupt:
            key_int = True
        finally:

            out_file.close()

            print("Deleting:", out_data_file, "...")
            os.remove(out_data_file)

            if key_int:
                sys.stdout.write('\x1b[2K\n')
                print("... script gracefully stopped")
            else:
                print("... script finished")

                
                

if __name__ == "__main__":
    main()