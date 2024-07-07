import os

# Define the input file path and the number of output files
input_file_path = '/Users/patel/Desktop/work_dataset.json'
output_file_directory = '/Users/patel/Desktop/101_json/'
number_of_output_files = 100

# Function to calculate chunk size
def calculate_chunk_size(total_records, num_files):
    chunk_size = total_records // num_files
    remainder = total_records % num_files
    return chunk_size, remainder

# Read input JSON file line by line and distribute into output files
def distribute_json_lines(input_file_path, output_file_directory, number_of_output_files):
    # Open the input file and read lines
    with open(input_file_path, 'r') as input_file:
        lines = input_file.readlines()

    total_lines = len(lines)
    chunk_size, remainder = calculate_chunk_size(total_lines, number_of_output_files)

    # Distribute lines into output files
    for i in range(number_of_output_files):
        start_idx = i * chunk_size + min(i, remainder)
        end_idx = start_idx + chunk_size + (1 if i < remainder else 0)
        
        # Output file path
        output_file_path = os.path.join(output_file_directory, f'output_file_{i+1}.json')

        # Write lines to output file
        with open(output_file_path, 'w') as output_file:
            for line in lines[start_idx:end_idx]:
                output_file.write(line)

    print(f'Successfully distributed {total_lines} lines into {number_of_output_files} JSON files.')

# Call the function to distribute lines from input file into output files
distribute_json_lines(input_file_path, output_file_directory, number_of_output_files)
