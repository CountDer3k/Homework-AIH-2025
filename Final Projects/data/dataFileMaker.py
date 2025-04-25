import os
import json

def generate_data_per_directory(root_dir):
    allowed_extensions = [".png", ".jpg"]
    master_file_data = []

    for dirpath, dirnames, filenames in os.walk(root_dir):
        current_dir_data = []

        for filename in filenames:
            if os.path.splitext(filename)[1].lower() in allowed_extensions:
                full_path = os.path.join(dirpath, filename)
                first_letter = filename[0]
                current_dir_data.append([full_path, first_letter])
                master_file_data.append([full_path, first_letter])

        # Create data.json inside the current directory if there are valid files
        if current_dir_data:
            json_path = os.path.join(dirpath, 'data.json')
            with open(json_path, 'w') as json_file:
                json.dump(current_dir_data, json_file, indent=4)
            print(f"Created {json_path} with {len(current_dir_data)} entries.")

    # Create master data.json in root directory
    master_json_path = os.path.join(root_dir, 'data.json')
    with open(master_json_path, 'w') as master_json:
        json.dump(master_file_data, master_json, indent=4)
    print(f"\nCreated master data.json in {root_dir} with {len(master_file_data)} total entries.")

if __name__ == "__main__":
    root_directory = input("Enter the path to the root directory: ")
    if os.path.isdir(root_directory):
        generate_data_per_directory(root_directory)
    else:
        print("Invalid directory path!")