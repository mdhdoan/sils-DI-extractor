import os
import sys

def remove_spaces_in_filenames(directory):
    try:
        for filename in os.listdir(directory):
            old_path = os.path.join(directory, filename)
            # Skip if not a file
            if not os.path.isfile(old_path):
                continue
            
            new_filename = filename.replace(" ", "_")  # Replace spaces with underscores
            new_path = os.path.join(directory, new_filename)
            
            # Rename the file if the name has changed
            if old_path != new_path:
                os.rename(old_path, new_path)
                print(f"Renamed: '{filename}' to '{new_filename}'")
        
        print("All files have been processed.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Usage example
directory = sys.argv[1]  # Replace with your directory path
remove_spaces_in_filenames(directory)