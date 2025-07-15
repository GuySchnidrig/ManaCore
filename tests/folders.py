import os

def print_folder_structure(root, indent=0):
    for item in sorted(os.listdir(root)):
        path = os.path.join(root, item)
        print("    " * indent + "|-- " + item)
        if os.path.isdir(path):
            print_folder_structure(path, indent + 1)

project_root = r"G:\My Drive\MTG\ManaCore"  # Change to your path
print_folder_structure(project_root)