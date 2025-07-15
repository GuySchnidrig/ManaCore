import subprocess
import sys

def run_script(script_path):
    print(f"Running {script_path}...")
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        sys.exit(result.returncode)

def main():
    run_script("scripts/load_data.py")
    run_script("scripts/make_standings.py")

if __name__ == "__main__":
    main()