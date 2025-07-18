import sys
import os
import subprocess

# Add project root to sys.path to import manacore package locally
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

def run_script(script_path):
    print(f"Running {script_path}...")

    # Set PYTHONPATH env so subprocess finds the package
    env = os.environ.copy()
    env["PYTHONPATH"] = root_path + (":" + env.get("PYTHONPATH", "") if env.get("PYTHONPATH") else "")

    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True,
        env=env
    )

    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        sys.exit(result.returncode)

def main():
    run_script("scripts/load_data.py")
    run_script("scripts/make_standings.py")
    run_script("scripts/get_latest_cube.py")
    run_script("scripts/get_card_history.py")
    run_script("scripts/get_elo.py")

if __name__ == "__main__":
    main()
