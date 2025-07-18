import sys
import os
import subprocess
import datetime

# Add project root to sys.path to import manacore package locally
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

def run_script(script_path, continue_on_error=False):
    """
    Run a Python script with proper environment setup and error handling.
    
    Args:
        script_path (str): Path to the script to run
        continue_on_error (bool): If True, continue execution even if script fails
    """
    # Check if script exists
    if not os.path.exists(script_path):
        print(f"Error: Script {script_path} not found")
        if not continue_on_error:
            sys.exit(1)
        return False
    
    # Add timestamp to output
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] Running {script_path}...")
    
    # Set PYTHONPATH env so subprocess finds the package
    env = os.environ.copy()
    env["PYTHONPATH"] = root_path + (":" + env.get("PYTHONPATH", "") if env.get("PYTHONPATH") else "")
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            env=env,
            timeout=1800  # 30 minute timeout
        )
        
        # Always print stdout if there is any
        if result.stdout.strip():
            print(result.stdout)
        
        if result.returncode != 0:
            print(f"Error: {script_path} failed with return code {result.returncode}")
            if result.stderr.strip():
                print("STDERR:")
                print(result.stderr)
            
            if not continue_on_error:
                sys.exit(result.returncode)
            else:
                print(f"Warning: {script_path} failed but continuing...")
                return False
        else:
            completion_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{completion_time}] ✓ {script_path} completed successfully")
            return True
            
    except subprocess.TimeoutExpired:
        print(f"Error: {script_path} timed out after 30 minutes")
        if not continue_on_error:
            sys.exit(1)
        return False
    except Exception as e:
        print(f"Error running {script_path}: {str(e)}")
        if not continue_on_error:
            sys.exit(1)
        return False

def main():
    """Main function to orchestrate all scripts."""
    start_time = datetime.datetime.now()
    print(f"Starting script execution at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    scripts = [
        "scripts/load_data.py",
        "scripts/make_standings.py", 
        "scripts/get_latest_cube.py",
        "scripts/get_card_history.py",
        "scripts/get_elo.py"
    ]
    
    successful_scripts = []
    failed_scripts = []
    
    # Run each script with progress tracking
    for i, script in enumerate(scripts, 1):
        print(f"\nStep {i}/{len(scripts)}: {script}")
        print("-" * 40)
        
        success = run_script(script, continue_on_error=False)
        
        if success:
            successful_scripts.append(script)
        else:
            failed_scripts.append(script)
    
    # Summary report
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    
    print("\n" + "=" * 60)
    print("EXECUTION SUMMARY")
    print("=" * 60)
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total duration: {duration}")
    print(f"Successful scripts: {len(successful_scripts)}/{len(scripts)}")
    
    if successful_scripts:
        print("\n✓ Successful:")
        for script in successful_scripts:
            print(f"  - {script}")
    
    if failed_scripts:
        print("\n✗ Failed:")
        for script in failed_scripts:
            print(f"  - {script}")
        print(f"\nExiting with error code 1")
        sys.exit(1)
    else:
        print("\n🎉 All scripts completed successfully!")

if __name__ == "__main__":
    main()