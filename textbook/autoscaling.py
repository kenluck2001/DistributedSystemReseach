import subprocess
import psutil
import time
import os

MIN_THRESHOLD = 25
MAX_THRESHOLD = 75
PULSE_TIME = 300  # MAIN LOOP INTERVAL DELAY. It capture how frequent we have to provision new nodes.

###########################################################################
'''
This is a demonstration of how to build a basic autoscaling framework 
for increasing or descreasing the number of nodes in a distributed system 
using the heuristics of the average CPU load.
'''

###########################################################################
def run_command_nonblocking(command, shell=False):
    """
    Executes a command and returns the process ID immediately. 
    The caller is responsible for handling process completion and output.

    Args:
        command (str or list): The command to execute.
        shell (bool): Whether to execute via the shell.

    Returns:
        process.
    """
    process = subprocess.Popen(command, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return process  # Return both PID and process object


def get_process_output(process):
    """
    Gets the output (stdout, stderr) and return code of a process.

    Args:
        process (subprocess.Popen): The process object.

    Returns:
        tuple: (stdout, stderr, returncode)
    """
    stdout, stderr = process.communicate()
    returncode = process.returncode
    return stdout, stderr, returncode


def is_process_running(pid):
    """
    Checks if a process with the given PID is currently running.

    Args:
        pid (int): The process ID to check.

    Returns:
        bool: True if the process is running, False otherwise.
    """
    return psutil.pid_exists(pid)


def kill_process(process):
    """
    Kill a command.

    Args:
        process (process): process object

    Returns:
        Nothing
    """
    pid = process.pid
    returncode = process.returncode
    process.terminate()
    # Check if the process is still alive
    if not process.poll():
        print("Process: {} is still running, killing it...".format(pid))
        process.kill()
        print("Process killed with return code: {}".format(returncode))
        #time.sleep(10)
    else:
        print("Process terminated successfully.")
    process.wait()


def run_process(process):
    """
    run a process.

    Args:
        process (process): process object

    Returns:
        Nothing
    """
    pid = process.pid
    print("Command started with PID: {}".format(pid))
    # Example: Check if process is still running
    if is_process_running(pid):
        print("Process {} is still running.".format(pid))

    try:
        stdout, stderr, returncode = get_process_output(process) 
        print("Command finished.")
        print("Stdout:\n{}".format(stdout))
        if stderr:
            print("Stderr:\n{}".format(stderr))
        print("Return code: {}".format(returncode))
    except subprocess.CalledProcessError as e:
        print(e)
    finally:
        if is_process_running(pid):
            kill_process(process) # Ensure cleanup


def autoscale_processes():
    """
    Autoscaling of processes according average cpu load across nodes

    Args:
        Nothing

    Returns:
        Nothing
    """
    command = None
    process = None
    pid = None
    num_of_processes = 8
    previous_cpu_avg = 0

    # avoid re-running loop when not necessary
    while True:
        cmd = "mpicc -g cpu-stats.c -o cpu-stats && mpiexec -n {} ./cpu-stats".format(num_of_processes)
        cmdprocess = run_command_nonblocking(cmd, shell=True)
        stdout, _, _ = get_process_output(cmdprocess) 
        cpu_avg = float(stdout)  # Average CPU load across nodes
        print ("Average CPU load across nodes: {}".format(cpu_avg))

        difference = int(abs(cpu_avg - previous_cpu_avg)) #detect changepoint
        if (difference > MIN_THRESHOLD) or previous_cpu_avg == 0:
            #cpu_avg = 90  FOR TESTING TO FORCE STAGE CHANGE
            if (cpu_avg < MIN_THRESHOLD):
                # kill previous running process and start new process as part of autoscaling
                if pid and is_process_running(pid):
                    kill_process(process) # kill process

                num_of_processes = 8
                command = "mpicc -g sample.c -o sample && mpiexec -n {} ./sample".format(num_of_processes)
                process = run_command_nonblocking(command, shell=True)
                pid = process.pid
                run_process(process)
            
            elif (cpu_avg >= MIN_THRESHOLD and cpu_avg < MAX_THRESHOLD):
                # kill previous running process and start new process as part of autoscaling
                if pid and is_process_running(pid):
                    kill_process(process) # kill process

                num_of_processes = 16
                command = "mpicc -g sample.c -o sample && mpiexec -n {} ./sample".format(num_of_processes)
                process = run_command_nonblocking(command, shell=True)
                pid = process.pid
                run_process(process)
            elif cpu_avg > MAX_THRESHOLD:
                # kill previous running process and start new process as part of autoscaling
                if pid and is_process_running(pid):
                    kill_process(process) # kill process

                num_of_processes = 32
                command = "mpicc -g sample.c -o sample && mpiexec -n {} ./sample".format(num_of_processes)
                process = run_command_nonblocking(command, shell=True)
                pid = process.pid
                run_process(process)

        time.sleep(PULSE_TIME)
        previous_cpu_avg = cpu_avg
        #previous_cpu_avg = 100  FOR TESTING TO FORCE STAGE CHANGE


#############################################################################

def test2():
    command = "sleep 50 && echo 'Command finished 50'"
    process = run_command_nonblocking(command, shell=True)
    pid = process.pid

    # Do other work here...
    time.sleep(30)
    if is_process_running(pid):
        kill_process(process) # kill process

    command = "sleep 60 && echo 'Command finished 60'"
    process = run_command_nonblocking(command, shell=True)
    pid = process.pid
    run_process(process)
    time.sleep(30)
    if is_process_running(pid):
        kill_process(process) # kill process

    cmd = "mpicc -g cpu-stats.c -o cpu-stats && mpiexec -n {} ./cpu-stats".format(4)
    cmdprocess = run_command_nonblocking(cmd, shell=True)
    stdout, _, _ = get_process_output(cmdprocess) 

    cpu_avg = float(stdout)
    print("avg_cpu_load_across_processes: {}".format(cpu_avg))

#############################################################################

if __name__ == "__main__":
    #test2()
    autoscale_processes()

