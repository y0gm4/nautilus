import importlib.resources
import os
import subprocess
import sys


def main() -> None:
    binary_name = "nautilus.exe" if sys.platform == "win32" else "nautilus"
    binary = importlib.resources.files("nautilus") / binary_name
    env = dict(os.environ)
    env["NAUTILUS_PYTHON_WRAPPER"] = "1"
    argv = [str(binary), *sys.argv[1:]]
    if sys.platform == "win32":
        raise SystemExit(subprocess.run(argv, env=env).returncode)
    os.execve(str(binary), argv, env)


if __name__ == "__main__":
    main()
