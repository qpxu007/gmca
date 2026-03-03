import os
import sys
import json
import logging

# Set up a logger for configuration
logger = logging.getLogger("qp2.config.programs")

class ProgramConfig:
    """
    Centralized configuration for external program environments.
    Ensures consistent setup by explicitly defining PX and MODULEPATH.
    
    Priority:
    1. Environment Variable Override (QP2_SETUP_<PROGRAM>)
    2. JSON Configuration File (qp2/config/programs.json or QP2_PROGRAMS_CONFIG)
    3. Default Standard Command (module load <program>)
    """
    
    # Standard paths - configurable via env vars, but defaults provided
    # Note: These defaults match the requested 'crystfel' pattern
    PX_ROOT = os.environ.get("QP2_PX_ROOT", "/mnt/software/px/")
    MODULE_PATH = os.environ.get("QP2_MODULE_PATH", "/mnt/software/px/modulefiles")
    PROFILE_SCRIPT = os.environ.get("QP2_PROFILE_SCRIPT", "[ -f /usr/share/modules/init/bash ] && . /usr/share/modules/init/bash")
    
    # Cache for file configuration
    _file_config = None

    @classmethod
    def _load_file_config(cls):
        """Loads configuration from JSON file if not already loaded."""
        if cls._file_config is not None:
            return

        cls._file_config = {}
        
        # Determine config file path
        # 1. Env Var
        config_path = os.environ.get("QP2_PROGRAMS_CONFIG")
        
        # 2. Default location: sibling to this file (qp2/config/programs.json)
        if not config_path:
            if getattr(sys, 'frozen', False):
                # Handle PyInstaller frozen state
                if hasattr(sys, '_MEIPASS'):
                    base_dir = sys._MEIPASS
                else:
                    base_dir = os.path.dirname(sys.executable)
                config_path = os.path.join(base_dir, 'config', 'programs.json')
            else:
                current_dir = os.path.dirname(os.path.abspath(__file__))
                config_path = os.path.join(current_dir, "programs.json")
            
        if os.path.exists(config_path):
            try:
                with open(config_path, "r") as f:
                    data = json.load(f)
                    # Normalize keys to lower case for case-insensitive lookup
                    cls._file_config = {k.lower(): v for k, v in data.items()}
                logger.debug(f"Loaded program configuration from {config_path}")
            except Exception as e:
                logger.error(f"Failed to load program configuration from {config_path}: {e}")
        else:
            logger.debug(f"No program configuration file found at {config_path}")

    # Default library paths
    DEFAULT_LIBRARIES = {
        "xds-zcbf": "/mnt/software/px/XDS/xds-zcbf.so",
        "dectris-neggia": "/mnt/software/px/XDS/dectris-neggia.so",
        "raddose3d": "/mnt/software/px/bin/raddose3d.jar"
    }

    # Default executable paths
    DEFAULT_PROGRAMS = {
        "dozor": "/mnt/software/px/DOZOR/dozor2q",
        "dials_python": "/mnt/software/px/dials/build/bin/dials.python",
        "caget": "/mnt/software/epics/base/bin/linux-x86_64/caget",
        "python": "/mnt/software/px/miniconda3/envs/opencv/bin/python",
        # Added during config centralization
        "iv": "/mnt/software/scripts/iv",
        "adxv": "/mnt/software/px/bin/adxv",
        "eiger2cbf": "/mnt/software/px/bin/eiger2cbf-omp",
    }

    @classmethod
    def get_program_path(cls, program_name: str) -> str:
        """
        Returns the file path for a requested external program executable.

        Priority:
        1. Environment Variable: QP2_PROG_<PROGRAM_NAME_UPPER> (e.g. QP2_PROG_DOZOR)
        2. JSON Configuration File: "prog_<program_name>" (e.g. "prog_dozor")
        3. Hardcoded Defaults
        """
        prog_key = program_name.lower()
        env_var_name = f"QP2_PROG_{prog_key.replace('-', '_').upper()}"

        # 1. Check Env Var
        if os.environ.get(env_var_name):
            return os.environ[env_var_name]

        # 2. Check JSON Config
        cls._load_file_config()
        json_key = f"prog_{prog_key}"
        if json_key in cls._file_config:
            return cls._file_config[json_key]

        # 3. Default
        if prog_key == "iv":
             # Special handling for 'iv' to use the local script in qp2/bin
             # programs.py is in qp2/config/programs.py
             # bin is in qp2/bin
             current_dir = os.path.dirname(os.path.abspath(__file__))
             project_root = os.path.dirname(current_dir) # qp2
             iv_path = os.path.join(project_root, "bin", "iv")
             if os.path.exists(iv_path):
                 return iv_path

        return cls.DEFAULT_PROGRAMS.get(prog_key, "")

    @classmethod
    def get_library_path(cls, library_name: str) -> str:
        """
        Returns the file path for a requested external library.
        
        Priority:
        1. Environment Variable: QP2_LIB_<LIBRARY_NAME_UPPER> (e.g. QP2_LIB_DECTRIS_NEGGIA)
        2. JSON Configuration File: "lib_<library_name>" (e.g. "lib_dectris-neggia")
        3. Hardcoded Defaults
        """
        lib_key = library_name.lower()
        env_var_name = f"QP2_LIB_{lib_key.replace('-', '_').upper()}"
        
        # 1. Check Env Var
        if os.environ.get(env_var_name):
            return os.environ[env_var_name]
            
        # 2. Check JSON Config
        cls._load_file_config()
        json_key = f"lib_{lib_key}"
        if json_key in cls._file_config:
            return cls._file_config[json_key]
            
        # 3. Default
        return cls.DEFAULT_LIBRARIES.get(lib_key, "")

    @classmethod
    def get_setup_command(cls, program: str) -> str:
        """
        Returns the shell command to set up the environment for the given program.
        
        Priority:
        1. Environment variable: QP2_SETUP_<PROGRAM_NAME_UPPER> (Full Command)
        2. JSON Config file:
           - If entry contains " " or "$" or "(", it's treated as a Full Command.
           - Otherwise, it's treated as the 'module_name' to be used in the template.
        3. Built from template using robust MODULE initialization.
        """
        if not program or program.lower() == "none":
            return ":"

        program_key = program.lower()
        env_var_name = f"QP2_SETUP_{program_key.upper()}"
        
        # 1. Check Env Var Override (Full Command)
        if env_var_name in os.environ:
            val = os.environ[env_var_name]
            return val if val.strip() else ":"
            
        # 2. Check JSON Config File
        cls._load_file_config()
        
        target = program
        is_full_command = False
        
        if program_key in cls._file_config:
            val = cls._file_config[program_key]
            if not val or not val.strip():
                return ":"
                
            # If it looks like a full command, we'll return it as is (but still env-wrapped)
            if any(char in val for char in (" ", "$", "(", ")", "[", "]")):
                is_full_command = True
            
            target = val

        # 3. Build the Robust Command
        # We always prepend the PX and MODULEPATH, and ensure 'module' is defined via the profile script.
        env_setup = (
            f"export PX={cls.PX_ROOT}; "
            f"export MODULEPATH={cls.MODULE_PATH}:${{MODULEPATH:-}}; "
            f"{cls.PROFILE_SCRIPT}"
        )

        if is_full_command:
            return f"{env_setup}; {target}"
        else:
            return (
                f"{env_setup}; "
                f"module load {target} || {{ echo 'Error: Failed to load module {target}'; exit 1; }}"
            )
