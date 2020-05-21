# this script can be used to validate the config and override yamls

import yaml

yml_file = "config.yml"
try:
    with open(yml_file, "r") as f:
        yml_config = yaml.safe_load(f)
except FileNotFoundError as fnfe:
    msg = f"Unable to find config file: {yml_file} - {fnfe}"
    print(msg)
    raise
except yaml.scanner.ScannerError as se:
    msg = f"Error parsing config.yml: {se}"
    print(msg)
    raise KeyError(msg)

override_file = "override.yml"
try:
    with open(override_file, "r") as f:
        override_config = yaml.safe_load(f)
except FileNotFoundError as fnfe:
    msg = f"Unable to find override file: {override_file} - {fnfe}"
    print(msg)
    raise
except yaml.scanner.ScannerError as se:
    msg = f"Error parsing config.yml: {se}"
    print(msg)
    raise KeyError(msg)

for k in yml_config:
    v = yml_config[k]
    if k in override_config:
        o = override_config[k]
        print(f"{k}: {o} (OVERRIDE of {v})")
    else:
        print(f"{k}: {v}")
