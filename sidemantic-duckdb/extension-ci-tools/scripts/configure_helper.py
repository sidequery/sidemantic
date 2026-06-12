import subprocess
import argparse
from pathlib import Path
import os

def main():
    arg_parser = argparse.ArgumentParser(description='Script to aid in running the configure step of the extension build process')


    arg_parser.add_argument('-o', '--output-directory', type=str, help='Specify the output directory', default='configure')

    arg_parser.add_argument('-ev', '--extension-version', help='Write the autodetected extension version', action='store_true')
    arg_parser.add_argument('-p', '--duckdb-platform', help='Write the auto-detected duckdb platform', action='store_true')

    args = arg_parser.parse_args()

    OUTPUT_DIR = args.output_directory

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # Write version
    if args.extension_version:
        git_tag = subprocess.getoutput("git tag --points-at HEAD")
        if git_tag:
            EXTENSION_VERSION = git_tag
        else:
            EXTENSION_VERSION = subprocess.getoutput("git --no-pager log -1 --format=%h")

        version_file = Path(os.path.join(OUTPUT_DIR, "extension_version.txt"))
        with open(version_file, 'w') as f:
            print(f"Writing version {EXTENSION_VERSION} to {version_file}")
            f.write(EXTENSION_VERSION)

    # Write duck
    if args.duckdb_platform:
        import duckdb
        platform_file = Path(os.path.join(OUTPUT_DIR, "platform.txt"))
        duckdb_platform = duckdb.execute('pragma platform').fetchone()[0]
        with open(platform_file, 'w') as f:
            print(f"Writing platform {duckdb_platform} to {platform_file}")
            f.write(duckdb_platform)

if __name__ == '__main__':
    main()