#!/usr/bin/env python3

import os
import time
import configparser
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import mysql.connector
from mysql.connector import Error

class SQLFileHandler(FileSystemEventHandler):
    def __init__(self, db_config, watch_dir):
        self.db_config = db_config
        self.watch_dir = watch_dir

    def on_created(self, event):
        if event.is_directory:
            return
        filepath = event.src_path
        if filepath.endswith('.sql'):
            self.process_sql_file(filepath)

    def process_sql_file(self, filepath):
        base_name = os.path.basename(filepath)
        log_file = os.path.splitext(filepath)[0] + '.log'
        try:
            print(f"Processing SQL file: {filepath}")
            # Import SQL file using mysql client command line for better compatibility
            cmd = [
                'mysql',
                f'-h{self.db_config["host"]}',
                f'-P{self.db_config["port"]}',
                f'-u{self.db_config["user"]}',
                f'-p{self.db_config["password"]}',
                self.db_config["database"]
            ]
            with open(filepath, 'r') as sql_file:
                result = subprocess.run(cmd, stdin=sql_file, capture_output=True, text=True)
            if result.returncode == 0:
                log_content = f"SUCCESS: Imported {base_name} successfully.\n"
                print(log_content.strip())
            else:
                log_content = f"ERROR: Failed to import {base_name}.\n"
                log_content += f"stderr:\n{result.stderr}\n"
                print(log_content.strip())
            # Write log file
            with open(log_file, 'w') as lf:
                lf.write(log_content)
            # Delete the original SQL file
            os.remove(filepath)
        except Exception as e:
            error_msg = f"Exception processing {base_name}: {str(e)}"
            print(error_msg)
            with open(log_file, 'w') as lf:
                lf.write(error_msg)

def read_db_config():
    config_path = os.path.expanduser('~/.tacosroy/tacosroy.conf')
    config = configparser.ConfigParser()
    config.read(config_path)
    prefijo = config.get('DB', 'Prefijo', fallback=None)
    if prefijo is None:
        raise ValueError("Prefijo not found in config file")
    # Strip surrounding quotes if present
    prefijo = prefijo.strip('\'"')
    database_name = f"db_tacosroy_{prefijo}"
    db_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 't4a2x0a6',
        'database': database_name
    }
    return db_config

def main():
    db_config = read_db_config()
    prefijo = db_config['database'].replace('db_tacosroy_', '')
    base_watch_dir = os.path.expanduser('~/Dropbox/2020/TRdumps/importar')
    watch_dir = os.path.join(base_watch_dir, prefijo)
    if not os.path.exists(watch_dir):
        print(f"Watch directory {watch_dir} does not exist. Creating it.")
        os.makedirs(watch_dir)
    event_handler = SQLFileHandler(db_config, watch_dir)
    observer = Observer()
    observer.schedule(event_handler, watch_dir, recursive=False)
    observer.start()
    print(f"Monitoring directory: {watch_dir} for new SQL files...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
