#!/bin/bash
# setup_music_env.sh

echo "Updating package list and installing Python3 and pip if needed..."
sudo apt update
sudo apt install -y python3 python3-pip

echo "Installing required Python packages..."
pip3 install --upgrade pip
pip3 install mutagen musicbrainzngs requests

echo "All done! You can now run your Python scripts."
