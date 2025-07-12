import sys
import subprocess
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QPushButton, QVBoxLayout, QWidget,
    QFileDialog, QTextEdit, QLabel, QStatusBar
)
from PySide6.QtCore import QProcess, QTimer

class MusicManagerGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Music Library Manager")
        self.setMinimumSize(700, 500)

        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)

        # Output log panel
        self.output_log = QTextEdit()
        self.output_log.setReadOnly(True)

        # Buttons for actions
        self.btn_organize = QPushButton("Organize Music")
        self.btn_tag = QPushButton("Auto Tag Files")
        self.btn_fetch_art = QPushButton("Fetch Album Art")

        self.btn_organize.clicked.connect(self.run_organize_script)
        self.btn_tag.clicked.connect(self.run_tag_script)
        self.btn_fetch_art.clicked.connect(self.run_fetch_art_script)

        layout = QVBoxLayout()
        layout.addWidget(QLabel("Output Log:"))
        layout.addWidget(self.output_log)
        layout.addWidget(self.btn_organize)
        layout.addWidget(self.btn_tag)
        layout.addWidget(self.btn_fetch_art)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

        # Subprocess handler
        self.process = QProcess()
        self.process.readyReadStandardOutput.connect(self.read_stdout)
        self.process.readyReadStandardError.connect(self.read_stderr)
        self.process.finished.connect(self.on_process_finished)

    def run_script(self, script_path, *args):
        self.output_log.append(f"[▶] Running: {script_path} {' '.join(args)}\n")
        self.status_bar.showMessage("Running...")
        self.process.start(sys.executable, [script_path, *args])

    def run_organize_script(self):
        source_dir = QFileDialog.getExistingDirectory(self, "Select Source Directory")
        if not source_dir:
            return
        alias_json, _ = QFileDialog.getOpenFileName(self, "Select Alias JSON File", filter="JSON Files (*.json)")
        if not alias_json:
            return
        self.run_script("organize_music.py", source_dir, alias_json)

    def run_tag_script(self):
        music_root = QFileDialog.getExistingDirectory(self, "Select Music Directory")
        if not music_root:
            return
        self.run_script("log_missing_album_art.py", music_root, "missing_art.json")

    def run_fetch_art_script(self):
        json_path, _ = QFileDialog.getOpenFileName(self, "Select Missing Art JSON", filter="JSON Files (*.json)")
        if not json_path:
            return
        self.run_script("fetch_and_embed_album_art.py", json_path)

    def read_stdout(self):
        output = self.process.readAllStandardOutput().data().decode()
        self.output_log.append(output)

    def read_stderr(self):
        error = self.process.readAllStandardError().data().decode()
        self.output_log.append(f"[stderr] {error}")

    def on_process_finished(self):
        self.output_log.append("\n[✓] Script finished.\n")
        self.status_bar.showMessage("Ready", 3000)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    gui = MusicManagerGUI()
    gui.show()
    sys.exit(app.exec())
# This script provides a GUI for managing a music library, allowing users to organize files,
# auto-tag them, and fetch album art. It uses PySide6 for the GUI and QProcess to run external scripts.
# The GUI includes buttons for each action, a log output area, and a status bar to show the current state.
# Users can select directories and files through file dialogs, and the output log displays the results of each operation.
# The script is designed to be run in a Python environment with the necessary scripts (`organize_music.py`, `log_missing_album_art.py`, and `fetch_and_embed_album_art.py`) available in the same directory.
# It handles subprocess output and errors, updating the log in real-time as the scripts run.
# The GUI is responsive and provides feedback on the status of each operation, making it user-friendly for managing a music collection