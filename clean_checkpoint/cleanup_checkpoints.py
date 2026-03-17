#!/usr/bin/env python3
"""
Safely clean up old Spark Structured Streaming checkpoint files.
Keeps the last N commits/offsets and recent state files for recovery.
"""

import os
import shutil
from pathlib import Path

CHECKPOINT_BASE = "/Users/jay/PycharmProjects/AvriocProject/storage/checkpoint"
KEEP_COMMITS = 10  # Keep last 10 commits
KEEP_OFFSETS = 10  # Keep last 10 offsets
KEEP_STATE_BATCHES = 20  # Keep last 20 state batches


def get_latest_number(files):
    """Extract numeric values from filenames and return the max."""
    numbers = []
    for f in files:
        try:
            num = int(f)
            numbers.append(num)
        except ValueError:
            continue
    return max(numbers) if numbers else 0


def cleanup_checkpoint(checkpoint_dir, checkpoint_name):
    """Clean up old files in a checkpoint directory."""
    checkpoint_path = Path(checkpoint_dir)

    if not checkpoint_path.exists():
        print(f"Checkpoint {checkpoint_name} does not exist: {checkpoint_path}")
        return

    print(f"\n=== Cleaning {checkpoint_name} ===")

    # Clean commits
    commits_dir = checkpoint_path / "commits"
    if commits_dir.exists():
        commit_files = [f.name for f in commits_dir.iterdir() if f.is_file() and not f.name.startswith('.')]
        if commit_files:
            latest_commit = get_latest_number(commit_files)
            cutoff = latest_commit - KEEP_COMMITS

            deleted_commits = 0
            for commit_file in commit_files:
                try:
                    commit_num = int(commit_file)
                    if commit_num <= cutoff:
                        file_path = commits_dir / commit_file
                        crc_path = commits_dir / f".{commit_file}.crc"
                        file_path.unlink()
                        if crc_path.exists():
                            crc_path.unlink()
                        deleted_commits += 2  # file + crc
                except ValueError:
                    continue

            print(f"  Deleted {deleted_commits} old commit files (kept commits {cutoff + 1}-{latest_commit})")

    # Clean offsets
    offsets_dir = checkpoint_path / "offsets"
    if offsets_dir.exists():
        offset_files = [f.name for f in offsets_dir.iterdir() if f.is_file() and not f.name.startswith('.')]
        if offset_files:
            latest_offset = get_latest_number(offset_files)
            cutoff = latest_offset - KEEP_OFFSETS

            deleted_offsets = 0
            for offset_file in offset_files:
                try:
                    offset_num = int(offset_file)
                    if offset_num <= cutoff:
                        file_path = offsets_dir / offset_file
                        crc_path = offsets_dir / f".{offset_file}.crc"
                        file_path.unlink()
                        if crc_path.exists():
                            crc_path.unlink()
                        deleted_offsets += 2  # file + crc
                except ValueError:
                    continue

            print(f"  Deleted {deleted_offsets} old offset files (kept offsets {cutoff + 1}-{latest_offset})")

    # Clean state files
    state_dir = checkpoint_path / "state" / "0"
    if state_dir.exists():
        batch_dirs = [f.name for f in state_dir.iterdir() if f.is_dir() and f.name != "_metadata"]
        if batch_dirs:
            batch_numbers = []
            for bd in batch_dirs:
                try:
                    batch_numbers.append(int(bd))
                except ValueError:
                    continue

            if batch_numbers:
                latest_batch = max(batch_numbers)
                cutoff = latest_batch - KEEP_STATE_BATCHES

                deleted_batches = 0
                deleted_files = 0
                for batch_num in batch_numbers:
                    if batch_num <= cutoff:
                        batch_path = state_dir / str(batch_num)
                        if batch_path.exists():
                            # Count files before deletion
                            file_count = sum(1 for _ in batch_path.rglob('*') if _.is_file())
                            shutil.rmtree(batch_path)
                            deleted_batches += 1
                            deleted_files += file_count

                print(f"  Deleted {deleted_batches} old state batch directories ({deleted_files} files)")
                print(f"  Kept state batches {cutoff + 1}-{latest_batch}")


def main():
    print("Spark Checkpoint Cleanup Script")
    print("=" * 50)
    print(f"Keeping last {KEEP_COMMITS} commits")
    print(f"Keeping last {KEEP_OFFSETS} offsets")
    print(f"Keeping last {KEEP_STATE_BATCHES} state batches")
    print("=" * 50)

    # Clean both checkpoints
    item_checkpoint = Path(CHECKPOINT_BASE) / "item_interactions_checkpoint"
    user_checkpoint = Path(CHECKPOINT_BASE) / "user_interactions_checkpoint"

    cleanup_checkpoint(item_checkpoint, "item_interactions_checkpoint")
    cleanup_checkpoint(user_checkpoint, "user_interactions_checkpoint")

    print("\n=== Cleanup Complete ===")
    print("\n⚠️  IMPORTANT: Make sure your Spark streaming job is STOPPED before running this script!")
    print("   Restart the job after cleanup to ensure it uses the remaining checkpoint files.")


if __name__ == "__main__":
    # Safety check - ask for confirmation
    response = input("\n⚠️  WARNING: This will delete old checkpoint files. "
                     "Make sure Spark jobs are STOPPED. Continue? (yes/no): ")
    if response.lower() == 'yes':
        main()
    else:
        print("Cleanup cancelled.")
