#!/usr/bin/env python3
"""
Fix the double-nested EIA directory issue by moving files to the correct location.
"""
import shutil
from pathlib import Path

def fix_nested_eia_files():
    """Move files from data/raw/eia/eia/ to data/raw/eia/"""

    nested_dir = Path("data/raw/eia/eia")
    correct_dir = Path("data/raw/eia")

    print(f"🔍 Checking for nested files in: {nested_dir}")

    if not nested_dir.exists():
        print("✅ No nested directory found - nothing to fix")
        return

    # Find all files in the nested structure
    nested_files = list(nested_dir.rglob("*.json"))
    print(f"📁 Found {len(nested_files)} JSON files in nested directory")

    if not nested_files:
        print("✅ No JSON files in nested directory - removing empty dir")
        shutil.rmtree(nested_dir)
        return

    # Create the correct directory structure
    correct_dir.mkdir(parents=True, exist_ok=True)

    moved_count = 0
    for file_path in nested_files:
        # Calculate the relative path from the nested eia directory
        relative_path = file_path.relative_to(nested_dir)
        destination = correct_dir / relative_path

        # Create destination directory if needed
        destination.parent.mkdir(parents=True, exist_ok=True)

        # Move the file
        print(f"📦 Moving: {file_path} -> {destination}")
        shutil.move(str(file_path), str(destination))
        moved_count += 1

    print(f"✅ Moved {moved_count} files from nested to correct location")

    # Remove the empty nested directory
    try:
        shutil.rmtree(nested_dir)
        print(f"🗑️  Removed empty nested directory: {nested_dir}")
    except OSError as e:
        print(f"⚠️  Could not remove nested directory: {e}")

    # Verify the fix
    correct_files = list(correct_dir.rglob("*.json"))
    print(f"✅ Verification: {len(correct_files)} JSON files now in correct location")

if __name__ == "__main__":
    fix_nested_eia_files()
