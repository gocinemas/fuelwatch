#!/usr/bin/env python3
"""
Auto-update brand count references across all templates and config files.
Called as final step after adding new brands to the database.

Usage:
    python3 update_brand_counts.py
    (or) railway run python3 update_brand_counts.py
"""

import os
import re
from pathlib import Path


def count_brands_in_db() -> int:
    """Get actual brand count from Supabase."""
    try:
        import library as lib
        sb = lib._sb()
        result = sb.table("brand_phase1_intelligence").select("brand_name", count="exact").execute()
        # Count unique brands (each brand appears 3 times for 3 markets)
        brands = result.data if result.data else []
        unique_brands = len(set(b["brand_name"] for b in brands))
        print(f"✅ Found {unique_brands} unique brands in database")
        return unique_brands
    except Exception as e:
        print(f"❌ Failed to count brands: {e}")
        return None


def update_template_files(new_count: int) -> dict:
    """Update brand count in all template files."""
    templates_dir = Path("/Users/srevi/fuelwatch/templates")
    files_updated = {}

    patterns = [
        (r"60\+\s*brands", f"{new_count} brands"),
        (r"60\+\s*consumer brands", f"{new_count} consumer brands"),
        (r"Phase 1.*?60\+", f"Phase 1 (LIVE): {new_count}"),
        (r"60 Brands", f"{new_count} Brands"),
        (r"with 60", f"with {new_count}"),
        (r"60\+\s*real consumer", f"{new_count} real consumer"),
    ]

    for template_file in templates_dir.glob("*.html"):
        try:
            content = template_file.read_text()
            original_content = content

            for pattern, replacement in patterns:
                content = re.sub(pattern, replacement, content, flags=re.IGNORECASE)

            if content != original_content:
                template_file.write_text(content)
                # Count how many replacements
                diff_count = len(re.findall(r"60\+", original_content)) + len(re.findall(r"\b60\b", original_content))
                files_updated[template_file.name] = diff_count
                print(f"✅ Updated {template_file.name}")
        except Exception as e:
            print(f"⚠️  Could not update {template_file.name}: {e}")

    return files_updated


def update_python_files(new_count: int) -> dict:
    """Update brand count in Python docstrings and comments."""
    root_dir = Path("/Users/srevi/fuelwatch")
    files_updated = {}

    for py_file in root_dir.glob("*.py"):
        if py_file.name in ["update_brand_counts.py"]:
            continue

        try:
            content = py_file.read_text()
            original_content = content

            # Update docstring references
            content = re.sub(r"Research and populate 93 brands", f"Research and populate {new_count} brands", content)
            content = re.sub(r"Load all 93 brands", f"Load all {new_count} brands", content)
            content = re.sub(r"\(60 brands", f"({new_count} brands", content)
            content = re.sub(r"60 brands\)", f"{new_count} brands)", content)

            if content != original_content:
                py_file.write_text(content)
                files_updated[py_file.name] = 1
                print(f"✅ Updated {py_file.name}")
        except Exception as e:
            print(f"⚠️  Could not update {py_file.name}: {e}")

    return files_updated


def main():
    print("\n" + "="*70)
    print("BRAND COUNT AUTO-UPDATE AGENT")
    print("="*70 + "\n")

    # Step 1: Get actual count from DB
    brand_count = count_brands_in_db()
    if brand_count is None:
        print("❌ Could not determine brand count. Aborting.")
        return False

    print(f"\n📊 Updating all references to: {brand_count} brands\n")

    # Step 2: Update templates
    print("Updating templates...")
    template_updates = update_template_files(brand_count)
    print(f"  → {len(template_updates)} template(s) updated\n")

    # Step 3: Update Python files
    print("Updating Python files...")
    py_updates = update_python_files(brand_count)
    print(f"  → {len(py_updates)} Python file(s) updated\n")

    # Step 4: Summary
    total_updates = len(template_updates) + len(py_updates)
    print("="*70)
    print(f"✅ COMPLETE: Updated {total_updates} files with {brand_count} brands")
    print("="*70)
    print("\n📋 Files updated:")
    for fname, count in sorted(template_updates.items()):
        print(f"  • {fname}")
    for fname in sorted(py_updates.keys()):
        print(f"  • {fname}")
    print()

    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
