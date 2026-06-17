#!/usr/bin/env python3
"""
Split sms_service.py into miru_service.py and intel_service.py
Preserves all imports and utilities; removes routes that don't belong.
"""

import re

# Read the original file
with open('sms_service.py', 'r') as f:
    content = f.read()
    lines = content.split('\n')

# Define Intel-only routes (routes to REMOVE from Miru)
intel_routes = {
    '/intel',
    '/<company_slug>',
    '/api/brand/debug',
    '/api/brand/basic',
    '/api/brand/spin',
    '/api/brand',
    '/api/brand/deep-research',
    '/api/brand/social',
    '/api/brand/save-to-library',
    '/api/brand/standing',
    '/api/brand/ask',
    '/api/intel/brand-choices',
    '/api/intel/pinpoint/collection',
    '/api/intel/research-docs',
    '/api/intel/pins',
    '/api/intel/pin',
    '/api/intel/unpin',
    '/api/intel/snapshot',
    '/api/intel/research',
    '/api/intel/deep-dive',
    '/api/intel/email-report',
    '/api/intel/compare',
    '/api/intel/compare-batch',
    '/api/intel/competitors',
    '/api/intel/verticals',
    '/api/intel/news',
    '/api/brands/search',
    '/api/admin/clear-brand-cache',
    '/api/brand/scan',
    '/api/company',
    '/api/company/metrics',
    '/api/company/results',
    '/api/company/chat',
    '/api/company/media',
}

# Define Miru-only routes (routes to REMOVE from Intel)
miru_routes = {
    '/',
    '/sms',
    '/saves-login',
    '/my-saves',
    '/home-v2',
    '/home-2026',
    '/commute-test',
    '/school',
    '/api/myarea',
    '/api/myarea/place-search',
    '/api/myarea/places',
    '/api/school',
    '/api/school/poll',
    '/api/search',
    '/api/library',
    '/api/commute',
    '/api/v2',
    '/api/home',
    '/api/saves',
}

def extract_routes(lines, routes_to_keep, output_file):
    """Extract routes to keep from sms_service.py"""
    output = []
    skip_mode = False
    skip_until_next_route = False
    in_function = False
    indent_level = 0

    i = 0
    while i < len(lines):
        line = lines[i]

        # Check for @app.route decorator
        if line.strip().startswith('@app.route('):
            # Extract route path
            match = re.search(r'@app\.route\(["\']([^"\']+)', line)
            if match:
                route = match.group(1)
                # Check if this route should be kept
                should_keep = any(route.startswith(keep) for keep in routes_to_keep)
                skip_mode = not should_keep

        # Skip this route and its function if not keeping
        if skip_mode and (line.strip().startswith('@app.route(') or line.strip().startswith('def ')):
            if line.strip().startswith('def '):
                # Skip until next @ or def at column 0
                while i < len(lines) and (i == 0 or lines[i][0] not in ('@', 'd') or (lines[i][0] == 'd' and not lines[i].startswith('def '))):
                    i += 1
                i -= 1  # Back up one since loop will increment
                skip_mode = False
        else:
            output.append(line)

        i += 1

    with open(output_file, 'w') as f:
        f.write('\n'.join(output))

    print(f"✓ Extracted {len(output)} lines to {output_file}")

# Extract routes
#extract_routes(lines, miru_routes, '/Users/srevi/miru-app/sms_service.py')
#extract_routes(lines, intel_routes, '/Users/srevi/intel-app/sms_service.py')

print("⚠️  Route extraction is complex due to massive file size.")
print("Recommend manual approach: keep full sms_service.py, environment variable to toggle which routes load.")
