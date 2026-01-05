# Complete Branch Comparison: publishing2 vs main

Generated: $(date)

## Summary Statistics

$(git diff main --shortstat)

## Files Changed

### Modified Files
$(git diff main --name-status | grep "^M" | awk '{print $2}' | sort)

### Added Files
$(git diff main --name-status | grep "^A" | awk '{print $2}' | sort)

### Deleted Files
$(git diff main --name-status | grep "^D" | awk '{print $2}' | sort)

## Detailed Changes

$(git diff main --stat | head -100)

