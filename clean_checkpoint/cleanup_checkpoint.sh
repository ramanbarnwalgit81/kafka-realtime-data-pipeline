# Navigate to checkpoint directory
cd /Users/jay/PycharmProjects/AvriocProject/storage/checkpoint

# For item_interactions_checkpoint - delete old commits (keep last 10)
find item_interactions_checkpoint/commits -type f ! -name ".*" | sort -V | head -n -10 | xargs rm -f
find item_interactions_checkpoint/commits -name ".*.crc" | xargs rm -f 2>/dev/null || true

# For user_interactions_checkpoint - delete old commits (keep last 10)
find user_interactions_checkpoint/commits -type f ! -name ".*" | sort -V | head -n -10 | xargs rm -f
find user_interactions_checkpoint/commits -name ".*.crc" | xargs rm -f 2>/dev/null || true

# Similar for offsets...
