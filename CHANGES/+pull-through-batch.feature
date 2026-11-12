Added PULL_THROUGH_BATCH_WINDOW: content fetched through a remote is added to its repository in one
version per window instead of one task and version per file, with the pending content kept in Redis.
