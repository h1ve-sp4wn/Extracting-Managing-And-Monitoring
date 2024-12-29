CREATE INDEX idx_repo_name ON github_events (repo_name);
CREATE INDEX idx_created_at ON github_events (created_at);