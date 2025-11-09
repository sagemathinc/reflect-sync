set -ev

# Temporarily pay attention to it
git update-index --no-skip-worktree ../todo.tasks

# stage & commit it explicitly
git add ../todo.tasks
git commit -m "Update todo.tasks"

# Re-hide future local edits
git update-index --skip-worktree ../todo.tasks
