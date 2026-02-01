@echo off
echo Removing sensitive files from git history...

REM Remove files with credentials from git history
git filter-branch --force --index-filter "git rm --cached --ignore-unmatch test-direct-email.js" --prune-empty --tag-name-filter cat -- --all
git filter-branch --force --index-filter "git rm --cached --ignore-unmatch setup-domains-email.js" --prune-empty --tag-name-filter cat -- --all
git filter-branch --force --index-filter "git rm --cached --ignore-unmatch .env" --prune-empty --tag-name-filter cat -- --all

REM Force push to remote
git push origin --force --all
git push origin --force --tags

echo Done! Sensitive files removed from git history.