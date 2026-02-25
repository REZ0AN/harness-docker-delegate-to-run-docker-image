#!/bin/bash

# send_email.sh - Send audit reports via email individually

set -e

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[EMAIL]${NC} $1"
}

log_error() {
    echo -e "${RED}[EMAIL ERROR]${NC} $1" >&2
}

log_warning() {
    echo -e "${YELLOW}[EMAIL WARNING]${NC} $1"
}

RECIPIENTS_FILE="${EMAIL_RECIPIENTS_FILE:-./email_recipients.txt}"
AUDIT_DIR="${AUDIT_DIR:-./audits}"
SUBJECT_PREFIX="${EMAIL_SUBJECT:-GitHub Audit Report}"
    
log_info "Starting email distribution..."

# Find all CSV files
AUDIT_FILES=$(find "$AUDIT_DIR" -name "*.csv" -type f)

if [ -z "$AUDIT_FILES" ]; then
    log_warning "No CSV files found in $AUDIT_DIR"
    exit 1
fi

FILE_COUNT=$(echo "$AUDIT_FILES" | wc -l)
log_info "Found $FILE_COUNT audit file(s)"

# Get period info
SUBJECT="$SUBJECT_PREFIX"
    
# Create email body file
BODY_FILE=$(mktemp)
cat > "$BODY_FILE" << EOF
GitHub Commit Audit Report

Generated: $(date '+%Y-%m-%d %H:%M:%S')
Total Reports: $FILE_COUNT

This email contains all commits audit reports for the specified period.

Best regards,
Automated Audit System
EOF

# Send to each recipient
SUCCESS_COUNT=0
FAIL_COUNT=0

while IFS= read -r recipient; do
    # Skip empty lines and comments
    [[ -z "$recipient" || "$recipient" =~ ^[[:space:]]*# ]] && continue
    
    # Trim whitespace
    recipient=$(echo "$recipient" | xargs)
    [ -z "$recipient" ] && continue
    
    log_info "Sending to: $recipient"
    
    # Build attachment arguments
    ATTACH_ARGS=""
    for file in $AUDIT_FILES; do
        ATTACH_ARGS="$ATTACH_ARGS -A \"$file\""
    done
    
    # Send email with all attachments
    set +e  # Temporarily disable exit on error
    eval "mail -s \"$SUBJECT\" $ATTACH_ARGS -- \"$recipient\" < \"$BODY_FILE\""
    if [ $? -eq 0 ]; then
        log_info "✓ Sent to $recipient"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        log_error "✗ Failed to send to $recipient"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
    set -e  # Re-enable exit on error
    
done < "$RECIPIENTS_FILE"

# Clean up body file after all emails are sent
rm -f "$BODY_FILE"

log_info "Successful: $SUCCESS_COUNT | Failed: $FAIL_COUNT"

if [ $FAIL_COUNT -eq 0 ]; then
    log_info "All emails sent successfully."
    exit 0
else
    log_warning "$FAIL_COUNT email(s) failed to send."
    exit 1
fi