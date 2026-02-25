#!/bin/bash

## Setup Mail Transfer Agent (msmtp) Configuration
MSMTP_CONFIG="/etc/msmtprc"

echo "[INFO] Configuring MSMTP..."


# Check required variables
if [ -z "$SMTP_USER" ] || [ -z "$SMTP_PASSWORD" ]; then
    echo "[ERROR] SMTP_USER and SMTP_PASSWORD must be set in .env file"
    exit 1
fi


# Set defaults
SMTP_SOURCE_NAME="${SMTP_SOURCE_NAME:-gmail}"
SMTP_HOST="${SMTP_HOST:-smtp.gmail.com}"
SMTP_PORT="${SMTP_PORT:-587}"
SMTP_FROM="${SMTP_FROM:-$SMTP_USER}"
SMTP_USER="${SMTP_USER}"
SMTP_PASSWORD="${SMTP_PASSWORD//\'/}"

# Replace placeholders in msmtprc
sed -i "s/SOURCE_NAME/${SMTP_SOURCE_NAME:-default}/g" $MSMTP_CONFIG
sed -i "s/MAIL_HOST/${SMTP_HOST}/g" $MSMTP_CONFIG
sed -i "s/MAIL_PORT/${SMTP_PORT}/g" $MSMTP_CONFIG
sed -i "s/MAIL_FROM/${SMTP_FROM}/g" $MSMTP_CONFIG
sed -i "s/MAIL_USER/${SMTP_USER}/g" $MSMTP_CONFIG
sed -i "s/MAIL_PASSWORD/${SMTP_PASSWORD}/g" $MSMTP_CONFIG



## use environment variable ORG_NAME to set the organization name
# If ORG_NAME is not set, it will default to 'your-org-name'
ORG_NAME="${ORG_NAME:-your-org-name}"

## use environment variable TEAM_ID to set the team id
# If TEAM_ID is not set, it will default to 'your-team-id'
TEAM_ID="${TEAM_ID:-your-team-id}"

## use environment variable APPLICATION_NAME to set the application name
# If APPLICATION_NAME is not set, it will default to 'your-application-name'
APPLICATION_NAME="${APPLICATION_NAME:-your-application-name}"

# ## Getting the Data We Need To Process Further
# for testing we won't run this
# python3 get_data.py "$ORG_NAME" "$TEAM_ID" "$APPLICATION_NAME"
# ## Now two files will be generated repos.txt and team_users.txt

# Check if get_data.py succeeded
if [ $? -ne 0 ]; then
    echo "Error: get_data.py failed to execute successfully"
    exit 1
fi

## Check if repos.txt exists and has content
if [ ! -f "repos.txt" ]; then
    echo "Error: repos.txt file not found. get-data.py may have failed to generate it."
    exit 1
fi

if [ ! -s "repos.txt" ]; then
    echo "Error: repos.txt exists but is empty. No repository data available."
    exit 1
fi

echo "✓ repos.txt found and contains data"

## Check if team_users.txt exists and has content
if [ ! -f "team_users.txt" ]; then
    echo "Error: team_users.txt file not found. get-data.py may have failed to generate it."
    exit 1
fi

if [ ! -s "team_users.txt" ]; then
    echo "Error: team_users.txt exists but is empty. No team user data available."
    exit 1
fi

echo "✓ team_users.txt found and contains data"

## Use environment variable with detfault values to set iIS_PERIOD and MONTH_START, MONTH_END, PERIOD

# Set Period if you want to set a period for the audit instead of month range
# 0 for month range and 1 for period

IS_PERIOD="${IS_PERIOD:-0}"

# Set the month range for the audit
# month has to be in this format  YYYY-MM
# for example 2025-01

# The months for which the audit is being generated
# You can change the month as per your requirement
# Got the values from the environment variable or set default values
# If MONTH_START is not set, it will default to '2025-01'
# If MONTH_END is not set, it will default to '2025-02'
# If you set IS_PERIOD to 1 then MONTH_END will be ignored
MONTH_START="${MONTH_START:-2025-01}"
MONTH_END="${MONTH_END:-2025-02}"


# if you set is_period to 1 then you have to set the period
## Got the value from the environment variable or set default value
# If PERIOD is not set, it will default to '3' (3 months)
# This means the audit will be generated for the last 3 months from the start month
PERIOD="${PERIOD:-3}"

## Auditing Based on Team name and User Under the team and also in the filtered list of 
## repositories by Custom_Properties Audit Field

declare -A team_map

# Read file and group users by team
while IFS='=' read -r TEAM_NAME USERNAMES; do
    # Trim whitespace
    TEAM_NAME=$(echo "$TEAM_NAME" | xargs)
    USERNAMES=$(echo "$USERNAMES" | xargs)
    
    # Append users to existing team entry
    if [[ -n "${team_map[$TEAM_NAME]}" ]]; then
        team_map[$TEAM_NAME]="${team_map[$TEAM_NAME]} $USERNAMES"
    else
        team_map[$TEAM_NAME]="$USERNAMES"
    fi
done < ./team_users.txt

echo "✓ Team and user mapping created successfully"
echo "Teams found: ${!team_map[@]}"
# Now call Python script for each team with all their users
for TEAM_NAME in "${!team_map[@]}"; do

    USERNAMES=${team_map[$TEAM_NAME]}
    python3 monthly_audit.py "$USERNAMES" "$MONTH_START" "$MONTH_END" "$TEAM_NAME" "$IS_PERIOD" "$PERIOD" "$APPLICATION_NAME"

    # Check if monthly-audit.py succeeded for this team
    if [ $? -ne 0 ]; then
        echo "Warning: monthly-audit.py failed for team: $TEAM_NAME"
        # Continue with other teams instead of exiting
    else
        echo "✓ Successfully processed team: $TEAM_NAME"
    fi

done

# After processing all teams, check if email should be sent
SEND_EMAIL="${SEND_EMAIL:-false}"


if [ "$SEND_EMAIL" = "true" ]; then

    # Test MSMTP configuration
    echo "[INFO] Testing MSMTP configuration..."
    if echo -e "Subject: MSMTP Test\n\nThis is a test message from MSMTP" | \
       msmtp --from=$SMTP_FROM $SMTP_FROM >/dev/null 2>&1; then
        echo "[SUCCESS] Test email sent successfully"
    else
        echo "[ERROR] Failed to send test email"
        exit 1
    fi
    
    echo ""
    echo "=================================================="
    echo "SENDING EMAIL REPORTS"
    echo "=================================================="
    
    # Source the email script
    if [ -f /app/send_email.sh ]; then
        source /app/send_email.sh
        
        # Send emails
        main
        
        if [ $? -eq 0 ]; then
            echo "[SUCCESS] Email reports sent successfully"
        else
            echo "[WARNING] Failed to send some or all email reports"
        fi
    else
        echo "[ERROR] send_email.sh not found"
    fi
fi

if [ $? -eq 0 ]; then
    echo "[SUCCESS] Audit process completed successfully"
    exit 0
else
    echo "[ERROR] Audit process encountered errors"
    exit 1
fi
