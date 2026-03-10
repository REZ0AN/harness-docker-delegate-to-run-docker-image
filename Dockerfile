FROM python:3.10.17-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies including mailx
RUN apt-get update && apt-get install -y \
    mailutils \
    msmtp \
    msmtp-mta \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* 



# Copy the current directory contents into the container at /app
COPY get_data.py /app/
COPY monthly_audit.py /app/
COPY audit_gen.sh /app/
COPY requirements.txt /app/
COPY .env /app/
COPY .email_recipients.txt /app/
COPY send_email.sh /app/
COPY msmtprc /etc/msmtprc 

# Testing files
COPY repos.txt /app/
COPY team_users.txt /app/

RUN chmod 600 /etc/msmtprc
# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt 

# Make the shell scripts executable
RUN chmod +x /app/audit_gen.sh
RUN chmod +x /app/send_email.sh

# Create data and output directories
RUN mkdir -p /app/audits /app/logs/error

# Set the entrypoint to the bash script
ENTRYPOINT ["/app/audit_gen.sh"]
