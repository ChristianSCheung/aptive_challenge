# Base image
FROM python:3.11-slim

# Environment settings
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system dependencies needed for pip packages
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    git \
    libffi-dev \
    libpq-dev \
    libssl-dev \
    unixodbc-dev \
    libxml2-dev \
    libxmlsec1-dev \
    pkg-config \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip (optional but good practice)
RUN pip install --upgrade pip

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files into the image
COPY . .

# Expose port 8080 (Cloud Run expects this)
EXPOSE 8080

# Set the default command to run the app on port 8080
CMD ["python", "-m", "pipeline.app"]
