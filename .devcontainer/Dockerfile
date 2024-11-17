# Start with a Python 3.9 base image
FROM python:3.9

# Set the working directory in the container
WORKDIR /workspace

# Install Java (required by PySpark)
RUN apt-get update && \
    apt-get install -y default-jdk && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable (automatically detects the path)
RUN export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "JAVA_HOME is set to $JAVA_HOME" >> /etc/environment

# Load JAVA_HOME in the current session
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:${PATH}"

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install additional Python tools
RUN pip install --no-cache-dir black flake8 pytest

# Copy the rest of the application code to the working directory
COPY . /workspace

# Set the default command to bash
CMD ["bash"]
