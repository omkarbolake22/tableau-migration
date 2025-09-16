FROM python:3.11-slim

WORKDIR /app

# Copy everything (you can adjust later)
COPY . /app

# Install Python deps first (better layer caching)
COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --upgrade pip \
 && python -m pip install -r /tmp/requirements.txt

# No requirements, skip pip install

# Default command does nothing but keeps container alive
CMD ["python", "-c", "print('Container ran successfully!')"]
