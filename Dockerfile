# Use official lightweight Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy app files
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Set environment variables at runtime
ENV FLASK_APP=app.py
ENV FLASK_RUN_PORT=3978
ENV PYTHONUNBUFFERED=1

# Expose port for Bot Framework
EXPOSE 3978

# Run the app
CMD ["python", "app.py"]
