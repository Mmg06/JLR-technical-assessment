# official Python runtime as a parent image
FROM python:3.9-slim

# Setting the working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y git

# Copy the current directory contents into the container
COPY . /app

# Installing packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 80

# Run main.py when the container launches
CMD ["python", "main.py"]
