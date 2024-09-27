# Use a base image that includes Python, such as Python 3.11
FROM python:3.12

# Set the working directory in the container
WORKDIR /app

# Copy the local files to the working directory in the container
COPY . .

# Install any Python dependencies (assuming you have a requirements.txt file)
RUN pip install --no-cache-dir -r requirements.txt

# Make sure main.py is executable
RUN chmod +x main.py

# Run the main.py script during the build
RUN python main.py

# Add a final message to signal the build is complete
RUN echo "process finished successfully!"

# Run the Python script when the container starts
CMD ["python", "main.py"]
