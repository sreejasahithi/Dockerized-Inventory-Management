FROM python:3.12.2-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container
WORKDIR /usr/src/app/consumer_2

# Copy the requirements file into the container at /app
COPY .  /usr/src/app/consumer_2/

# Install any dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python" , "consumer_2.py"]