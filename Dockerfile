# Change the platform based on your Foundry resource queue
FROM --platform=linux/amd64 python:3.12

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install requests
COPY src .

COPY data.json .
COPY .env .

# USER is required to be non-root and numeric for running Compute Modules in Foundry
USER 5000
CMD ["python", "app.py"]