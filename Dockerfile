FROM python:3.11-slim

WORKDIR /app

COPY main.py .
COPY BlissfulBonsai.json .

RUN pip install --no-cache-dir tqdm google-api-python-client google-auth-oauthlib google-auth-httplib2

CMD ["bash"]
