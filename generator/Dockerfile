FROM python:3.14-slim
WORKDIR /app
COPY generator/requirements.txt ./generator/requirements.txt
RUN pip install --no-cache-dir -r generator/requirements.txt
COPY generator/ ./generator/
CMD ["python", "-m", "generator.main"]