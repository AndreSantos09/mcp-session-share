FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN useradd -m -u 1000 appuser
USER appuser

EXPOSE 8000

CMD ["python", "-m", "app.main"]
