FROM python:3.9

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 6969

CMD ["sh", "-c", "celery -A app.celery worker --loglevel=info & python app.py"]