FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt
COPY myapp ./myapp
RUN useradd --create-home --uid 10001 appuser && mkdir -p /app/instance && chown -R appuser:appuser /app
USER appuser
EXPOSE 8080
CMD ["python", "-m", "flask", "--app", "myapp.app", "run", "--host", "0.0.0.0", "--port", "8080"]
