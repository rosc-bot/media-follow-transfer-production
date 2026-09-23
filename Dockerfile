FROM python:3.11-slim
ARG RELEASE_COMMIT=unknown
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 RELEASE_COMMIT=${RELEASE_COMMIT}
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN printf '%s\n' "$RELEASE_COMMIT" > /app/RELEASE_COMMIT
CMD ["python", "-m", "app.bot.main"]
