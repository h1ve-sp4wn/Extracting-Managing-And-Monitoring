# Build Stage
FROM python:3.11-slim AS build
WORKDIR /usr/src/app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

# Production Stage
FROM python:3.11-slim AS production
WORKDIR /usr/src/app
COPY --from=build /usr/src/app /usr/src/app
CMD ["python3", "main.py"]