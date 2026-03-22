# Timesheet Auditor Backend (stateless)

Upload a CSV timesheet → returns flagged rows + reasons.

## Quickstart

```bash
python -m venv .venv
# Windows
.\.venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## API

- `GET /health`
- `POST /audit` (multipart form with `file`)

Response: JSON with `flags` and `summary`.
