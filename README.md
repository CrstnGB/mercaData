# MercaData PDF Invoice Processor API

This project is a FastAPI-based web application that allows users to upload PDF invoices. The system extracts relevant data from each PDF and stores it in a PostgreSQL database hosted on Render. This workflow is designed to automate invoice processing and enable easy integration with external tools like Make.com for further automation.

---

## 🚀 Features

- Upload PDF invoices through a REST API
- Temporary storage of uploaded files
- Automated data extraction and processing via a custom Python script (`processing.py`)
- Storage of processed data into a PostgreSQL database
- Environment variables managed securely via a `.env` file
- Ready to deploy on platforms like Render
- Swagger and ReDoc documentation automatically provided by FastAPI

---

## 📁 Project Structure

```
MercaData/
├── app.py                # Main FastAPI app with /upload-pdf endpoint
├── processing.py         # Custom module that processes the uploaded PDF
├── .env                  # Environment variables (e.g., DB credentials) — do not upload to GitHub
├── requirements.txt      # Python dependencies
├── temp_files/           # Temporary folder to store uploaded files
└── README.md             # Project documentation
```

---

## 🛠️ Requirements

- Python 3.8+
- PostgreSQL (Render-hosted or local)
- Virtual environment (recommended)

---

## 🧪 How to Use

1. **Install dependencies**:

```bash
pip install -r requirements.txt
```

2. **Create a `.env` file** with your environment variables:

```
DB_HOST=...
DB_PORT=...
DB_NAME=...
DB_USER=...
DB_PASSWORD=...
```

3. **Run the API**:

```bash
uvicorn app:app --reload
```

4. **Access the documentation**:

- Swagger UI: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- ReDoc: [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

---

## 🔧 API Endpoint

### `POST /upload-pdf/`

Upload a PDF file to be processed.

#### Request

- `file`: PDF invoice (`multipart/form-data`)

#### Response

```json
{
  "status": "OK",
  "message": "example_invoice.pdf processed successfully"
}
```

---

## 📦 Deployment

This project is ready to be deployed using [Render](https://render.com):

- PostgreSQL database instance hosted on Render
- FastAPI app can be deployed as a web service
- Make sure to set environment variables on the Render dashboard

---

## 🔒 Security Note

> Do **NOT** upload your `.env` file to public repositories. Add it to `.gitignore` to keep sensitive credentials safe.

---

## 🧩 Integrations

This API can be easily integrated with:

- [Make.com](https://www.make.com): For automated workflows (e.g., trigger on email received, send PDF to API)
- PostgreSQL clients like pgAdmin for database inspection
- Web UIs for frontend interaction (if needed)

---

## 📬 Contact

For questions or collaboration, feel free to open an issue or pull request.

---