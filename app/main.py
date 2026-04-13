from fastapi import FastAPI

app = FastAPI(title="Oil Flows Backend")

@app.get("/health")
def health():
    return {"status": "ok"}
