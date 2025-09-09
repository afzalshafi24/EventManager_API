from fastapi import FastAPI

app = FastAPI()

@app.get("/data")
async def get_data():
    # Return a tuple of data
    data = ("value1", 42, 3.14)
    return {"data": data}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)