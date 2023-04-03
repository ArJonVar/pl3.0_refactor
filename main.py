from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/pl-update")
async def plupdate():
    return{"message": "Update World"}


