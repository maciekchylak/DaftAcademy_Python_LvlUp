from fastapi import FastAPI, status, Response, Request
import datetime, hashlib

app = FastAPI()
app.counter = 0
app.data = dict()

@app.get("/")
def root():
    return {"message": "Hello world!"}


@app.get("/method")
def root(request: Request):
    method = request.method
    return {"method": f"{method}"}

@app.put("/method")
def root(request: Request):
    method = request.method
    return {"method": f"{method}"}

@app.options("/method")
def root(request: Request):
    method = request.method
    return {"method": f"{method}"}

@app.delete("/method")
def root(request: Request):
    method = request.method
    return {"method": f"{method}"}

@app.post("/method", status_code=201)
def root(request: Request):
    method = request.method
    return {"method": f"{method}"}

@app.get("/auth", status_code=401)
def root(password: str, password_hash: str, response: Response):
    m = hashlib.sha512()
    m.update(bytes(f"{password}", encoding="utf-8"))

    if password_hash == m.hexdigest():
        response.status_code = status.HTTP_204_NO_CONTENT
        return
    else:
        return

@app.post("/register", status_code= 201)
def root(json_all: dict, response: Response):
    name = json_all.get("name")
    surname = json_all.get("surname")

    if name is None or surname is None:
        response.status_code = 422
        return
    app.counter += 1
    start_date = datetime.datetime.today()
    end_date = start_date + datetime.timedelta(days=(len(name) + len(surname)))
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    json_new = {"id": app.counter, "name": name,
            "surname": surname, "register_date": start_date, "vaccination_date": end_date}
    app.data[app.counter] = json_new
    return json_new

@app.get("/patient/{id}", status_code= 200)
def root(id: int, response: Response):
    if id < 1:
        response.status_code = 400
        return
    if id in app.data:
        return app.data.get(id)
    else:
        response.status_code = 404
        return

