from datetime import datetime


from fastapi import FastAPI, Response, Request, Depends, status, HTTPException, Cookie
from fastapi.responses import HTMLResponse, PlainTextResponse, JSONResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import Optional

app = FastAPI()


@app.get("/hello", response_class=HTMLResponse)
def hello():
    return f'<h1>Hello! Today date is {str(datetime.today().strftime("%Y-%m-%d"))} </h1>'


app.token1 = []
app.token2 = []
poprawnosc = HTTPBasic()

@app.post("/login_session", status_code=201)
def logowanie(*, dane: HTTPBasicCredentials = Depends(poprawnosc), response: Response):

    if (not(dane.username == '4dm1n')) or (not (dane.password == 'NotSoSecurePa$$')):
        raise HTTPException(status_code=401)

    response.set_cookie(key="session_token", value="123454321")


@app.post("/login_token", status_code=201)
def login_token(*, dane: HTTPBasicCredentials = Depends(poprawnosc),  response: Response):

    if not (dane.username == '4dm1n' and dane.password == 'NotSoSecurePa$$'):
        raise HTTPException(status_code=401)

    return {"token": "1234321"}


@app.get("/welcome_session")
def welcome_session(*, request: Request, session_token: str = Cookie(None)):
    if not (str(session_token) in app.token1):
        raise HTTPException(status_code=401)
    if str(request.query_params.get("format")) == "json":
        return JSONResponse(content={"message": "Welcome!"})
    elif str(request.query_params.get("format")) == "html":
        return HTMLResponse(content="<h1>Welcome!</h1>")
    else:
        return PlainTextResponse(content="Welcome!")


@app.get("/welcome_token")
def welcome_token(format: Optional[str] = "", token: Optional[str] = ""):
    if not (str(token) in app.token2):
        raise HTTPException(status_code=401)
    if format == "json":
        return JSONResponse(content={"message": "Welcome!"})
    elif format == "html":
        return HTMLResponse(content="<h1>Welcome!</h1>")
    else:
        return PlainTextResponse(content="Welcome!")


@app.delete("/logout_session")
def logout_session(format: Optional[str] = "", session_token: str = Cookie(None)):
    if not (str(session_token) in app.token1):
        raise HTTPException(status_code=401)
    while session_token in app.token1: app.token1.remove(session_token)
    return RedirectResponse("/logged_out?format=" + format, status_code=303)


@app.delete("/logout_token")
def logout_token(token: Optional[str], format: Optional[str] = ""):
    if not (str(token) in app.token2):
        raise HTTPException(status_code=401)
    while token in app.token2:
        app.token2.remove(token)
    return RedirectResponse("/logged_out?format=" + format, status_code=303)


@app.get("/logged_out")
def logged_out(format: Optional[str] = ""):
    if format == "json":
        return JSONResponse(content={"message": "Logged out!"})
    elif format == "html":
        return HTMLResponse(content="<h1>Logged out!</h1>")
    else:
        return PlainTextResponse(content="Logged out!")