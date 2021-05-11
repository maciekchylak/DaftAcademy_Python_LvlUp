from typing import Optional

from fastapi import FastAPI, HTTPException
import sqlite3

app = FastAPI()

@app.on_event("startup")
async def startup():
    app.db_connection = sqlite3.connect("northwind.db")
    app.db_connection.text_factory = lambda b: b.decode(errors="ignore")  # northwind specific


@app.on_event("shutdown")
async def shutdown():
    app.db_connection.close()


@app.get("/")
async def root():
    return {"sqlite3_library_version": sqlite3.version, "sqlite_version": sqlite3.sqlite_version}


@app.get("/categories")
async def categories():
    category  = app.db_connection.execute('''SELECT CategoryID AS id, CategoryName AS name
                                              FROM Categories
                                              ORDER BY id
                                              ''')
    result = []
    for el in category:
        result.append({"id": el[0], "name": el[1]})
    return {"categories": result}


@app.get("/products/{id}")
async def products(id: int):
        category  = app.db_connection.execute('''Select CustomerID AS id, CompanyName AS name, COALESCE(Address,''), COALESCE(PostalCode,''), COALESCE(City,''), COALESCE(Country,'')
                                             FROM Customers
                                             ORDER BY id COLLATE NOCASE
                                             ''')
        if category is None:
            raise HTTPException(status_code=404)
        return {"id": category[0], "name": category[1]}



@app.get("/employees")
async def employees(limit: Optional[int]=None, offset: Optional[int]=None, order: Optional[str]=None):

    if order not in ("first_name", "last_name", "city", None):
        raise HTTPException(status_code=400)
    if limit is None:
        limit = 10000000
    if offset is None:
        offset = 0
    if order is None:
        order = "id"
    if limit < 0 or offset < 0:
        raise HTTPException(status_code=400)

    employee = app.db_connection.execute('''SELECT EmployeeID AS id, LastName AS last_name, FirstName AS first_name, City AS city
                                             FROM Employees
                                             ORDER BY {}
                                             LIMIT :limit OFFSET :offset
                                             '''.format(order), {'limit': limit, 'offset': offset})


    result = []
    for el in employee:
        result.append({"id": el[0], "last_name": el[1], "first_name": el[2], "city": el[3]})
    return {"employees": result}


@app.get("/products_extended")
async def products():
        employee = app.db_connection.execute('''SELECT p.ProductID AS id, p.ProductName AS name, c.CategoryName AS category, s.CompanyName AS supplier
                                                     FROM Products AS p
                                                     JOIN Categories AS c
                                                     ON p.CategoryID = c.CategoryID
                                                     JOIN Suppliers AS s
                                                     ON p.SupplierID = s.SupplierID''')
        result = []
        for el in employee:
            result.append({"id": el[0], "name": el[1], "category": el[2], "supplier": el[3]})
        return {"products_extended": result}


@app.get("/products/{id}/orders", status_code=200)
async def orders(id: int):
    with sqlite3.connect("northwind.db") as connection:
        connection.text_factory = lambda b: b.decode(errors="ignore")
        cursor = connection.cursor()
        orders = app.db_connection.execute('''SELECT o.OrderID AS id, c.CompanyName AS customer, od.Quantity, ROUND((od.UnitPrice * od.Quantity) - (od.Discount * (od.UnitPrice * od.Quantity)), 2) AS total_price
                                                   FROM Orders AS o
                                                   JOIN Customers AS c
                                                   ON o.CustomerID = c.CustomerID
                                                   JOIN "Order Details" as od
                                                   ON o.OrderID = od.OrderID
                                                   WHERE od.ProductID = :id
                                                   ''', {"id": id}).fetchall()
    if orders is None or len(orders) == 0:
        raise HTTPException(status_code=404)
    result = []
    for el in orders:
        result.append({"id": el[0], "customer": el[1], "quantity": el[2], "total_price": el[3]})
    return {"products_extended": result}