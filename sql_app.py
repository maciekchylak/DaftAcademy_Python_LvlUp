from typing import Optional

from fastapi import FastAPI, HTTPException
import sqlite3

app = FastAPI()

@app.on_event("startup")
async def startup():
    app.db_connection = sqlite3.connect("northwind.db")
    app.db_connection.text_factory = lambda b: b.decode(errors="ignore")


@app.on_event("shutdown")
async def shutdown():
    app.db_connection.close()


@app.get("/")
async def root():
    return

@app.get("/categories")
async def categories():
    app.db_connection.row_factory = sqlite3.Row
    category = app.db_connection.execute('''SELECT CategoryID as id, CategoryName as name FROM Categories ORDER BY id''')
    result = []
    for el in category:
        result.append({"id": el[0], "name": el[1]})
    return {"categories": result}


@app.get("/products/{id}")
async def products(id: int):
    app.db_connection.row_factory = sqlite3.Row
    category = app.db_connection.execute(
        '''SELECT ProductID as id, ProductName as name FROM Products WHERE ProductId = :id''', {"id", id}).fetchone()
    if category is None:
        raise HTTPException(status_code=404)
    return {"id" : category[0], "name": category[1]}



@app.get("/employees")
async def employees(limit: Optional[int]=None, offset: Optional[int]=None, order: Optional[str]=None):

    if order not in ("first_name", "last_name", "city", None):
        raise HTTPException(status_code=400)
    if limit is None:
        limit = 100000
    if offset is None:
        offset = 0
    if order is None:
        order = "id"
    if limit < 0 or offset < 0:
        raise HTTPException(status_code=400)
    app.db_connection.row_factory = sqlite3.Row
    employee = app.db_connection.execute(
            '''SELECT EmployeeID AS id, LastName AS last_name, FirstName AS first_name, 
            City AS city FROM Employees LIMIT :limit ORDER BY :order OFFSET :offset''',
        {"limit" : limit,"order": order, "offset" : offset})

    result = []
    for el in employee:
        result.append({"id": el[0], "last_name": el[1], "first_name": el[2], "city": el[3]})
    return {"employees": result}


@app.get("/products_extended")
async def products():
    app.db_connection.row_factory = sqlite3.Row
    employee = app.db_connection.execute('''SELECT ProductID AS id, ProductName AS name, 
                              CategoryName AS category, CompanyName AS supplier 
                              FROM Products products
                              JOIN Categories categories ON products.CategoryID=categories.CategotyID
                              JOIN Suppliers suppliers ON suppliers.SupplierID=products.SupplierID''')

    result = []
    for el in employee:
        result.append({"id": el[0], "name": el[1], "category": el[2], "supplier": el[3]})
    return {"products_extended": result}


@app.get("/products/{id}/orders", status_code=200)
async def orders(id: int):
    app.db_connection.row_factory = sqlite3.Row
    orders = app.db_connection.execute('''SELECT OrderID as id, CompanyName as customer, Quantity as quantity, 
                                  round((UnitPrice * Quantity) - (Discount * (UnitPrice * Quantity)), 2) as total_price
                                  FROM Orders orders
                                  JOIN Customers customers ON orders.CustomerID=customers.CustomerID
                                  JOIN [Order Details] order_details ON order_details.OrderID=orders.OrderID
                                  WHERE ProductID=:id''', {"id": id}).fetchone()
    if orders is None or len(orders) == 0:
        raise HTTPException(status_code=404)
    result = []
    for el in orders:
        result.append({"id": el[0], "customer": el[1], "quantity": el[2], "total_price": el[3]})
    return {"products_extended": result}