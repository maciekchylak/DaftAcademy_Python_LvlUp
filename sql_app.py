import sqlite3
from fastapi import FastAPI, HTTPException


app = FastAPI()

@app.on_event("startup")
async def startup():
    app.db_connection = sqlite3.connect("northwind.db")
    app.db_connection.text_factory = lambda b: b.decode(errors="ignore")  # northwind specific


@app.on_event("shutdown")
async def shutdown():
    app.db_connection.close()


@app.get("/categories")
async def categories():
    category = app.db_connection.execute('''SELECT CategoryID AS id, CategoryName AS name
                                              FROM Categories
                                              ORDER BY id
                                              ''')
    result = []
    for el in category:
        result.append({"id": el[0], "name": el[1]})
    return {"categories": result}



@app.get("/products/{id}")
async def products(id: int):
        category  = app.db_connection.execute('''SELECT ProductID, ProductName
                                           FROM Products
                                           WHERE ProductID = :id
                                           ''', {'id': id}).fetchone()
        if category is None or len(category) == 0:
            raise HTTPException(status_code=404)
        return {"id": category[0], "name": category[1]}


@app.get("/employees")
async def employees(limit: int = 100000, offset: int = 0, order: str = "id"):

    if limit < 0 or offset < 0 or order not in ["first_name", "last_name", "city", "id", None]:
        raise HTTPException(status_code=400)

    employee = app.db_connection.execute('''SELECT EmployeeID AS id, LastName AS last_name, FirstName AS first_name, City AS city
                                             FROM Employees
                                             ORDER BY :order
                                             LIMIT :limit OFFSET :offset
                                             ''', {'order': order,'limit': limit, 'offset': offset}).fetchall()
    result = []
    for el in employee:
        result.append({"id": el[0], "last_name": el[1], "first_name": el[2], "city": el[3]})
    return {"employees": result}


@app.get("/products_extended")
async def products():
        employee = app.db_connection.execute('''SELECT products.ProductID AS id, products.ProductName AS name, categories.CategoryName AS category, suppliers.CompanyName AS supplier
                                                     FROM Products AS products
                                                     JOIN Categories AS categories
                                                     ON products.CategoryID = categories.CategoryID
                                                     JOIN Suppliers AS suppliers
                                                     ON products.SupplierID = suppliers.SupplierID''')
        result = []
        for el in employee:
            result.append({"id": el[0], "name": el[1], "category": el[2], "supplier": el[3]})
        return {"products_extended": result}


@app.get("/products/{id}/orders", status_code=200)
async def orders(id: int):
    orders = app.db_connection.execute('''SELECT o.OrderID AS id, c.CompanyName AS customer, od.Quantity, ROUND((od.UnitPrice * od.Quantity) - (od.Discount * (od.UnitPrice * od.Quantity)), 2) AS total_price
                                                   FROM Orders AS o
                                                   JOIN Customers AS c
                                                   ON o.CustomerID = c.CustomerID
                                                   JOIN "Order Details" as od
                                                   ON o.OrderID = od.OrderID
                                                   WHERE od.ProductID = :id                                                                                                
                                                   ''', {"id": id}).fetchone()
    if orders is None:
        raise HTTPException(status_code=404)

    result = []
    for el in orders:
        result.append({"id": el[0], "customer": el[1], "quantity": el[2], "total_price": el[3]})
    return {"products_extended": result}