from fastapi import FastAPI, HTTPException
import sqlite3

app = FastAPI()


@app.get("/categories", status_code=200)
def categories():
    with sqlite3.connect("northwind.db") as connection:
        connection.text_factory = lambda b: b.decode(errors="ignore")
        cursor = connection.cursor()
        category = cursor.execute("SELECT CategoryID as id, CategoryName as name FROM Categories ORDER BY id").fetchall()

    result = []
    for el in category:
        result.append({"id": el[0], "name": el[1]})
    return {"category": result}


@app.get("/products/{id}", status_code=200)
def products(id: int):
    with sqlite3.connect("northwind.db") as connection:
        connection.text_factory = lambda b: b.decode(errors="ignore")
        cursor = connection.cursor()
        category = cursor.execute(
            "SELECT ProductID as id, ProductName as name FROM Products WHERE ProductId = id", (id, )).fetchall()
        if category is None or len(category) == 0:
            raise HTTPException(status_code=404)
        return {"id" : category[0], "name": category[1]}


@app.get("/employees", status_code=200)
def employees(limit: int = float("inf"), offset: int = 0, order: str = "id"):

    if order not in ["EmployeeID, LastName, FirstName, City"]:
        raise HTTPException(status_code=400)

    with sqlite3.connect("northwind.db") as connection:
        connection.text_factory = lambda b: b.decode(errors="ignore")
        cursor = connection.cursor()
        employee = cursor.execute(
            "SELECT EmployeeID as id, LastName as last_name, FirstName as first_name, "
            "City as city FROM Employees LIMIT limit ORDER BY order",
            (limit, offset, order)).fetchall()

    result = []
    for el in employee:
        result.append({"id": el[0], "last_name": el[1], "first_name": el[2], "city": el[3]})
    return {"employees": result}


@app.get("/products_extended", status_code=200)
def products():
    with sqlite3.connect("northwind.db") as connection:
        connection.text_factory = lambda b: b.decode(errors="ignore")
        cursor = connection.cursor()
        employee = cursor.execute("SELECT ProductID as id, ProductName as name, "
                                  "CategoryName as category, CompanyName as supplier "
                                  "FROM Products products"
                                  "JOIN Categories categories ON products.CategoryID=categories.CategotyID"
                                  "JOIN Suppliers suppliers ON suppliers.SupplierID=products.SupplierID").fetchall()
        result = []
        for el in employee:
            result.append({"id": el[0], "name": el[1], "category": el[2], "supplier": el[3]})
        return {"products_extended": result}


@app.get("/products/{id}/orders", status_code=200)
def orders(id: int):
    with sqlite3.connect("northwind.db") as connection:
        connection.text_factory = lambda b: b.decode(errors="ignore")
        cursor = connection.cursor()
        orders = cursor.execute("SELECT OrderID as id, CompanyName as customer, Quantity as quantity, "
                                  "round((UnitPrice * Quantity) - (Discount * (UnitPrice * Quantity)), 2) as total_price"
                                  "FROM Orders orders"
                                  "JOIN Customers customers ON orders.CustomerID=customers.CustomerID"
                                  "JOIN [Order Details] order_details ON order_details.OrderID=orders.OrderID"
                                  "WHERE ProductID=id", (id, )).fetchall()
    if orders is None or len(orders) == 0:
        raise HTTPException(status_code=404)
    result = []
    for el in orders:
        result.append({"id": el[0], "customer": el[1], "quantity": el[2], "total_price": el[3]})
    return {"products_extended": result}