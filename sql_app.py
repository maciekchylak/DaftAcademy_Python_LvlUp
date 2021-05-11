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
    category = app.db_connection.execute("SELECT CategoryID as id, CategoryName as name FROM Categories ORDER BY id")
    result = []
    for el in category:
        result.append({"id": el[0], "name": el[1]})
    return {"categories": result}


@app.get("/products/{id}")
async def products(id: int):
        category = app.db_connection.execute(
            "SELECT ProductID as id, ProductName as name FROM Products WHERE ProductId = ?", (id, )).fetchall()
        if category is None:
            raise HTTPException(status_code=404)
        return {"id" : category[0], "name": category[1]}



@app.get("/employees")
async def employees(limit: int = float("inf"), offset: int = 0, order: str = "id"):

    if order not in ["EmployeeID, LastName, FirstName, City"]:
        raise HTTPException(status_code=400)

    with sqlite3.connect("northwind.db") as connection:
        connection.text_factory = lambda b: b.decode(errors="ignore")
        cursor = connection.cursor()
        employee = cursor.execute(
            "SELECT EmployeeID as id, LastName as last_name, FirstName as first_name, "
            "City as city FROM Employees LIMIT ? ORDER BY ? OFFSET ?",
            (limit, order, offset)).fetchall()

    result = []
    for el in employee:
        result.append({"id": el[0], "last_name": el[1], "first_name": el[2], "city": el[3]})
    return {"employees": result}


@app.get("/products_extended")
async def products():
    with sqlite3.connect("northwind.db") as connection:
        connection.text_factory = lambda b: b.decode(errors="ignore")
        cursor = connection.cursor()
        employee = cursor.execute("SELECT ProductID as id, ProductName as name, "
                                  "CategoryName as category, CompanyName as supplier "
                                  "FROM Products products"
                                  "JOIN Categories categories ON products.CategoryID=categories.CategotyID"
                                  "JOIN Suppliers suppliers ON suppliers.SupplierID=products.SupplierID")
        result = []
        for el in employee:
            result.append({"id": el[0], "name": el[1], "category": el[2], "supplier": el[3]})
        return {"products_extended": result}


@app.get("/products/{id}/orders", status_code=200)
async def orders(id: int):
    with sqlite3.connect("northwind.db") as connection:
        connection.text_factory = lambda b: b.decode(errors="ignore")
        cursor = connection.cursor()
        orders = cursor.execute("SELECT OrderID as id, CompanyName as customer, Quantity as quantity, "
                                  "round((UnitPrice * Quantity) - (Discount * (UnitPrice * Quantity)), 2) as total_price"
                                  "FROM Orders orders"
                                  "JOIN Customers customers ON orders.CustomerID=customers.CustomerID"
                                  "JOIN [Order Details] order_details ON order_details.OrderID=orders.OrderID"
                                  "WHERE ProductID=?", (id, )).fetchall()
    if orders is None or len(orders) == 0:
        raise HTTPException(status_code=404)
    result = []
    for el in orders:
        result.append({"id": el[0], "customer": el[1], "quantity": el[2], "total_price": el[3]})
    return {"products_extended": result}