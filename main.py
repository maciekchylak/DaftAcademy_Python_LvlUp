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


@app.get("/suppliers", status_code=200)
async def suppliers():
    suppliers= app.db_connection.execute('''SELECT SupplierID, CompanyName FROM Suppliers
                                            ORDER BY SupplierID
                                              ''')

    result = []
    for el in suppliers:
        result.append({"SupplierID": el[0], "CompanyName": el[1]})
    return result


@app.get("/suppliers/{id}", status_code=200)
async def suppliers_id(id: int):
    suppliers= app.db_connection.execute('''SELECT *
                                             FROM Suppliers
                                             WHERE SupplierID = :id
                                             ''', {'id' : id}).fetchall()

    if suppliers is None or len(suppliers) == 0:
        raise HTTPException(status_code=404)
    return suppliers

@app.get("/suppliers/{id}/products", status_code=200)
async def suppliers_id_products(id: int):
    suppliers= app.db_connection.execute('''SELECT prod.ProductID, prod.ProductName, 
                                cat.CategoryID, cat.CategoryName, prod.Discontinued
                                             FROM Products AS prod
                                             JOIN Categories AS cat
                                             ON prod.CategoryID = cat.CategoryID
                                             WHERE SupplierID = :id
                                             ORDER BY ProductID DESC
                                             ''', {'id' : id}).fetchall()

    if suppliers is None or len(suppliers) == 0:
        raise HTTPException(status_code=404)
    return suppliers
    result = []
    for el in suppliers:
        result.append({"ProductID": el[0], "ProductName": el[1], "Category": {"CategoryID": el[2], "CategoryName": el[3]},
                       "Discontinued": el[4]})
    return result

@app.post("/suppliers", status_code=201)
async def suppliers_insert(jsonn: dict):


    suppliers= app.db_connection.execute('''INSERT INTO Suppliers 
    (CompanyName, ContactName, ContactTitle, Address, City, PostalCode, Country, 
    Phone, Fax, HomePage) VALUES (:CompanyName, 'abc', 'abc', 'asd', 'dsad', '83-110', 'country', '696-123-421', :a, :b)
                                              ''', {'CompanyName': jsonn["CompanyName"], 'a': None, 'b': None}).fetchall()

    return dict(app.db_connection.execute('''SELECT *
                                  FROM Suppliers
                                  ORDER BY SupplierID DESC
                                  LIMIT 1 '''))

@app.put("/suppliers{id}", status_code=200)
async def suppliers_post(id: int, jsonn: dict):

    suppliers = app.db_connection.execute('''SELECT *
                                             FROM Suppliers
                                             WHERE SupplierID = :id
                                             ''', {'id': id}).fetchall()
    if suppliers is None or len(suppliers) == 0:
        raise HTTPException(status_code=404)

    app.db_connection.execute('''UPDATE Suppliers SET CompanyName = :company, ContactName = :contact WHERE SupplierID = :id
                                                 ''', {'company': jsonn["CompanyName"],'contact': jsonn["ContactName"], 'id': id}).fetchall()
    return dict(app.db_connection.execute('''SELECT *
                                  FROM Suppliers
                                  WHERE SupplierID = :id ''', {'id': id}).fetchall())



