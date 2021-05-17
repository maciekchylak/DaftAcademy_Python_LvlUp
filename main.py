import sqlite3
from typing import Optional

from fastapi import FastAPI, HTTPException


app = FastAPI()

@app.on_event("startup")
async def startup():
    app.db_connection = sqlite3.connect("northwind.db")
    def pom(b: str):
        b = b.decode(encoding="latin1")
        b = b.replace("\n", " ")
        if len(b) > 0:
            while b[-1] == " ":
                b = b[:-1]
        return b
    app.db_connection.text_factory = pom # northwind specific


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

    result = []
    for el in suppliers:
        for el1 in el:
            result.append(el1)

    return {"SupplierID": result[0], "CompanyName": result[1], "ContactName": result[2],
            "ContactTitle": result[3], "Address": result[4], "City": result[5], "Region": result[6],
            "PostalCode": result[7], "Country": result[8], "Phone": result[9], "Fax": result[10], "HomePage": result[11]}

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
    return [{"ProductID": el[0], "ProductName": el[1], "Category": {"CategoryID": el[2], "CategoryName": el[3]},
             "Discontinued": int(el[4])} for el in suppliers]

class Supplier(BaseModel):
    CompanyName: str
    ContactName: Optional[str] = ""
    ContactTitle: Optional[str] = ""
    Address: Optional[str] = ""
    City: Optional[str] = ""
    PostalCode: Optional[str] = ""
    Country: Optional[str] = ""
    Phone: Optional[str] = ""

@app.post("/suppliers", status_code=201)
async def post_suppliers(supplier: Supplier):
    for atribute in supplier.__fields__:
        if atribute == "":
            atribute = None
    app.db_connection.execute('''
                                INSERT INTO Suppliers (CompanyName, ContactName, ContactTitle, Address, City, PostalCode, Country, Phone)
                                VALUES (:CompanyName, :ContactName, :ContactTitle, :Address, :City, :PostalCode, :Country, :Phone)
                                ''', {"CompanyName": supplier.CompanyName, "ContactName": supplier.ContactName,
                                      "ContactTitle": supplier.ContactTitle,
                                      "Address": supplier.Address, "City": supplier.City,
                                      "PostalCode": supplier.PostalCode, "Country": supplier.Country,
                                      "Phone": supplier.Phone})

    cursor = app.db_connection.cursor()
    cursor.row_factory = sqlite3.Row
    suppliers = cursor.execute('''SELECT *
                                  FROM Suppliers
                                  ORDER BY SupplierID DESC
                                  LIMIT 1''').fetchone()

    suppliers = dict(suppliers)
    for key in suppliers:
        if suppliers[key] == "":
            suppliers[key] = None

    return suppliers


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

