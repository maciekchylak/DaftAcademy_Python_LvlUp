import sqlite3
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
    app.db_connection.text_factory = pom


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
    return [{"ProductID": el[0], "ProductName": el[1], "Category": {"CategoryID": el[3], "CategoryName": el[4]},
             "Discontinued": int(el[2])} for el in suppliers]



@app.delete("/suppliers/{id}", status_code=204)
async def delete(id: int):

    suppliers = app.db_connection.execute('''SELECT * FROM Suppliers WHERE SupplierID = :id''', {"id": id}).fetchone()

    if suppliers is None or len(suppliers) == 0:
        raise HTTPException(status_code=404)

    app.db_connection.execute("DELETE FROM Suppliers WHERE SupplierID = :id", {"id": id})