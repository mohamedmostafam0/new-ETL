import pandas as pd

def CustomerID_PlaceHolders(df):
    """ Fill missing CustomerID values with 0 """
    df['CustomerID'].fillna(0, inplace=True)
    return df

def Remove_Negative_Quantity(df):
    """ Remove rows with negative or zero UnitPrice """
    return df[df['UnitPrice'] > 0]

def Remove_Duplicates(df):
    """ Remove duplicate rows from the dataset """
    return df.drop_duplicates()

def Remove_Outliers(df):
    """ Remove outliers from UnitPrice using the IQR method """
    Q1 = df['UnitPrice'].quantile(0.25)
    Q3 = df['UnitPrice'].quantile(0.75)
    IQR = Q3 - Q1
    return df[(df['UnitPrice'] >= (Q1 - 1.5 * IQR)) & (df['UnitPrice'] <= (Q3 + 1.5 * IQR))]

def transform_row(row):
    """ Clean and transform individual row before sending to Kafka """
    try:
        return {
            "InvoiceNo": row.get("InvoiceNo", "").strip(),
            "StockCode": row.get("StockCode", "").strip(),
            "Description": row.get("Description", "").strip(),
            "Quantity": int(row["Quantity"]) if row["Quantity"].isdigit() else None,
            "InvoiceDate": row.get("InvoiceDate", "").strip(),
            "UnitPrice": float(row["UnitPrice"]) if row["UnitPrice"].replace('.', '', 1).isdigit() else None,
            "CustomerID": int(row["CustomerID"]) if row["CustomerID"] and row["CustomerID"].isdigit() else 0,
            "Country": row.get("Country", "").strip()
        }
    except Exception as e:
        print(f"Error transforming row: {e}")
        return None

def transform_dataframe(df):
    """ Apply all transformations on the full dataset """
    df = CustomerID_PlaceHolders(df)
    df = Remove_Negative_Quantity(df)
    df = Remove_Duplicates(df)
    df = Remove_Outliers(df)
    return df
