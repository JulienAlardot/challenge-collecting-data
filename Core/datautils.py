import os

import pandas as pd

_zipcodes = pd.read_csv(os.path.join(os.path.dirname(os.path.dirname(__file__)), "Data", "zipcodes.csv"))


class DataStruct:
    COLUMNS = (
        "Id",
        "Url",
        "Source",
        "Zip",
        "Locality",
        "Type of property",
        "Subtype of property",
        "Price",
        "Type of sale",
        "Number of rooms",
        "Area",
        "Fully equipped kitchen",
        "Furnished",
        "Open fire",
        "Terrace",
        "Terrace Area",
        "Garden",
        "Garden Area",
        "Surface of the land",
        "Surface area of the plot of land",
        "Number of facades",
        "Swimming pool",
        "State of the building"
    )

    DTYPES = (
        int,
        str,
        str,
        int,
        str,
        str,
        str,
        int,
        str,
        int,
        int,
        bool,
        bool,
        bool,
        bool,
        int,
        bool,
        int,
        int,
        int,
        int,
        bool,
        str
    )

    __TRUEDTYPES = (
        int,
        str,
        "category",
        int,
        "category",
        "category",
        "category",
        int,
        "category",
        int,
        int,
        bool,
        bool,
        bool,
        bool,
        int,
        bool,
        int,
        int,
        int,
        int,
        bool,
        "category"
    )
    COLUMNSDATA = {c: d for c, d in zip(COLUMNS, DTYPES)}

    __DATATEMPLATE = pd.DataFrame([], columns=COLUMNS)

    for column in __DATATEMPLATE.columns:
        __DATATEMPLATE[column] = __DATATEMPLATE[column].astype(COLUMNSDATA[column])

    DATAFRAMETEMPLATE = __DATATEMPLATE.copy()

    @classmethod
    def clean_data(cls, df):
        df = df.copy()
        df = df.dropna(how="all")
        df = df.drop_duplicates(keep="first")
        df = cls.convert_data(df)
        return df

    @classmethod
    def convert_data(cls, df):
        df = df.copy()
        for columns, dtype in zip(df.columns, cls.__TRUEDTYPES):
            if dtype == "category":
                df[columns] = pd.Categorical(df[columns])
            else:
                df[columns] = df[columns].astype(str)
        return df

    @classmethod
    def get_locality(cls, zipcode):
        where = _zipcodes[_zipcodes["zipcode"] == zipcode]["local"].str.capitalize().values
        return where

    @classmethod
    def get_zipcode(cls, locality):
        where = _zipcodes[_zipcodes["local"].str.lower() == locality.lower()]["zipcode"].values
        return where


if __name__ == "__main__":
    print(DataStruct.COLUMNS)
    print(DataStruct.DTYPES)

    for k, v in DataStruct.COLUMNSDATA.items():
        print((k, v))
    print(DataStruct.DATAFRAMETEMPLATE.info())
    print(DataStruct.get_locality(5101))
    print(DataStruct.get_zipcode("Loyers"))
