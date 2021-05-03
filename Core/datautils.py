import pandas as pd


class DataStruct:
    COLUMNS = (
        "id",
        "url",
        "source",
        "street",
        "number",
        "zip",
        "locality",
        "property_type",
        "property_subtype",
        "price",
        "sale_type",
        "rooms",
        "inside_area",
        "full_equ_kitchen",
        "furnished",
        "open_fire",
        "terrace",
        "terrace_area",
        "garden",
        "garden_area",
        "land_area",
        "facades",
        "swmming_pool",
        "building_state"
    )

    DTYPES = (
        int,
        str,
        str,
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
        bool,
        str
    )

    COLUMNSDATA = {c: d for c, d in zip(COLUMNS, DTYPES)}

    __DATATEMPLATE = pd.DataFrame([], columns=COLUMNS)

    for column in __DATATEMPLATE.columns:
        __DATATEMPLATE[column] = __DATATEMPLATE[column].astype(COLUMNSDATA[column])

    DATAFRAMETEMPLATE = __DATATEMPLATE.copy()


if __name__ == "__main__":
    print(DataStruct.COLUMNS)
    print(DataStruct.DTYPES)

    for k, v in DataStruct.COLUMNSDATA.items():
        print((k, v))
    print(DataStruct.DATAFRAMETEMPLATE.info())

