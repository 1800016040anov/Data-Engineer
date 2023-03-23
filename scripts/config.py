    #check value


def view_all_data(tables):
    for key,value in tables.items():
        display(value)
        print(value.isna().sum())
        print(value.dtypes)
        print(key)