def transfrom(df_tmp, product):
    df_tmp[f"flag_{product}"] = df_tmp.apply(
        lambda x: x[f"sum_{product}"] != 0 and x[f"count_{product}"] != 0, axis=1
    ).astype(int)

    df_tmp = df_tmp.filter(regex="flag").reset_index()

    return df_tmp
