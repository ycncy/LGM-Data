def clean_series_dataframe(series_dataframe):
    keys_filter = series_dataframe.filter(items=["id", "league_id", "full_name", "slug", "begin_at", "end_at"])

    cleaned_dataframe = keys_filter.drop_duplicates()

    return cleaned_dataframe


def clean_matches_dataframe(matches_dataframe):
    return