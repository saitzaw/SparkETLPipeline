def job_filter(df, job_title):
    return df.loc[df["job_title"] == job_title]