class Load:
    """
    A class used to load dataframes

    Attributes
    ----------
    df : dataframe
        ip4 data in tabular format

    write_mode : string
        parameter to decide if 'parquet' or 'database' mode is chosen

    db_url : string
        url to databas

    db_properties : dictionairy
        properties for the database

    Methods
    -------
    load()
        writes dataframe according to mode
    """

    def __init__(self, df, name, **kwargs):
        self.df = df
        self.name = name
        self.write_mode = kwargs.get("write_mode", None)
        self.db_url = kwargs.get("db_url", None)
        self.db_properties = kwargs.get("db_properties", None)

    def load(self):
        if self.write_mode == "database":
            self._write_db(self.name)
        else:
            self._write_parquet(self.name)

    def _write_parquet(self, name):
        self.df.write.parquet(name)

    def _write_db(self, name):
        mode = "overwrite"
        url = self.db_url
        properties = self.db_properties
        self.df.write.jdbc(url=url, table=name, mode=mode, properties=properties)