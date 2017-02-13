import luigi
from luigi.s3 import S3PathTask
from luigi.contrib.redshift import RedshiftTarget, S3CopyToTable


class S3ToRedshift(S3CopyToTable):
    s3_load_path = luigi.Parameter()
    aws_access_key_id = luigi.Parameter()
    aws_secret_access_key = luigi.Parameter()
    host = luigi.Parameter()
    database = luigi.Parameter(default="storetrans")
    table = luigi.Parameter(default="store_transaction")
    copy_options = luigi.Parameter(default="DELIMITER ';'")
    user = luigi.Parameter()
    password = luigi.Parameter()

    def requires(self):
        return S3PathTask(path=self.s3_load_path)



if __name__ == "__main__":
    luigi.run()
