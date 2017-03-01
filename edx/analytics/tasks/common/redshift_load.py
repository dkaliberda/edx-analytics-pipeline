import logging


import luigi
from luigi import configuration
from luigi.contrib.redshift import S3CopyToTable
from edx.analytics.tasks.insights.enrollments import EnrollmentSummaryRecord
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL

log = logging.getLogger(__name__)


class RedshiftS3CopyToTable(S3CopyToTable):

    path = luigi.Parameter()

    def credentials(self):
        config = configuration.get_config()
        section = 'redshift'
        return {
            'host': config.get(section, 'host'),
            'database': config.get(section, 'database'),
            'user': config.get(section, 'user'),
            'password': config.get(section, 'password'),
            'aws_account_id': config.get(section, 'account_id'),
            'aws_arn_role_name': config.get(section, 'role_name'),
        }

    def s3_load_path(self):
        return self.path

    @property
    def aws_account_id(self):
        return self.credentials()['aws_account_id']

    @property
    def aws_arn_role_name(self):
        return self.credentials()['aws_arn_role_name']

    @property
    def host(self):
        return self.credentials()['host']

    @property
    def database(self):
        return self.credentials()['database']

    @property
    def user(self):
        return self.credentials()['user']

    @property
    def password(self):
        return self.credentials()['password']

    @property
    def aws_access_key_id(self):
        return ''

    @property
    def aws_secret_access_key(self):
        return ''

    @property
    def copy_options(self):
        return "DELIMITER '\t'"

    def copy(self, cursor, f):
        cursor.execute("""
         COPY {table} from '{source}'
         IAM_ROLE 'arn:aws:iam::{id}:role/{role}'
         {options}
         ;""".format(
            table=self.table,
            source=f,
            id=self.aws_account_id,
            role=self.aws_arn_role_name,
            options=self.copy_options)
        )


class LoadUserCourseSummaryToRedshift(RedshiftS3CopyToTable):
    columns = EnrollmentSummaryRecord.get_sql_schema

    @property
    def table(self):
        return 'd_user_course'
