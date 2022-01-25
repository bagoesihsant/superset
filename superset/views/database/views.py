# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import io
import os
import tempfile
import zipfile
from typing import TYPE_CHECKING

import pandas as pd
import numpy as np

from flask import flash, g, redirect
from flask_appbuilder import expose, SimpleFormView
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_appbuilder.security.decorators import has_access
from flask_babel import lazy_gettext as _
from sqlalchemy.exc import IntegrityError
from werkzeug.wrappers import Response
from superset.helpers.txt_hash.hash import hash_text
from wtforms.fields import StringField
from wtforms.validators import ValidationError

import superset.models.core as models
from superset import app, db, is_feature_enabled, exc
from superset.connectors.sqla.models import SqlaTable
from superset.constants import MODEL_VIEW_RW_METHOD_PERMISSION_MAP, RouteMethod
from superset.exceptions import CertificateException
from superset.sql_parse import Table
from superset.typing import FlaskResponse
from superset.utils import core as utils
from superset.views.base import DeleteMixin, SupersetModelView, YamlExportMixin
from superset.helpers import hash, punctuation, regex, stopword, str_columns, dbhelper

from .forms import ColumnarToDatabaseForm, CsvToDatabaseForm, ExcelToDatabaseForm
from .mixins import DatabaseMixin
from .validators import schema_allows_csv_upload, sqlalchemy_uri_validator

if TYPE_CHECKING:
    from werkzeug.datastructures import FileStorage

config = app.config
stats_logger = config["STATS_LOGGER"]


def sqlalchemy_uri_form_validator(_: _, field: StringField) -> None:
    """
    Check if user has submitted a valid SQLAlchemy URI
    """

    sqlalchemy_uri_validator(field.data, exception=ValidationError)


def certificate_form_validator(_: _, field: StringField) -> None:
    """
    Check if user has submitted a valid SSL certificate
    """
    if field.data:
        try:
            utils.parse_ssl_cert(field.data)
        except CertificateException as ex:
            raise ValidationError(ex.message) from ex


def upload_stream_write(form_file_field: "FileStorage", path: str) -> None:
    chunk_size = app.config["UPLOAD_CHUNK_SIZE"]
    with open(path, "bw") as file_description:
        while True:
            chunk = form_file_field.stream.read(chunk_size)
            if not chunk:
                break
            file_description.write(chunk)

class DatabaseView(
    DatabaseMixin, SupersetModelView, DeleteMixin, YamlExportMixin
):  # pylint: disable=too-many-ancestors
    datamodel = SQLAInterface(models.Database)

    class_permission_name = "Database"
    method_permission_name = MODEL_VIEW_RW_METHOD_PERMISSION_MAP

    include_route_methods = RouteMethod.CRUD_SET

    add_template = "superset/models/database/add.html"
    edit_template = "superset/models/database/edit.html"
    validators_columns = {
        "sqlalchemy_uri": [sqlalchemy_uri_form_validator],
        "server_cert": [certificate_form_validator],
    }

    yaml_dict_key = "databases"

    def _delete(self, pk: int) -> None:
        DeleteMixin._delete(self, pk)

    @expose("/list/")
    @has_access
    def list(self) -> FlaskResponse:
        if not is_feature_enabled("ENABLE_REACT_CRUD_VIEWS"):
            return super().list()

        return super().render_app_template()


class CsvToDatabaseView(SimpleFormView):
    form = CsvToDatabaseForm
    form_template = "superset/form_view/csv_to_database_view/edit.html"
    form_title = _("CSV to Database configuration")
    add_columns = ["database", "schema", "table_name"]

    def form_get(self, form: CsvToDatabaseForm) -> None:
        form.sep.data = ","
        form.header.data = 0
        form.mangle_dupe_cols.data = True
        form.skipinitialspace.data = False
        form.skip_blank_lines.data = True
        form.infer_datetime_format.data = True
        form.decimal.data = "."
        form.if_exists.data = "fail"
        form.hash_status.data = False
        form.pre_process.data = False

    def form_post(self, form: CsvToDatabaseForm) -> Response:
        database = form.con.data
        csv_table = Table(table=form.name.data, schema=form.schema.data)

        if not schema_allows_csv_upload(database, csv_table.schema):
            message = _(
                'Database "%(database_name)s" schema "%(schema_name)s" '
                "is not allowed for csv uploads. Please contact your Superset Admin.",
                database_name=database.database_name,
                schema_name=csv_table.schema,
            )
            flash(message, "danger")
            return redirect("/csvtodatabaseview/form")

        if "." in csv_table.table and csv_table.schema:
            message = _(
                "You cannot specify a namespace both in the name of the table: "
                '"%(csv_table.table)s" and in the schema field: '
                '"%(csv_table.schema)s". Please remove one',
                table=csv_table.table,
                schema=csv_table.schema,
            )
            flash(message, "danger")
            return redirect("/csvtodatabaseview/form")

        try:
            df = pd.concat(
                pd.read_csv(
                    chunksize=1000,
                    encoding="utf-8",
                    filepath_or_buffer=form.csv_file.data,
                    header=form.header.data if form.header.data else 0,
                    index_col=form.index_col.data,
                    infer_datetime_format=form.infer_datetime_format.data,
                    iterator=True,
                    keep_default_na=not form.null_values.data,
                    mangle_dupe_cols=form.mangle_dupe_cols.data,
                    usecols=form.usecols.data if form.usecols.data else None,
                    na_values=form.null_values.data if form.null_values.data else None,
                    nrows=form.nrows.data,
                    parse_dates=form.parse_dates.data,
                    sep=form.sep.data,
                    skip_blank_lines=form.skip_blank_lines.data,
                    skipinitialspace=form.skipinitialspace.data,
                    skiprows=form.skiprows.data,
                )
            )

            # # Checking if the file uploaded have the same column(s) as the standard column(s)
            # if str_columns.check_std_columns(df):
            #     # If the file uploaded have the same column(s) as the standard column(s)
            #     pass
            # else:
            #     # If the file uploaded don't have the same column(s) as the standard column(s)
            #     raise Exception("Column(s) inside the file doesn't match the standard column(s) that has been set. Please rename your file column(s) and try again.")

            # Pre Processing Form
            if form.pre_process.data == True:
                # If Pre Processing Selected
                # Check if the user is giving column(s) to pre process or not
                if form.selected_col.data == None:
                    # If the user did not give any column(s)
                    # Getting the column(s) name and datatype
                    dfType = dict(df.dtypes)

                    # Looping the column(s)
                    for key, value in dfType.items():
                        # Check if the column(s) datatype equals to object
                        if value == np.object:
                            # If the Column(s) datatype equals to object

                            # Pre Processing Begin
                            # Transforming the text into lowercase character(s)
                            df[key] = df[key].apply(stopword.lowercase_text)

                            # Removing Special Character(s) using Regex
                            # Checking if the user specify a new regex string
                            if form.regex_str.data == None:
                                # If the user do not specify a new regex string
                                df[key] = df[key].apply(regex.regex_word)
                            else:
                                # If the user specify a new regex string
                                df[key] = df[key].apply(lambda x: regex.regex_word(x, form.regex_str.data))

                            # Removing Text Punctuation(s)
                            df[key] = df[key].apply(punctuation.remove_punctuation)

                            # Removing Text Stopword(s)
                            df[key] = df[key].apply(stopword.remove_stopword)

                            # Stem the Text(s)
                            df[key] = df[key].apply(stopword.stemming_word)
                        else:
                            # If the Column(s) datatype is not equals to object
                            pass
                else:
                    # If the user give any column(s) as input 
                    # Checking User Input Column(s) with available Data Frame Column(s)
                    check_cols = [col for col in form.selected_col.data.split(",") if col in df.columns]

                    # Check if the User Input Column(s) have at least 1 valid column(s)
                    if len(check_cols) > 0:
                        # If there's at least 1 column(s) available from user input and match the dataframe columns 
                        # Looping the column(s)
                        for col in check_cols:
                            # Pre Processing Begin
                            # Transforming the text into lowercase character(s)
                            df[col] = df[col].apply(stopword.lowercase_text)

                            # Removing Special Character(s) using Regex
                            # Checking if the user specify a new regex string
                            if form.regex_str.data == None:
                                # If the user do not specify a new regex string
                                df[col] = df[col].apply(regex.regex_word)
                            else:
                                # If the user specify a new regex string
                                df[col] = df[col].apply(lambda x: regex.regex_word(x, form.regex_str.data))

                            # Removing Text Punctuation(s)
                            df[col] = df[col].apply(punctuation.remove_punctuation)

                            # Removing Text Stopword(s)
                            df[col] = df[col].apply(stopword.remove_stopword)

                            # Stem the Text(s)
                            df[col] = df[col].apply(stopword.stemming_word)
                    else:
                        # If there's no column(s) that match the dataframe columns
                        raise Exception("Column(s) not found inside the file provided")
            else:
                # If Pre Processing was not selected
                pass
            

            # Checking if the user decide to hash the text(s)
            if form.hash_status.data == True:
                # If the user decide to hash the text(s)
                # Check if the user give any specific column(s) to hash
                if form.hash_str.data == None:
                    # If the user decide not to give any specific column(s) to hash
                    # Getting the column(s) name and datatype
                    dfType = dict(df.dtypes)

                    # Looping the column(s)
                    for key,value in dfType.items():
                        # Check if the column(s) datatype equals to object
                        if value == np.object:
                            # If the Column(s) datatype equals to object
                            df[key] = df[key].apply(hash.hash_text)
                        else:
                            # If the Column(s) datatype is not equals to object
                            pass
                else:
                    # If the user decide to give any specific column(s) to hash
                    # Checking User Input Column(s) with available Data Frame Column(s)
                    check_cols = [col for col in form.hash_str.data.split(",") if col in df.columns]

                    # Check if the User Input Column(s) have at least 1 valid column(s)
                    if len(check_cols) > 0:
                        # If there's at least 1 column(s) available from user input and match the dataframe columns 
                        # Looping the column(s)
                        for col in check_cols:
                            # Hashing specified column(s)
                            df[col] = df[col].apply(hash.hash_text)
                    else:
                        # If there's no column(s) that match the dataframe columns
                        raise Exception("Column(s) not found inside the file provided")
            else:
                # If the user decide not to hash the text(s)
                pass

            # Change Datatype Object to Datetime for Date Column
            for col in df.columns:
                if "DATE" in col:
                    if (pd.api.types.is_datetime64_any_dtype(df[col])):
                        pass
                    else:
                        df[col] = pd.to_datetime(df[col])
                    # pd.to_datetime(d[c], infer_datetime_format=True) 

            database = (
                db.session.query(models.Database)
                .filter_by(id=form.data.get("con").data.get("id"))
                .one()
            )

            # Intercept The Database

            # Create Empty Table for body
            empty_table = df.head(0)

            # Get Database Name
            # db_name = form.con.data

            database.db_engine_spec.df_to_sql(
                database,
                csv_table,
                empty_table,
                to_sql_kwargs={
                    "chunksize": 1000,
                    "if_exists": form.if_exists.data,
                    "index": form.index.data,
                    "index_label": form.index_label.data,
                },
            )

            with database.get_sqla_engine().connect() as conn:
                conn.execute(dbhelper.create_sequence())
                conn.execute(dbhelper.add_id_on_table(csv_table))
            #     conn.execute(dbhelper.create_before_insert_trigger_table(csv_table))
            #     conn.execute(dbhelper.create_after_insert_trigger_table(csv_table))
            #     conn.execute(dbhelper.create_function_get_delimiter_count(csv_table))
            #     conn.execute(dbhelper.create_function_split_by_delimiter(csv_table))
            #     conn.execute(dbhelper.create_diaglist_table(csv_table))
            #     conn.execute(dbhelper.create_proclist_table(csv_table))
            #     conn.execute(dbhelper.create_procedure_insert_diaglist(csv_table))
            #     conn.execute(dbhelper.create_procedure_insert_proclist(csv_table))
            #     conn.execute(dbhelper.create_trigger_after_insert_diaglist(csv_table))
            #     conn.execute(dbhelper.create_trigger_after_insert_proclist(csv_table))
                
                

            # Manual Insert
            # for i in range(len(df)):
            #     try:
            #         database.db_engine_spec.df_to_sql(
            #             database,
            #             csv_table,
            #             df.iloc[i:(i+1)],
            #             to_sql_kwargs= {
            #                 "if_exists" : form.if_exists.data,
            #                 "index": form.index.data
            #             }
            #         )
            #     except exc.IntegrityError as e:
            #         pass

            # to_sql insert ( batch insert )
            database.db_engine_spec.df_to_sql(
                database,
                csv_table,
                df,
                to_sql_kwargs={
                    "chunksize": 1000,
                    "if_exists": form.if_exists.data,
                    "index": form.index.data,
                    "index_label": form.index_label.data,
                },
            )

            # Connect table to the database that should be used for exploration.
            # E.g. if hive was used to upload a csv, presto will be a better option
            # to explore the table.
            expore_database = database
            explore_database_id = database.explore_database_id
            if explore_database_id:
                expore_database = (
                    db.session.query(models.Database)
                    .filter_by(id=explore_database_id)
                    .one_or_none()
                    or database
                )

            sqla_table = (
                db.session.query(SqlaTable)
                .filter_by(
                    table_name=csv_table.table,
                    schema=csv_table.schema,
                    database_id=expore_database.id,
                )
                .one_or_none()
            )

            if sqla_table:
                sqla_table.fetch_metadata()
            if not sqla_table:
                sqla_table = SqlaTable(table_name=csv_table.table)
                sqla_table.database = expore_database
                sqla_table.database_id = database.id
                sqla_table.user_id = g.user.get_id()
                sqla_table.schema = csv_table.schema
                sqla_table.fetch_metadata()
                db.session.add(sqla_table)
            db.session.commit()
        except Exception as ex:  # pylint: disable=broad-except
            db.session.rollback()
            message = _(
                'Unable to upload CSV file "%(filename)s" to table '
                '"%(table_name)s" in database "%(db_name)s". '
                "Error message: %(error_msg)s",
                filename=form.csv_file.data.filename,
                table_name=form.name.data,
                db_name=database.database_name,
                error_msg=str(ex),
            )

            flash(message, "danger")
            stats_logger.incr("failed_csv_upload")
            return redirect("/csvtodatabaseview/form")

        # Go back to welcome page / splash screen
        message = _(
            'CSV file "%(csv_filename)s" uploaded to table "%(table_name)s" in '
            'database "%(db_name)s"',
            csv_filename=form.csv_file.data.filename,
            table_name=str(csv_table),
            db_name=sqla_table.database.database_name,
        )
        flash(message, "info")
        stats_logger.incr("successful_csv_upload")
        return redirect("/tablemodelview/list/")


class ExcelToDatabaseView(SimpleFormView):
    form = ExcelToDatabaseForm
    form_template = "superset/form_view/excel_to_database_view/edit.html"
    form_title = _("Excel to Database configuration")
    add_columns = ["database", "schema", "table_name"]

    def form_get(self, form: ExcelToDatabaseForm) -> None:
        form.header.data = 0
        form.mangle_dupe_cols.data = True
        form.decimal.data = "."
        form.if_exists.data = "fail"
        form.sheet_name.data = ""

    def form_post(self, form: ExcelToDatabaseForm) -> Response:
        database = form.con.data
        excel_table = Table(table=form.name.data, schema=form.schema.data)

        if not schema_allows_csv_upload(database, excel_table.schema):
            message = _(
                'Database "%(database_name)s" schema "%(schema_name)s" '
                "is not allowed for excel uploads. Please contact your Superset Admin.",
                database_name=database.database_name,
                schema_name=excel_table.schema,
            )
            flash(message, "danger")
            return redirect("/exceltodatabaseview/form")

        if "." in excel_table.table and excel_table.schema:
            message = _(
                "You cannot specify a namespace both in the name of the table: "
                '"%(excel_table.table)s" and in the schema field: '
                '"%(excel_table.schema)s". Please remove one',
                table=excel_table.table,
                schema=excel_table.schema,
            )
            flash(message, "danger")
            return redirect("/exceltodatabaseview/form")

        uploaded_tmp_file_path = tempfile.NamedTemporaryFile(  # pylint: disable=consider-using-with
            dir=app.config["UPLOAD_FOLDER"],
            suffix=os.path.splitext(form.excel_file.data.filename)[1].lower(),
            delete=False,
        ).name

        try:
            utils.ensure_path_exists(config["UPLOAD_FOLDER"])
            upload_stream_write(form.excel_file.data, uploaded_tmp_file_path)

            df = pd.read_excel(
                header=form.header.data if form.header.data else 0,
                index_col=form.index_col.data,
                io=form.excel_file.data,
                keep_default_na=not form.null_values.data,
                mangle_dupe_cols=form.mangle_dupe_cols.data,
                na_values=form.null_values.data if form.null_values.data else None,
                parse_dates=form.parse_dates.data,
                skiprows=form.skiprows.data,
                sheet_name=form.sheet_name.data if form.sheet_name.data else 0,
            )

            # Pre Processing Form
            if form.pre_process.data == True:
                # If Pre Processing Selected
                # Check if the user is giving column(s) to pre process or not
                if form.selected_col.data == None:
                    # If the user did not give any column(s)
                    # Getting the column(s) name and datatype
                    dfType = dict(df.dtypes)

                    # Looping the column(s)
                    for key, value in dfType.items():
                        # Check if the column(s) datatype equals to object
                        if value == np.object:
                            # If the Column(s) datatype equals to object

                            # Pre Processing Begin
                            # Transforming the text into lowercase character(s)
                            df[key] = df[key].apply(stopword.lowercase_text)

                            # Removing Special Character(s) using Regex
                            # Checking if the user specify a new regex string
                            if form.regex_str.data == None:
                                # If the user do not specify a new regex string
                                df[key] = df[key].apply(regex.regex_word)
                            else:
                                # If the user specify a new regex string
                                df[key] = df[key].apply(lambda x: regex.regex_word(x, form.regex_str.data))

                            # Removing Text Punctuation(s)
                            df[key] = df[key].apply(punctuation.remove_punctuation)

                            # Removing Text Stopword(s)
                            df[key] = df[key].apply(stopword.remove_stopword)

                            # Stem the Text(s)
                            df[key] = df[key].apply(stopword.stemming_word)
                        else:
                            # If the Column(s) datatype is not equals to object
                            pass
                else:
                    # If the user give any column(s) as input 
                    # Checking User Input Column(s) with available Data Frame Column(s)
                    check_cols = [col for col in form.selected_col.data.split(",") if col in df.columns]

                    # Check if the User Input Column(s) have at least 1 valid column(s)
                    if len(check_cols) > 0:
                        # If there's at least 1 column(s) available from user input and match the dataframe columns 
                        # Looping the column(s)
                        for col in check_cols:
                            # Pre Processing Begin
                            # Transforming the text into lowercase character(s)
                            df[col] = df[col].apply(stopword.lowercase_text)

                            # Removing Special Character(s) using Regex
                            # Checking if the user specify a new regex string
                            if form.regex_str.data == None:
                                # If the user do not specify a new regex string
                                df[col] = df[col].apply(regex.regex_word)
                            else:
                                # If the user specify a new regex string
                                df[col] = df[col].apply(lambda x: regex.regex_word(x, form.regex_str.data))

                            # Removing Text Punctuation(s)
                            df[col] = df[col].apply(punctuation.remove_punctuation)

                            # Removing Text Stopword(s)
                            df[col] = df[col].apply(stopword.remove_stopword)

                            # Stem the Text(s)
                            df[col] = df[col].apply(stopword.stemming_word)
                    else:
                        # If there's no column(s) that match the dataframe columns
                        raise Exception("Column(s) not found inside the file provided")
            else:
                # If Pre Processing was not selected
                pass
            

            # Checking if the user decide to hash the text(s)
            if form.hash_status.data == True:
                # If the user decide to hash the text(s)
                # Check if the user give any specific column(s) to hash
                if form.hash_str.data == None:
                    # If the user decide not to give any specific column(s) to hash
                    # Getting the column(s) name and datatype
                    dfType = dict(df.dtypes)

                    # Looping the column(s)
                    for key,value in dfType.items():
                        # Check if the column(s) datatype equals to object
                        if value == np.object:
                            # If the Column(s) datatype equals to object
                            df[key] = df[key].apply(hash.hash_text)
                        else:
                            # If the Column(s) datatype is not equals to object
                            pass
                else:
                    # If the user decide to give any specific column(s) to hash
                    # Checking User Input Column(s) with available Data Frame Column(s)
                    check_cols = [col for col in form.hash_str.data.split(",") if col in df.columns]

                    # Check if the User Input Column(s) have at least 1 valid column(s)
                    if len(check_cols) > 0:
                        # If there's at least 1 column(s) available from user input and match the dataframe columns 
                        # Looping the column(s)
                        for col in check_cols:
                            # Hashing specified column(s)
                            df[col] = df[col].apply(hash.hash_text)
                    else:
                        # If there's no column(s) that match the dataframe columns
                        raise Exception("Column(s) not found inside the file provided")
            else:
                # If the user decide not to hash the text(s)
                pass

            database = (
                db.session.query(models.Database)
                .filter_by(id=form.data.get("con").data.get("id"))
                .one()
            )

            database.db_engine_spec.df_to_sql(
                database,
                excel_table,
                df,
                to_sql_kwargs={
                    "chunksize": 1000,
                    "if_exists": form.if_exists.data,
                    "index": form.index.data,
                    "index_label": form.index_label.data,
                },
            )

            # Connect table to the database that should be used for exploration.
            # E.g. if hive was used to upload a excel, presto will be a better option
            # to explore the table.
            expore_database = database
            explore_database_id = database.explore_database_id
            if explore_database_id:
                expore_database = (
                    db.session.query(models.Database)
                    .filter_by(id=explore_database_id)
                    .one_or_none()
                    or database
                )

            sqla_table = (
                db.session.query(SqlaTable)
                .filter_by(
                    table_name=excel_table.table,
                    schema=excel_table.schema,
                    database_id=expore_database.id,
                )
                .one_or_none()
            )

            if sqla_table:
                sqla_table.fetch_metadata()
            if not sqla_table:
                sqla_table = SqlaTable(table_name=excel_table.table)
                sqla_table.database = expore_database
                sqla_table.database_id = database.id
                sqla_table.user_id = g.user.get_id()
                sqla_table.schema = excel_table.schema
                sqla_table.fetch_metadata()
                db.session.add(sqla_table)
            db.session.commit()
        except Exception as ex:  # pylint: disable=broad-except
            db.session.rollback()
            message = _(
                'Unable to upload Excel file "%(filename)s" to table '
                '"%(table_name)s" in database "%(db_name)s". '
                "Error message: %(error_msg)s",
                filename=form.excel_file.data.filename,
                table_name=form.name.data,
                db_name=database.database_name,
                error_msg=str(ex),
            )

            flash(message, "danger")
            stats_logger.incr("failed_excel_upload")
            return redirect("/exceltodatabaseview/form")

        # Go back to welcome page / splash screen
        message = _(
            'Excel file "%(excel_filename)s" uploaded to table "%(table_name)s" in '
            'database "%(db_name)s"',
            excel_filename=form.excel_file.data.filename,
            table_name=str(excel_table),
            db_name=sqla_table.database.database_name,
        )
        flash(message, "info")
        stats_logger.incr("successful_excel_upload")
        return redirect("/tablemodelview/list/")


class ColumnarToDatabaseView(SimpleFormView):
    form = ColumnarToDatabaseForm
    form_template = "superset/form_view/columnar_to_database_view/edit.html"
    form_title = _("Columnar to Database configuration")
    add_columns = ["database", "schema", "table_name"]

    def form_get(self, form: ColumnarToDatabaseForm) -> None:
        form.if_exists.data = "fail"

    def form_post(  # pylint: disable=too-many-locals
        self, form: ColumnarToDatabaseForm
    ) -> Response:
        database = form.con.data
        columnar_table = Table(table=form.name.data, schema=form.schema.data)
        files = form.columnar_file.data
        file_type = {file.filename.split(".")[-1] for file in files}

        if file_type == {"zip"}:
            zipfile_ob = zipfile.ZipFile(  # pylint: disable=consider-using-with
                form.columnar_file.data[0]
            )  # pylint: disable=consider-using-with
            file_type = {filename.split(".")[-1] for filename in zipfile_ob.namelist()}
            files = [
                io.BytesIO((zipfile_ob.open(filename).read(), filename)[0])
                for filename in zipfile_ob.namelist()
            ]

        if len(file_type) > 1:
            message = _(
                "Multiple file extensions are not allowed for columnar uploads."
                " Please make sure all files are of the same extension.",
            )
            flash(message, "danger")
            return redirect("/columnartodatabaseview/form")

        read = pd.read_parquet
        kwargs = {
            "columns": form.usecols.data if form.usecols.data else None,
        }

        if not schema_allows_csv_upload(database, columnar_table.schema):
            message = _(
                'Database "%(database_name)s" schema "%(schema_name)s" '
                "is not allowed for columnar uploads. "
                "Please contact your Superset Admin.",
                database_name=database.database_name,
                schema_name=columnar_table.schema,
            )
            flash(message, "danger")
            return redirect("/columnartodatabaseview/form")

        if "." in columnar_table.table and columnar_table.schema:
            message = _(
                "You cannot specify a namespace both in the name of the table: "
                '"%(columnar_table.table)s" and in the schema field: '
                '"%(columnar_table.schema)s". Please remove one',
                table=columnar_table.table,
                schema=columnar_table.schema,
            )
            flash(message, "danger")
            return redirect("/columnartodatabaseview/form")

        try:
            chunks = [read(file, **kwargs) for file in files]
            df = pd.concat(chunks)

            database = (
                db.session.query(models.Database)
                .filter_by(id=form.data.get("con").data.get("id"))
                .one()
            )

            database.db_engine_spec.df_to_sql(
                database,
                columnar_table,
                df,
                to_sql_kwargs={
                    "chunksize": 1000,
                    "if_exists": form.if_exists.data,
                    "index": form.index.data,
                    "index_label": form.index_label.data,
                },
            )

            # Connect table to the database that should be used for exploration.
            # E.g. if hive was used to upload a csv, presto will be a better option
            # to explore the table.
            expore_database = database
            explore_database_id = database.explore_database_id
            if explore_database_id:
                expore_database = (
                    db.session.query(models.Database)
                    .filter_by(id=explore_database_id)
                    .one_or_none()
                    or database
                )

            sqla_table = (
                db.session.query(SqlaTable)
                .filter_by(
                    table_name=columnar_table.table,
                    schema=columnar_table.schema,
                    database_id=expore_database.id,
                )
                .one_or_none()
            )

            if sqla_table:
                sqla_table.fetch_metadata()
            if not sqla_table:
                sqla_table = SqlaTable(table_name=columnar_table.table)
                sqla_table.database = expore_database
                sqla_table.database_id = database.id
                sqla_table.user_id = g.user.get_id()
                sqla_table.schema = columnar_table.schema
                sqla_table.fetch_metadata()
                db.session.add(sqla_table)
            db.session.commit()
        except Exception as ex:  # pylint: disable=broad-except
            db.session.rollback()
            message = _(
                'Unable to upload Columnar file "%(filename)s" to table '
                '"%(table_name)s" in database "%(db_name)s". '
                "Error message: %(error_msg)s",
                filename=[file.filename for file in form.columnar_file.data],
                table_name=form.name.data,
                db_name=database.database_name,
                error_msg=str(ex),
            )

            flash(message, "danger")
            stats_logger.incr("failed_columnar_upload")
            return redirect("/columnartodatabaseview/form")

        # Go back to welcome page / splash screen
        message = _(
            'Columnar file "%(columnar_filename)s" uploaded to table "%(table_name)s" '
            'in database "%(db_name)s"',
            columnar_filename=[file.filename for file in form.columnar_file.data],
            table_name=str(columnar_table),
            db_name=sqla_table.database.database_name,
        )
        flash(message, "info")
        stats_logger.incr("successful_columnar_upload")
        return redirect("/tablemodelview/list/")
