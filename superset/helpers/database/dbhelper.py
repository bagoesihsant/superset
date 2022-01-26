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

def add_id_on_table(table_name):
    return """
        ALTER TABLE {} ADD COLUMN IF NOT EXISTS id INT NOT NULL PRIMARY KEY DEFAULT nextval('eklaim_sequences');
    """.format(table_name)

def create_sequence():
    return """
        CREATE SEQUENCE IF NOT EXISTS eklaim_sequences START 1 INCREMENT 1;
    """
def create_diaglist_table(table_name):
    return """
        CREATE TABLE IF NOT EXISTS diaglist_{} (
            id BIGSERIAL PRIMARY KEY NOT NULL,
            id_eklaim INT NOT NULL,
            SEP VARCHAR(255) NOT NULL,
            kode_diagnosis VARCHAR(255) NOT NULL
        )
    """.format(table_name)

def create_proclist_table(table_name):
    return """
        CREATE TABLE IF NOT EXISTS proclist_{} (
            id BIGSERIAL PRIMARY KEY NOT NULL,
            id_eklaim INT NOT NULL,
            SEP VARCHAR(255) NOT NULL,
            kode_probabilitas VARCHAR(255) NOT NULL
        )
    """.format(table_name)

def create_function_add_diaglist(table_name):
    return """
        CREATE OR REPLACE FUNCTION add_diaglist()
        RETURNS TRIGGER
        AS
        $$
        BEGIN
            INSERT INTO diaglist_{} (id_eklaim, SEP, kode_diagnosis) SELECT "id", "SEP", UNNEST(STRING_TO_ARRAY("DIAGLIST", ';')) FROM {} WHERE id = NEW.id;
            RETURN NEW;
        END
        $$ LANGUAGE 'plpgsql';
    """.format(table_name, table_name)

def create_function_add_proclist(table_name):
    return """
        CREATE OR REPLACE FUNCTION add_proclist()
        RETURNS TRIGGER
        AS
        $$
        BEGIN
            INSERT INTO proclist_{} (id_eklaim, SEP, kode_probabilitas) SELECT "id", "SEP", UNNEST(STRING_TO_ARRAY("PROCLIST", ';')) FROM {} WHERE id = NEW.id;
            RETURN NEW;
        END
        $$ LANGUAGE 'plpgsql';
    """.format(table_name, table_name)

def create_trigger_after_insert_diaglist(table_name):
    return """
        DROP TRIGGER IF EXISTS after_insert_diaglist ON {}; 

        CREATE TRIGGER after_insert_diaglist
            AFTER INSERT
            ON {}
            FOR EACH ROW
            EXECUTE PROCEDURE add_diaglist();
    """.format(table_name, table_name)

def create_trigger_after_insert_proclist(table_name):
    return """
        DROP TRIGGER IF EXISTS after_insert_proclist ON {}; 

        CREATE TRIGGER after_insert_proclist
            AFTER INSERT
            ON {}
            FOR EACH ROW
            EXECUTE PROCEDURE add_proclist();
    """.format(table_name, table_name)

