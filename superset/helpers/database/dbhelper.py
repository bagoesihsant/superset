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

def add_id_on_table(table_name, db_name):
    return """
        IF NOT EXISTS( SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{}' AND table_name = '{}' AND column_name = 'id') THEN
            ALTER TABLE {} ADD id INT PRIMARY KEY FIRST;
        END IF;
    """.format(db_name, table_name, table_name)

def create_sequence_table(table_name):
    return """
        CREATE TABLE IF NOT EXISTS sequence_id_{} (
            id INT AUTO_INCREMENT PRIMARY KEY
        )
    """.format(table_name)

def create_before_insert_trigger_table(table_name):
    return """
        DROP TRIGGER IF EXISTS get_id_{};
        CREATE TRIGGER get_id_{} BEFORE INSERT ON {} FOR EACH ROW
            BEGIN
                INSERT INTO sequence_id_{} VALUES (NULL);
                SET NEW.id = LAST_INSERT_ID();
            END 
    """.format(table_name, table_name, table_name, table_name)

def create_after_insert_trigger_table(table_name):
    return """
        DROP TRIGGER IF EXISTS remove_id_{};
        CREATE TRIGGER remove_id_{} AFTER INSERT ON {} FOR EACH ROW
            BEGIN
                DELETE FROM sequence_id_{} WHERE id = (SELECT * FROM sequence_id_{} ORDER BY id ASC LIMIT 1);
            END
    """.format(table_name, table_name, table_name, table_name, table_name)