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

def create_diaglist_table(table_name):
    return """
        CREATE TABLE IF NOT EXISTS diaglist_{} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_eklaim INT,
            kode_diagnosis VARCHAR(255)
        )
    """.format(table_name)

def create_proclist_table(table_name):
    return """
        CREATE TABLE IF NOT EXISTS proclist_{} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_eklaim INT,
            kode_probabilitas VARCHAR(255)
        )
    """.format(table_name)

def create_function_get_delimiter_count(table_name):
    return """
        DROP FUNCTION IF EXISTS func_{}_get_delimiter_count;
        CREATE FUNCTION func_{}_get_delimiter_count (f_string VARCHAR(255), f_delimiter VARCHAR(5))
        RETURNS INT(11)
        BEGIN
            RETURN 1 + (LENGTH(f_string) - LENGTH(REPLACE(f_string, f_delimiter, '')));
        END
    """.format(table_name, table_name)

def create_function_split_by_delimiter(table_name):
    return """
        DROP FUNCTION IF EXISTS func_{}_split_by_delimiter;
        CREATE FUNCTION func_{}_split_by_delimiter (f_string VARCHAR(255), f_delimiter VARCHAR(5), f_order INT(11)) RETURNS VARCHAR(255) CHARSET utf8
        BEGIN
            DECLARE result VARCHAR(255) DEFAULT '';
            SET result = REVERSE(SUBSTRING_INDEX(REVERSE(SUBSTRING_INDEX(f_string, f_delimiter, f_order)), f_delimiter, 1));
            RETURN result;
        END
    """.format(table_name, table_name)

def create_procedure_insert_diaglist(table_name):
    return """
        DROP PROCEDURE IF EXISTS insert_split_result_diaglist_{};
        CREATE PROCEDURE insert_split_result_diaglist_{} (IN f_string VARCHAR(255), IN f_delimiter VARCHAR(5), IN f_old_id INT(11))
        BEGIN
            DECLARE counter INT DEFAULT 0;
            DECLARE i INT DEFAULT 0;
            SET counter = func_{}_get_delimiter_count(f_string, f_delimiter);
            WHILE i < counter
                DO
                    SET i = i + 1;
                    INSERT INTO diaglist_{} (id_eklaim, kode_diagnosis) VALUES (f_old_id, func_{}_split_by_delimiter(f_string, f_delimiter, i));
            END WHILE;
        END    
    """.format(table_name, table_name, table_name, table_name, table_name)

def create_procedure_insert_proclist(table_name):
    return """
        DROP PROCEDURE IF EXISTS insert_split_result_proclist_{};
        CREATE PROCEDURE insert_split_result_proclist_{} (IN f_string VARCHAR(255), IN f_delimiter VARCHAR(5), IN f_old_id INT(11))
        BEGIN
            DECLARE counter INT DEFAULT 0;
            DECLARE i INT DEFAULT 0;
            SET counter = func_{}_get_delimiter_count(f_string, f_delimiter);
            WHILE i < counter
                DO
                    SET i = i + 1;
                    INSERT INTO proclist_{} (id_eklaim, kode_probabilitas) VALUES (f_old_id, func_{}_split_by_delimiter(f_string, f_delimiter, i));
            END WHILE;
        END    
    """.format(table_name, table_name, table_name, table_name, table_name)

def create_trigger_after_insert_diaglist(table_name):
    return """
        DROP TRIGGER IF EXISTS after_insert_diaglist_{};
        CREATE TRIGGER after_insert_diaglist_{} AFTER INSERT ON {} FOR EACH ROW
            BEGIN
                CALL insert_split_result_diaglist_{}(NEW.DIAGLIST, ';', NEW.id);
            END
    """.format(table_name, table_name, table_name, table_name)

def create_trigger_after_insert_proclist(table_name):
    return """
        DROP TRIGGER IF EXISTS after_insert_proclist_{};
        CREATE TRIGGER after_insert_proclist_{} AFTER INSERT ON {} FOR EACH ROW
            BEGIN
                CALL insert_split_result_proclist_{}(NEW.PROCLIST, ';', NEW.id);
            END
    """.format(table_name, table_name, table_name, table_name)