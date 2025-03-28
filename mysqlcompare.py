import mysql.connector

def get_table_structure(db_config, db_name):
    """Recupera informazioni sulle colonne e chiavi esterne."""
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)

    # Recupera le colonne della tabella
    query_columns = f"""
        SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{db_name}';
    """
    cursor.execute(query_columns)
    
    table_info = {}
    
    for row in cursor.fetchall():
        table = row["TABLE_NAME"]
        if table not in table_info:
            table_info[table] = {"columns": {}, "foreign_keys": []}
        
        table_info[table]["columns"][row["COLUMN_NAME"]] = {
            "name": row["COLUMN_NAME"],
            "type": row["COLUMN_TYPE"],
            "nullability": "NOT NULL" if row["IS_NULLABLE"] == "NO" else "NULL",
            "default": row["COLUMN_DEFAULT"],
            "extra": row["EXTRA"]
        }

    # Recupera le chiavi esterne
    query_foreign_keys = f"""
        SELECT 
            kcu.TABLE_NAME, kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME,
            rc.CONSTRAINT_NAME, rc.UPDATE_RULE, rc.DELETE_RULE
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
        ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
        WHERE kcu.TABLE_SCHEMA = '{db_name}' AND kcu.REFERENCED_TABLE_NAME IS NOT NULL;
    """
    
    cursor.execute(query_foreign_keys)

    for row in cursor.fetchall():
        table = row["TABLE_NAME"]
        table_info[table]["foreign_keys"].append({
            "column": row["COLUMN_NAME"],
            "ref_table": row["REFERENCED_TABLE_NAME"],
            "ref_column": row["REFERENCED_COLUMN_NAME"],
            "constraint_name": row["CONSTRAINT_NAME"],
            "update_rule": row["UPDATE_RULE"],
            "delete_rule": row["DELETE_RULE"]
        })

    cursor.close()
    conn.close()
    
    return table_info

def generate_sql_diff(dev_schema, prod_schema):
    sql_diff = []

    for table in dev_schema:
        if table in prod_schema:
            dev_columns = dev_schema[table]["columns"]
            prod_columns = prod_schema[table]["columns"]

            added_columns = []
            removed_columns = []
            modified_columns = []

            for col_name, col_info in dev_columns.items():
                if col_name not in prod_columns:
                    added_columns.append(col_info)
                else:
                    # Controlla se il tipo, nullability o default sono cambiati
                    prod_col = prod_columns[col_name]
                    if (col_info["type"] != prod_col["type"] or
                        col_info["nullability"] != prod_col["nullability"] or
                        col_info["default"] != prod_col["default"] or
                        col_info["extra"] != prod_col["extra"]):

                        modified_columns.append((col_name, prod_col, col_info))

            for col_name in prod_columns:
                if col_name not in dev_columns:
                    removed_columns.append(col_name)

            # Genera gli ALTER TABLE per colonne
            if added_columns:
                for col in added_columns:
                    alter_stmt = f"ALTER TABLE `{table}` ADD COLUMN `{col['name']}` {col['type']} {col['nullability']}"
                    if col["default"] is not None:
                        alter_stmt += f" DEFAULT '{col['default']}'"
                    if col["extra"]:
                        alter_stmt += f" {col['extra']}"
                    sql_diff.append(alter_stmt + ";")

            if removed_columns:
                for col in removed_columns:
                    sql_diff.append(f"ALTER TABLE `{table}` DROP COLUMN `{col}`;")

            if modified_columns:
                for col_name, old_col, new_col in modified_columns:
                    alter_stmt = f"ALTER TABLE `{table}` MODIFY COLUMN `{col_name}` {new_col['type']} {new_col['nullability']}"

                    if new_col["default"] is not None:
                        default_value = new_col["default"].strip("'")  # Rimuove apici extra se presenti

                        # Se il default è una funzione (es. CURRENT_TIMESTAMP()), non usare apici
                        if default_value.upper() in ["CURRENT_TIMESTAMP()", "NOW()"]:
                            alter_stmt += f" DEFAULT {default_value}"
                        else:
                            alter_stmt += f" DEFAULT '{default_value}'"

                    if new_col["extra"]:
                        alter_stmt += f" {new_col['extra']}"
                    sql_diff.append(alter_stmt + ";")


            # Confronto delle FOREIGN KEY
            dev_fks = {fk["constraint_name"]: fk for fk in dev_schema[table]["foreign_keys"]}
            prod_fks = {fk["constraint_name"]: fk for fk in prod_schema[table]["foreign_keys"]}

            # Aggiunta di nuove FK
            for fk_name, fk in dev_fks.items():
                if fk_name not in prod_fks:
                    sql_diff.append(
                        f"ALTER TABLE `{table}` ADD CONSTRAINT `{fk_name}` FOREIGN KEY (`{fk['column']}`) "
                        f"REFERENCES `{fk['ref_table']}` (`{fk['ref_column']}`) "
                        f"ON UPDATE {fk['update_rule']} ON DELETE {fk['delete_rule']};"
                    )

            # Rimozione di FK esistenti
            for fk_name in prod_fks:
                if fk_name not in dev_fks:
                    sql_diff.append(f"ALTER TABLE `{table}` DROP FOREIGN KEY `{fk_name}`;")

        else:
            sql_diff.append(f"-- TODO: CREATE TABLE `{table}` con definizione completa;")

    return sql_diff

def main():
    db_config = {
        'user': 'root',
        'password': '',
        'host': 'localhost',
    }

    dev_schema = get_table_structure(db_config, "tobiascivoletto")
    prod_schema = get_table_structure(db_config, "tobiascivoletto_prod")

    sql_diff = generate_sql_diff(dev_schema, prod_schema)

    if sql_diff:
        print("Le modifiche necessarie sono:")
        for diff in sql_diff:
            print(diff)
    else:
        print("Le strutture dei database sono uguali!")

if __name__ == "__main__":
    main()
