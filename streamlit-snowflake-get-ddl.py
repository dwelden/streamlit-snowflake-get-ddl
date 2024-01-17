# Based on https://cloudyard.in/2024/01/streamlit-app-export-ddl/

import re
import streamlit as st
from snowflake.snowpark.context import get_active_session

st.title("Get DDL")
st.write("This is a basic example and enhancements are needed for a mature app.")

session = get_active_session()

db_list = session.sql("SHOW DATABASES").collect()
db_names = [db[1] for db in db_list]

db_name = st.selectbox("Select Database", db_names, key=f"selected_dbnames")
if db_name:
    # Fetch schemas in selected database
    sch_list = session.sql(f"SHOW SCHEMAS IN DATABASE {db_name}").collect()
    sch_names = [sch[1] for sch in sch_list]
    sch_name = st.selectbox("Select Schema", sch_names, index=0, key=f"schemaname_list")
    if sch_name:
        entity_types = [
            "Dynamic Table",
            "Event Table",
            "File Format",
            "Function",
            "Iceberg Table",
            "Masking Policy",
            "Password Policy",
            "Pipe",
            "Procedure",
            "Row Access Policy",
            "Sequence",
            "Session Policy",
            "Stream",
            "Table",
            "Tag",
            "Task",
            "View"
        ]
        entity_type = st.selectbox("Select Object Type", entity_types)

        if entity_type:
            
            ent_list = (session
                        .sql(f"SHOW {re.sub('Policy','Policie',entity_type)}S IN SCHEMA {db_name}.{sch_name}")
                        .collect()
            )
            if ent_list:
                if ent_list[0].asDict().get("is_builtin",""):
                    ent_names = [ent.arguments for ent in ent_list if ent.is_builtin == 'N']
                else:
                    ent_names = [ent.name for ent in ent_list]
                entity_name = st.selectbox(f"Select {entity_type}", ent_names, key=f"selected_entity_list")
                if entity_name:
                    if 'Policy' in entity_type:
                        ent_type = 'Policy'
                    else:
                        ent_type = re.sub(" ", "_", entity_type)
                    ent_name = re.sub("(.*?) RETURN.*", "\\1", entity_name)
                    df = (session
                          .sql(f"SELECT GET_DDL('{ent_type}', '{db_name}.{sch_name}.{ent_name}', true) AS DDL")
                          .collect()
                    )
                    st.write(f"### DDL")
                    st.code(df[0].DDL, language="sql")
