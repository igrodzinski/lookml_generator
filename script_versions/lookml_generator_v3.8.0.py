import pandas as pd
import numpy as np
import re
import os
import json
import argparse

def load_base_columns(base_views_path):
    base_columns = {}
    for file_name in os.listdir(base_views_path):
        if file_name.endswith(".view.lkml"):
            view_name = file_name.replace(".view.lkml", "")
            with open(os.path.join(base_views_path, file_name), 'r') as f:
                content = f.read()
                columns = re.findall(r'(dimension|dimension_group|measure): (\w+)', content)
                base_columns[view_name] = {c[1].upper(): c[0] for c in columns}
    return base_columns

base_columns = load_base_columns('#models/_base/views')

predefined_columns = {}

def _get_column_data(row):
    return {
        'column_name': row['COLUMN NAME'].lower(),
        'description': row['DESCRIPTION'].replace('"', "''"),
        'data_type': row['TYPE'].lower(),
        'label': row['LABEL'],
        'group_label': row['GROUP_LABEL']
    }

def clean_excel_file(file_path,model_name, generate_lookml, save_datasets, generate_connections, output_dir):
    # Get file name (dataset name)
    dataset_name = os.path.basename(file_path).replace(".xlsx","")
    # Load the Excel file
    df = pd.read_excel(file_path, engine='openpyxl')
    
    # Drop blank rows
    df.dropna(how='all', inplace=True)
    
     # Usuń pierwszy wiersz
    df = df.iloc[0:].reset_index(drop=True)
    
    # Ustaw drugi wiersz jako nagłówek
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    
    # Inicjalizacja zmiennych
    df = df.drop(df.columns[df.columns.isna()],axis = 1)
    
    datasets = []
    current_dataset = []
    
    
    # Iteracja przez każdy wiersz w DataFrame
    for index, row in df.iterrows():
        if pd.isna(row['ID']):
            if current_dataset:
                datasets.append(pd.DataFrame(current_dataset))
                
                current_dataset = []
        else:
            current_dataset.append(row)
    
    if current_dataset:
        datasets.append(pd.DataFrame(current_dataset))
        
    if generate_connections:
        excluded_columns = ["FROM_DATE","TO_DATE","IS_LAST_FLAG","LINEAGE_ID","LOAD_TS","LAST_MOD_TS","SOURCE_SYSTEM_ID",
"EFFECTIVE_START_DATE","EFFECTIVE_END_DATE"]
        
        dict_datasets =  load_dataframes_from_json("DM_CLIENT.json")
        
        link_data_array = create_link_data_array(dict_datasets, excluded_columns)

        with open("link_data.json", "w", encoding="utf-8") as f:
            json.dump({"linkDataArray": link_data_array}, f, indent=4)
    
    if save_datasets:
            save_datasets_to_json(datasets,dataset_name)
    if generate_lookml:
        for i, dataset in enumerate(datasets):
            print(f'################## nr datasetu: {i}##################')
            generate_lookml_from_excel(dataset, dataset_name, model_name, output_dir, base_columns)

def load_dataframes_from_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return data
            
def create_link_data_array(dataframes, excluded_columns=None):
    if excluded_columns is None:
        excluded_columns = []
    
    link_data_array = []
    
    table_columns = {}
    for table_name, df in dataframes.items():
        filtered_columns = [col for col in df.columns.tolist() if col not in excluded_columns]
        table_columns[table_name] = filtered_columns
    
    for from_table, from_columns in table_columns.items():
        for to_table, to_columns in table_columns.items():
            if from_table != to_table:
                common_columns = set(from_columns) & set(to_columns)
                
                for column in common_columns:
                    link = {
                        "from": from_table,
                        "to": to_table,
                        "fromPort": column,
                        "toPort": column
                    }
                    
                    reverse_link = {
                        "from": to_table,
                        "to": from_table,
                        "fromPort": column,
                        "toPort": column
                    }
                    
                    if reverse_link not in link_data_array:
                        link_data_array.append(link)
    
    return link_data_array         
            
def save_datasets_to_json(datasets,dataset_name):
    print(datasets)
    f_name = dataset_name + '.json'
    dict_datasets = []
    for d in datasets:
        file_name = d['TABLE NAME'].iloc[0]
        dict_datasets.append({file_name: d.to_dict(orient='records')})
    with open(f_name,'w', encoding='utf-8') as f:
        json.dump(dict_datasets,f, indent = 4,ensure_ascii = False)

    
    print(f"Zapisano jako: {f_name}")
    return dict_datasets
    
def generate_lookml_from_excel(df, dataset_name, model_name, output_dir, base_columns):
    lookml_code_dim = []
    lookml_code_dimgr = []
    lookml_code_m = []
    file_name = df['TABLE NAME'].iloc[0]
    df_columns = set(df['COLUMN NAME'].str.upper().tolist()) # Get columns from DataFrame
    df = df.fillna('')
    df = df.sort_values(by='COLUMN NAME')
    
    extends_views = []
    include_paths = []
    commented_dimensions = set()
    comment_prefix = ""

    missing_base_columns = {}
    for view_name, columns in base_columns.items():
        base_column_names = set(columns.keys())
        if not base_column_names.isdisjoint(df_columns):
            extends_views.append(view_name)
            include_paths.append(f"/datasets/_base/views/{view_name}.view.lkml")
            commented_dimensions.update(base_column_names.intersection(df_columns))
            for col_name in base_column_names - df_columns:
                missing_base_columns[col_name] = columns[col_name]

    for index, row in df.iterrows():
        column_data = _get_column_data(row)
        column_name = column_data['column_name']
        description = column_data['description']
        data_type = column_data['data_type']
        label = column_data['label']
        group_label = column_data['group_label']
         
        table_id = file_name
        dataset_id = dataset_name.upper()

        

        if column_name in predefined_columns:
            if "dimension_group:" in predefined_columns[column_name]:
                if column_name.upper() in commented_dimensions:
                    lookml_code_dimgr.append(f'    dimension_group: {column_name} {{}}\n')
                else:
                    lookml_code_dimgr.append(predefined_columns[column_name])
            elif "measure:"in predefined_columns[column_name]:
                if column_name.upper() in commented_dimensions:
                    lookml_code_m.append(f'    measure: {column_name} {{}}\n')
                else:
                    lookml_code_m.append(predefined_columns[column_name])
            else:
                if column_name.upper() in commented_dimensions:
                    lookml_code_dim.append(f'    dimension: {column_name} {{}}\n')
                else:
                    lookml_code_dim.append(predefined_columns[column_name])
        else:
            if group_label == ''or group_label == ' ':
                if data_type == 'date' or data_type == 'datetime':
                    string_group_label = f'group_label: "{label}"'
                else:
                    string_group_label = 'group_label: ""'
                
            else:
                string_group_label = f'group_label: "{group_label}"'
                
            if data_type == 'date' or data_type == 'datetime':
                if column_name.upper() in commented_dimensions:
                    lookml_code_dimgr.append(f'    dimension_group: {column_name} {{}}\n')
                else:
                    lookml_code_dimgr.append(
                        """
    dimension_group: {column_name} {{
        label: "{label}"
        {string_group_label}
        description: "{description}"
        allow_fill: yes
        datatype: date
        type:  time
        timeframes: [date, day_of_week, month, quarter, year]
        drill_fields: [{column_name}_month, {column_name}_date]
        sql: ${{TABLE}}.{column_name} ;;
    }}""".format(column_name=column_name, label=label, string_group_label=string_group_label, description=description)
                    )
            elif data_type == 'timestamp':
                if column_name.upper() in commented_dimensions:
                    lookml_code_dim.append(f'    dimension: {column_name} {{}}\n')
                else:
                    lookml_code_dim.append(f"""
    dimension: {column_name} {{
        label: "{label}"
        {string_group_label}
        description: "{description}"
        type: date_time
        convert_tz: no
        sql: ${{TABLE}}.{column_name} ;;
    }}
""")
    
            elif data_type in ['number', 'integer', 'numeric']:
                if column_name.upper() in commented_dimensions:
                    lookml_code_dim.append(f'    dimension: {column_name} {{}}\n')
                else:
                    lookml_code_dim.append(comment_prefix + f"""
    dimension: {column_name} {{
        label: "{label}"
        {string_group_label}
        description: "{description}"
        type: number
        sql: ${{TABLE}}.{column_name} ;;
    }}
""")
            elif data_type == 'string':
                if column_name.upper() in commented_dimensions:
                    lookml_code_dim.append(f'    dimension: {column_name} {{}}\n')
                else:
                    lookml_code_dim.append(f"""
    dimension: {column_name} {{
        label: "{label}"
        {string_group_label}
        description: "{description}"
        type: string
        sql: ${{TABLE}}.{column_name} ;;
    }}
""")
            elif data_type == 'yesno':
                if column_name.upper() in commented_dimensions:
                    lookml_code_dim.append(f'    dimension: {column_name} {{}}\n')
                else:
                    lookml_code_dim.append(comment_prefix + f"""
    dimension: {column_name} {{
        label: "{label}"
        {string_group_label}
        description: "{description}"
        type: yesno
        sql: ${{TABLE}}.{column_name} ;;
    }}
""")
            elif data_type == 'sum':
                if column_name.upper() in commented_dimensions:
                    lookml_code_m.append(f'    measure: {column_name}_sum {{}}\n')
                else:
                    lookml_code_m.append(comment_prefix + f"""
    measure: {column_name}_sum {{
        label: "{label}"
        {string_group_label}
        description: "{description}"
        type: sum
        group_label: " Miary sumaryczne"
        value_format_name: liczba_2d
        sql: ${{{column_name}}} ;;
    }}
""")
            elif data_type == 'count':
                if column_name.upper() in commented_dimensions:
                    lookml_code_m.append(f'    measure: {column_name}_count {{}}\n')
                else:
                    lookml_code_m.append(comment_prefix + f"""
    measure: {column_name}_count {{
        label: "{label}"
        {string_group_label}
        description: "{description}"
        type: count
        group_label: " Miary ilościowe"
        value_format_name: liczba_2d
        sql: ${{{column_name}}} ;;
    }}
""")
            else:
                if column_name.upper() in commented_dimensions:
                    lookml_code_dim.append(f'    dimension: {column_name} {{}}\n')
                else:
                    lookml_code_dim.append(comment_prefix + f"""
    dimension: {column_name} {{
        label: "{label}"
        {string_group_label}
        description: "{description}"
        type: {data_type}
        sql: ${{TABLE}}.{column_name} ;;
    }}
""")

    # Add hidden dimensions for missing base columns
    for col_name, col_type in missing_base_columns.items():
        lookml_code_dim.append(f'    {col_type}: {col_name.lower()} {{hidden: yes}}\n')

    model_name = model_name.replace(".xlsx","")
    model_specific_output_dir = os.path.join(output_dir, model_name)
    os.makedirs(model_specific_output_dir, exist_ok=True)
    file_name = file_name.lower()
    output_path = os.path.join(model_specific_output_dir, f"{file_name}.view.lkml")

    with open(output_path, 'w', encoding = "utf-8") as f:
        for path in include_paths:
            f.write(f'include: "{path}"\n')
        f.write("view: {} {{\n  sql_table_name: `{}.{{_user_attributes['bank_id']}}.{} ` ;; \n".format(table_id.lower(), dataset_id, table_id))
        if extends_views:
            f.write(f"  extends: [{', '.join(extends_views)}]\n")
        f.write(''.join(lookml_code_dim))
        f.write(''.join(lookml_code_dimgr))
        f.write(''.join(lookml_code_m))
        f.write("\n}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate LookML from an Excel file.')
    parser.add_argument("file_path", help="Path to the Excel file")
    parser.add_argument("--output_dir", default="#generated", help="Directory to save generated LookML files.")
    parser.add_argument("--save_datasets", action="store_true", help="Save datasets to JSON")
    parser.add_argument("--generate_lookml", action="store_true", default=True, help="Generate LookML")
    parser.add_argument("--generate_connections", action="store_true", help="Generate connections")
    args = parser.parse_args()

    model_name = os.path.basename(args.file_path)
    
    clean_excel_file(args.file_path, model_name, args.generate_lookml, args.save_datasets, args.generate_connections, args.output_dir)