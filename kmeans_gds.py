import os
import pandas

from graphdatascience import GraphDataScience

def main():
    gds = GraphDataScience("neo4j://localhost:7687", auth=("neo4j", os.environ["NEO4J_PWD"]))
    print(gds.version())
    df = pandas.read_csv(f'{os.environ["NEO4J_HOME"]}/import/responses.csv')

    categorical_cols, person_properties = setup_person_properties_from_csv_fields(df)
    # Load
    print(gds.run_cypher(
        f"""
            LOAD CSV WITH HEADERS 
            FROM "file:///responses.csv" AS row
            CREATE {person_properties}
        """
    ))
    print(gds.run_cypher(
        """
            MATCH (p:Person) RETURN count(p)
        """
    ))
    # In Neo4j, since there is no table schema or equivalent to restrict possible properties,
    # non-existence and null are equivalent for node and relationship properties.
    # That is, there really is no such thing as a property with a null value; null indicates
    # that the property doesn’t exist at all
    # (https://neo4j.com/developer/kb/understanding-non-existent-properties-and-null-values/)
    # Assign averages to null values for non-categorical columns.
    query = query_to_assign_avgs_to_null_non_category_columns(categorical_cols)
    print(gds.run_cypher(
       query
    ))
    # Encode categorical variables
    for col in categorical_cols:
        cypher_query = query_to_encode_categorical_column(col, df)
        print(f"Running {cypher_query}")
        print(gds.run_cypher(cypher_query))

    # # construct vector representation
    print("Adding vector property...")
    print(gds.run_cypher(
        """
            WITH ["Personality", "Music", "Dreams", "Movies", "Fun with friends", "Comedy", "Medicine", "Chemistry", 
            "Shopping centres", "Physics", "Opera", "Animated", "Theatre", "Height", "Weight", "Age", 
            "Number of siblings"] AS excludedProperties
            MATCH (sp:Person)
            WITH [x in keys(sp) WHERE NOT x IN excludedProperties AND toInteger(sp[x]) IS NOT NULL| x] AS allKeys
            LIMIT 1
            MATCH (p:Person)
            UNWIND allKeys as key
            WITH p, collect(toInteger(p[key])) AS vector
            SET p.vector = vector;
        """
    ))
    # # project an in-memory graph using the vector property created earlier
    # # drop if exists (false parameter)
    print(gds.run_cypher(
        """
        CALL gds.graph.drop('survey', false);
        """
    ))
    print("Create graph projection using vector property...")
    print(gds.run_cypher(
        """
        CALL gds.graph.project(
          'survey',
          'Person',
          '*',
          {nodeProperties:['vector']}
        );
         """
    ))
    # # feature normalization using minMax as scaler
    # # necessary due to the sensitivity of the Euclidean distance measurement
    print("Normalize to create scaled vector property ...")
    print(gds.run_cypher(
        """
        CALL gds.alpha.scaleProperties.mutate(
          'survey',
          {
            nodeProperties: ['vector'],
            scaler: 'MinMax',
            mutateProperty: 'scaledVector'
          }
        )
        """
    ))
    # # run kmeans
    print("Running kmeans ...")
    print(gds.run_cypher(
        """
        CALL gds.beta.kmeans.write(
          'survey',
          {
            k:14,
            nodeProperty:'scaledVector',
            writeProperty: 'kmeansCommunity'
          }
        );
        """
    ))
    print(gds.run_cypher(
       """
        match (p:Person)
        return count(distinct(p.kmeansCommunity))
       """
    ))


def query_to_encode_categorical_column(col, df):
    # enumerate unique values to use for encoding
    m = {}
    for index, v in enumerate(df[col].dropna().unique()):
        m[v] = index + 1
    print(m)
    # setup cypher query to add encoded property
    cypher_query = f"""
                MATCH (p:Person)
                WITH p, CASE p['{col}']
        """
    max_index = 0
    for k, v in m.items():
        cypher_query += f"WHEN '{k}' THEN {v} "
        max_index = v
    # handle null values that fall through the case by assigning it average
    cypher_query += f" ELSE {round(max_index / 2)} END as encoded " \
                    f"SET p.`{col}_Encoded` = encoded"
    return cypher_query


def query_to_assign_avgs_to_null_non_category_columns(categorical_cols):
    categorical_cols_list = ""
    for col in categorical_cols:
        quoted_col = f"'{col}'"
        categorical_cols_list += "," + quoted_col if categorical_cols_list else quoted_col
    query = f"""
        MATCH (p:Person)
        UNWIND keys(p) AS key
        WITH p, key
        // filter only numerical values
        WHERE NOT key IN [{categorical_cols_list}]
        WITH key, avg(p[key]) AS averageValue
        MATCH (p1:Person) WHERE p1[key] IS NULL
        // Fill in missing values
        CALL apoc.create.setProperty(p1, key, averageValue)
        YIELD node
        RETURN distinct 'done'
        """
    return query


def setup_person_properties_from_csv_fields(df):
    person_properties = "(:Person {"
    categorical_cols = []
    for col in df.columns:
        if df[col].dtype == "float64":
            person_properties += f"`{col}` : toFloat(row.`{col}`), "
        else:
            categorical_cols.append(col)
            person_properties += f"`{col}` : row.`{col}`,"
    person_properties = person_properties.strip(",")
    person_properties += "})"
    return categorical_cols, person_properties


if __name__ == '__main__':
    main()



