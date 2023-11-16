from pyspark.sql.functions import *

def model(dbt, session):
    # dbt.config(submission_method="cluster")
    dbt.config(materialized="table")
    # dbt.config(packages = ["numpy==1.23.1", "scikit-learn"])
    my_sql_model_df = dbt.ref("lndg_enterprise_survey")

    final_df = my_sql_model_df.withColumn("message", lit("Hello from DBT"))\
                              .withColumn("email", lit("bkp@decisionminds.com"))

    final_df.show()

    return final_df